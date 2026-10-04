
import asyncio
import json
import os
import sqlite3
from datetime import datetime
from typing import Optional

import httpx
import feedparser
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.client.session.aiohttp import AiohttpSession
import aiohttp
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from openai import AsyncOpenAI

import config

# ========================================================================
# 1. ИНИЦИАЛИЗАЦИЯ
# ========================================================================
BOT_TOKEN = os.getenv("BOT_TOKEN")
PROXY_URL = os.getenv("PROXY_URL", "http://127.0.0.1:8080")

session = AiohttpSession(proxy=PROXY_URL)
bot = Bot(token=BOT_TOKEN, session=session)
dp = Dispatcher()
scheduler = AsyncIOScheduler()

llm = AsyncOpenAI(
    api_key=config.LLM_API_KEY,
    base_url=config.LLM_BASE_URL,
)
# Состояния (в памяти, для старта хватит)
pending_topics = {}   # admin_msg_id → list of topics
pending_plans = {}    # admin_msg_id → {topic, plans}
pending_articles = {} # admin_msg_id → {article, factcheck, platform}

# ========================================================================
# 2. БАЗА ДАННЫХ (SQLite)
# ========================================================================

def db_init():
    # Путь внутри контейнера — он соответствует ./db на хосте
    db_path = "/app/db/bot.db"
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS topics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            title TEXT,
            source TEXT,
            status TEXT DEFAULT 'new'
        );
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            topic_id INTEGER,
            platform TEXT,
            content TEXT,
            status TEXT DEFAULT 'draft',
            created_at TEXT,
            FOREIGN KEY(topic_id) REFERENCES topics(id)
        );
        CREATE TABLE IF NOT EXISTS applications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            role TEXT,
            skills TEXT,
            contact TEXT,
            created_at TEXT
        );
    """)
    conn.commit()
    conn.close()

# ========================================================================
# 3. СБОР ТРЕНДОВ
# ========================================================================

async def fetch_hackernews_top(limit: int = 30) -> list[dict]:
    """Top stories с Hacker News через Firebase API (без ключа)."""
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(
            "https://hacker-news.firebaseio.com/v0/topstories.json"
        )
        ids = r.json()[:limit]
        items = []
        for sid in ids:
            rr = await client.get(
                f"https://hacker-news.firebaseio.com/v0/item/{sid}.json"
            )
            data = rr.json()
            if data and data.get("title"):
                items.append({
                    "title": data["title"],
                    "url": data.get("url", f"https://news.ycombinator.com/item?id={sid}"),
                    "source": "HackerNews",
                    "score": data.get("score", 0),
                })
        return items

async def fetch_github_trending(limit: int = 15) -> list[dict]:
    """Свежие репозитории через GitHub Search API (без ключа, 10 req/min)."""
    date_since = datetime.now().strftime("%Y-%m-%d")
    queries = ["messaging", "chat", "collaboration", "real-time", "communication"]
    items = []
    async with httpx.AsyncClient(timeout=15) as client:
        for q in queries:
            r = await client.get(
                "https://api.github.com/search/repositories",
                params={
                    "q": f"created:>{date_since} stars:>50 {q}",
                    "sort": "stars",
                    "per_page": 5,
                },
                headers={"Accept": "application/vnd.github.v3+json"},
            )
            if r.status_code == 200:
                for repo in r.json().get("items", [])[:3]:
                    items.append({
                        "title": repo["full_name"] + " — " + (repo.get("description") or ""),
                        "url": repo["html_url"],
                        "source": "GitHub",
                        "score": repo.get("stargazers_count", 0),
                    })
    return items[:limit]

async def fetch_google_news_rss(query: str = "IT trends messaging") -> list[dict]:
    """Google News RSS — без ключа, без лимитов."""
    url = f"https://news.google.com/rss/search?q={query}&hl=ru&gl=RU"
    feed = feedparser.parse(url)
    items = []
    for entry in feed.entries[:10]:
        items.append({
            "title": entry.get("title", ""),
            "url": entry.get("link", ""),
            "source": "GoogleNews",
            "score": 0,
        })
    return items

async def collect_trends() -> list[dict]:
    """Собирает тренды из всех источников и дедуплицирует."""
    tasks = [
        fetch_hackernews_top(30),
        fetch_github_trending(15),
        fetch_google_news_rss("IT trends messaging collaboration"),
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_items = []
    for res in results:
        if isinstance(res, list):
            all_items.extend(res)
    # Дедупликация по URL
    seen = set()
    unique = []
    for item in all_items:
        if item["url"] not in seen:
            seen.add(item["url"])
            unique.append(item)
    return unique

# ========================================================================
# 4. LLM: ФИЛЬТРАЦИЯ И РАНЖИРОВАНИЕ ТЕМ
# ========================================================================

async def llm_filter_topics(items: list[dict]) -> list[dict]:
    """LLM фильтрует и ранжирует темы под ЦА (корпоративный мессенджер)."""
    items_text = "\n".join(
        f"{i+1}. {it['title']} (источник: {it['source']})"
        for i, it in enumerate(items[:40])
    )
    prompt = f"""Ты — редактор IT-блога про корпоративные мессенджеры и командную работу.
ЦА: разработчики, CTO, тимлиды, product-менеджеры.

Отфильтруй и отранжируй темы, которые:
1. Интересны этой аудитории
2. Могут привлечь сообщество и обсуждение
3. Связаны с IT, коммуникациями, разработкой

Верни строго JSON-массив из 5 элементов:
[{{"title": "...", "reason": "почему важно", "original_index": N}}]

Темы:
{items_text}"""
    resp = await llm.chat.completions.create(
        model=config.LLM_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3,
    )
    try:
        raw = resp.choices[0].message.content
        # Извлекаем JSON из ответа
        start = raw.find("[")
        end = raw.rfind("]") + 1
        parsed = json.loads(raw[start:end])
        result = []
        for p in parsed:
            idx = p.get("original_index", 0) - 1
            if 0 <= idx < len(items):
                it = items[idx]
                it["reason"] = p.get("reason", "")
                result.append(it)
        return result[:5]
    except Exception:
        # Fallback: первые 5 без LLM
        return items[:5]

# ========================================================================
# 5. LLM: ГЕНЕРАЦИЯ ПЛАНОВ СТАТЕЙ
# ========================================================================

async def generate_plans(topic: dict, platform_style: str) -> list[dict]:
    """Генерирует 3 варианта плана статьи в разных стилях подачи."""
    prompt = f"""Тема: {topic['title']}
Источник: {topic['url']}
Стиль для платформы: {platform_style}

Сгенерируй 3 варианта плана статьи (каждый — список из 4-6 пунктов):
А) Аналитический разбор (сравнение, выводы)
Б) Практический гайд (туториал, инструкция)
В) Провокационное мнение (вызов, обсуждение)

Верни строго JSON:
[{{"variant": "А", "title": "...", "points": ["п1","п2","п3"]}},
 {{"variant": "Б", "title": "...", "points": ["п1","п2","п3"]}},
 {{"variant": "В", "title": "...", "points": ["п1","п2","п3"]}}]"""
    resp = await llm.chat.completions.create(
        model=config.LLM_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.7,
    )
    try:
        raw = resp.choices[0].message.content
        start = raw.find("[")
        end = raw.rfind("]") + 1
        return json.loads(raw[start:end])
    except Exception:
        return [
            {"variant": "А", "title": topic["title"], "points": ["вступление", "разбор", "выводы"]},
            {"variant": "Б", "title": topic["title"], "points": ["введение", "инструкция", "результат"]},
            {"variant": "В", "title": topic["title"], "points": ["тезис", "аргументы", "призыв"]},
        ]

# ========================================================================
# 6. LLM: ГЕНЕРАЦИЯ СТАТЬИ + ФАКТЧЕКИНГ
# ========================================================================

async def generate_article(topic: dict, plan: dict, platform_style: str) -> str:
    """Генерирует полную статью по выбранному плану."""
    points = "\n".join(f"  {i+1}. {p}" for i, p in enumerate(plan["points"]))
    prompt = f"""Напиши статью для IT-блога.

Тема: {topic['title']}
Источник: {topic['url']}
Стиль: {platform_style}
План:
{points}

Требования:
- Объём: 1500-3000 слов
- Вставь ссылки на источники в формате [1], [2] и т.д.
- В конце добавь призыв к сообществу: мы строим корпоративный мессенджер, ищем разработчиков и энтузиастов. Напиши боту.
- Текст в HTML-разметке (h2, p, a, ul, li, strong)"""
    resp = await llm.chat.completions.create(
        model=config.LLM_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.6,
        max_tokens=4000,
    )
    return resp.choices[0].message.content

async def fact_check(article: str) -> str:
    """Независимая проверка статьи на ошибки (второй проход LLM)."""
    prompt = f"""Проверь статью на ошибки. Для каждого утверждения:
1. Подтверждается ли фактами?
2. Есть ли логические противоречия?
3. Нет ли галлюцинаций (выдуманных ссылок/фактов)?

Статья:
{article[:8000]}

Верни отчёт в виде списка:
✅ Что верно
⚠️ Что требует уточнения
❌ Что неверно (с объяснением)"""
    resp = await llm.chat.completions.create(
        model=config.LLM_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
    )
    return resp.choices[0].message.content

# ========================================================================
# 7. ПУБЛИКАЦИЯ
# ========================================================================

async def publish_to_telegram(article_html: str) -> Optional[str]:
    """Публикует в Telegram-канал (короткий анонс + ссылка на Telegraph)."""
    # Для канала берём первые 500 символов как анонс
    plain = article_html.replace("<", "<").replace(">", ">")
    plain = plain[:500] + "..."
    try:
        await bot.send_message(
            chat_id=config.CHANNEL_ID,
            text=plain,
            parse_mode="HTML",
        )
        return "ok"
    except Exception as e:
        print(f"Ошибка публикации в TG: {e}")
        return None

async def publish_to_telegraph(title: str, content_html: str) -> Optional[str]:
    """Публикует полную статью на Telegraph."""
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(
            "https://api.telegra.ph/createAccount",
            data={"short_name": "IT Trends", "author_name": "Trend Bot"},
        )
        token = r.json().get("result", {}).get("access_token")
        if not token:
            return None
        r2 = await client.post(
            "https://api.telegra.ph/createPage",
            data={
                "access_token": token,
                "title": title[:256],
                "author_name": "Trend Bot",
                "content": json.dumps([{"tag": "p", "html": content_html}]),
            },
        )
        return r2.json().get("result", {}).get("url")

# ========================================================================
# 8. TELEGRAM-БОТ: ОБРАБОТЧИКИ
# ========================================================================

@dp.message(Command("start"))
async def cmd_start(msg: Message):
    if msg.from_user.id == config.ADMIN_ID:
        await msg.answer(
            "👋 Привет! Я бот трендов.\n\n"
            "/trends — собрать темы прямо сейчас\n"
            "/help — справка"
        )
    else:
        # Читатель канала — заявка на коллаборацию
        await msg.answer(
            "👋 Мы строим корпоративный мессенджер!\n\n"
            "Чем интересуешься?",
            reply_markup=InlineKeyboardBuilder()
            .button(text="💻 Разработка", callback_data="collab_dev")
            .button(text="🎨 Дизайн", callback_data="collab_design")
            .button(text="📊 Продакт", callback_data="collab_product")
            .button(text="🤝 Партнёрство", callback_data="collab_partner")
            .as_markup(),
        )

@dp.message(Command("trends"))
async def cmd_trends(msg: Message):
    if msg.from_user.id != config.ADMIN_ID:
        return
    await msg.answer("⏳ Собираю тренды...")
    items = await collect_trends()
    if not items:
        await msg.answer("❌ Не удалось собрать тренды. Проверь подключение.")
        return
    top = await llm_filter_topics(items)
    if not top:
        await msg.answer("❌ LLM не отдал темы. Проверь API-ключ DeepSeek.")
        return
    # Сохраняем в БД
    conn = sqlite3.connect("bot.db")
    for t in top:
        conn.execute(
            "INSERT INTO topics (date, title, source, status) VALUES (?, ?, ?, 'new')",
            (datetime.now().isoformat(), t["title"], t["source"]),
        )
    conn.commit()
    conn.close()
    # Клавиатура
    kb = InlineKeyboardBuilder()
    for i, t in enumerate(top):
        kb.button(text=f"✅ Тема {i+1}", callback_data=f"topic_{i}")
    kb.adjust(3)
    text = "\n\n".join(
        f"📝 Тема {i+1}: «{t['title']}»\n   Источник: {t['source']} | Очки: {t.get('score', 0)}"
        for i, t in enumerate(top)
    )
    sent = await msg.answer(f"📰 Темы дня:\n\n{text}", reply_markup=kb.as_markup())
    pending_topics[sent.message_id] = top

@dp.callback_query(F.data.startswith("topic_"))
async def cb_topic(cb: CallbackQuery):
    idx = int(cb.data.split("_")[1])
    topics = pending_topics.get(cb.message.message_id, [])
    if idx >= len(topics):
        await cb.answer("Тема не найдена")
        return
    topic = topics[idx]
    weekday = datetime.now().weekday()
    style_info = config.PLATFORM_STYLES.get(weekday, config.PLATFORM_STYLES[0])
    platform = style_info["platform"]
    style = style_info["style"]
    await cb.answer("Генерирую планы...")
    plans = await generate_plans(topic, style)
    kb = InlineKeyboardBuilder()
    for i, p in enumerate(plans):
        kb.button(text=f"✅ План {p['variant']}", callback_data=f"plan_{i}")
    kb.adjust(3)
    text = f"Тема: «{topic['title']}»\nПлатформа: {platform} | Стиль: {style}\n\n"
    for p in plans:
        text += f"\n📌 План {p['variant']}: {p['title']}\n"
        for pt in p["points"]:
            text += f"   • {pt}\n"
    sent = await cb.message.answer(text, reply_markup=kb.as_markup())
    pending_plans[sent.message_id] = {"topic": topic, "plans": plans, "platform": platform, "style": style}

@dp.callback_query(F.data.startswith("plan_"))
async def cb_plan(cb: CallbackQuery):
    idx = int(cb.data.split("_")[1])
    data = pending_plans.get(cb.message.message_id, {})
    if not data:
        await cb.answer("Данные устарели")
        return
    topic = data["topic"]
    plan = data["plans"][idx]
    platform = data["platform"]
    style = data["style"]
    await cb.answer("Генерирую статью...")
    article = await generate_article(topic, plan, style)
    await cb.message.answer("🔍 Запускаю фактчекинг...")
    report = await fact_check(article)
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Опубликовать", callback_data="pub")
    kb.button(text="✏️ На доработку", callback_data="rework")
    text = f"📄 Статья готова ({len(article)} символов)\nПлатформа: {platform}\n\n🔍 Отчёт фактчекинга:\n{report[:2000]}"
    sent = await cb.message.answer(text, reply_markup=kb.as_markup())
    pending_articles[sent.message_id] = {
        "article": article,
        "factcheck": report,
        "topic": topic,
        "plan": plan,
        "platform": platform,
    }

@dp.callback_query(F.data == "pub")
async def cb_publish(cb: CallbackQuery):
    data = pending_articles.get(cb.message.message_id, {})
    if not data:
        await cb.answer("Данные устарели")
        return
    article = data["article"]
    topic = data["topic"]
    platform = data["platform"]
    if platform == "telegram":
        url = await publish_to_telegraph(topic["title"], article)
        await publish_to_telegram(article)
        await cb.message.answer(f"✅ Опубликовано в Telegram-канал + Telegraph: {url}")
    else:
        # Для ручных платформ — отправляем готовый текст
        await cb.message.answer(
            f"📄 Текст для {platform} готов. Скопируй и опубликуй вручную:\n\n{article[:3500]}"
        )

@dp.callback_query(F.data == "rework")
async def cb_rework(cb: CallbackQuery):
    await cb.message.answer(
        "✏️ Напиши, что исправить (ответь на это сообщение):"
    )
# ========================================================================
# 9. ОБРАБОТКА ЗАЯВОК ОТ ЧИТАТЕЛЕЙ
# ========================================================================

collab_state = {}  # user_id → step

@dp.callback_query(F.data.startswith("collab_"))
async def cb_collab(cb: CallbackQuery):
    role_map = {
        "collab_dev": "Разработка",
        "collab_design": "Дизайн",
        "collab_product": "Продакт",
        "collab_partner": "Партнёрство",
    }
    role = role_map.get(cb.data, "Не указано")
    collab_state[cb.from_user.id] = {"role": role, "step": "name"}
    await cb.message.answer("Как вас зовут?")

@dp.message(F.text)
async def handle_collab(msg: Message):
    if msg.from_user.id == config.ADMIN_ID:
        # Если админ пишет после "на доработку" — перегенерируем
        # (упрощённая версия)
        return
    state = collab_state.get(msg.from_user.id)
    if not state:
        return
    if state["step"] == "name":
        state["name"] = msg.text
        state["step"] = "skills"
        await msg.answer("Какие у вас навыки / стек?")
    elif state["step"] == "skills":
        state["skills"] = msg.text
        state["step"] = "contact"
        await msg.answer("Ваш контакт (Telegram или почта)?")
    elif state["step"] == "contact":
        state["contact"] = msg.text
        # Сохраняем
        conn = sqlite3.connect("bot.db")
        conn.execute(
            "INSERT INTO applications (name, role, skills, contact, created_at) VALUES (?, ?, ?, ?, ?)",
            (state["name"], state["role"], state["skills"], state["contact"], datetime.now().isoformat()),
        )
        conn.commit()
        conn.close()
        # Отправляем админу
        await bot.send_message(
            config.ADMIN_ID,
            f"🔔 Новая заявка!\n👤 {state['name']}\n💼 {state['role']}\n"
            f"🛠 {state['skills']}\n📫 {state['contact']}"
        )
        await msg.answer("✅ Заявка отправлена! Мы свяжемся с вами.")
        collab_state.pop(msg.from_user.id, None)

# ========================================================================
# 10. ПЛАНИРОВЩИК (ежедневный сбор трендов)
# ========================================================================

async def daily_trends():
    """Автоматический запуск в заданное время."""
    await bot.send_message(config.ADMIN_ID, "⏳ Ежедневный сбор трендов запущен...")
    # Аналогично /trends, но без команды
    items = await collect_trends()
    if not items:
        await bot.send_message(config.ADMIN_ID, "❌ Тренды не собраны сегодня.")
        return
    top = await llm_filter_topics(items)
    conn = sqlite3.connect("bot.db")
    for t in top:
        conn.execute(
            "INSERT INTO topics (date, title, source, status) VALUES (?, ?, ?, 'new')",
            (datetime.now().isoformat(), t["title"], t["source"]),
        )
    conn.commit()
    conn.close()
    kb = InlineKeyboardBuilder()
    for i, t in enumerate(top):
        kb.button(text=f"✅ Тема {i+1}", callback_data=f"topic_{i}")
    kb.adjust(3)
    text = "\n\n".join(
        f"📝 Тема {i+1}: «{t['title']}»\n   Источник: {t['source']}"
        for i, t in enumerate(top)
    )
    sent = await bot.send_message(
        config.ADMIN_ID,
        f"📰 Темы дня ({datetime.now().strftime('%d.%m')}):\n\n{text}",
        reply_markup=kb.as_markup(),
    )
    pending_topics[sent.message_id] = top

# ========================================================================
# 11. ЗАПУСК
# ========================================================================

async def main():
    db_init()
    # Планировщик
    hour, minute = config.TRENDS_TIME.split(":")
    scheduler.add_job(daily_trends, "cron", hour=int(hour), minute=int(minute))
    scheduler.start()
    print(f"Бот запущен. Тренды будут приходить в {config.TRENDS_TIME}")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
