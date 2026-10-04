import asyncio
import html
import json
import logging
import os
import re
import sys
from datetime import datetime
from typing import Any, Awaitable, Callable, Optional, Union

import aiohttp
import aiosqlite
import feedparser
import httpx
from aiogram import BaseMiddleware, Bot, Dispatcher, F
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.filters import Command
from aiogram.types import CallbackQuery, ErrorEvent, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from openai import AsyncOpenAI

import config

# ========================================================================
# 0. ЛОГИРОВАНИЕ
# ========================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("trend_bot")

# ========================================================================
# 1. ИНИЦИАЛИЗАЦИЯ (с валидацией токена)
# ========================================================================
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    logger.critical(
        "BOT_TOKEN не задан. Укажи переменную окружения BOT_TOKEN "
        "(в .env или docker-compose.yml) и перезапусти бота."
    )
    sys.exit(1)

PROXY_URL = os.getenv("PROXY_URL")
DB_PATH = os.getenv("DB_PATH", "/app/db/bot.db")


class ProxySession(AiohttpSession):
    """Сессия с поддержкой HTTP-прокси через trust_env."""
    async def get_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(trust_env=True)
        return self._session


def _build_session() -> Optional[AiohttpSession]:
    if PROXY_URL:
        logger.info("Инициализация сессии с прокси: %s", PROXY_URL)
        return ProxySession()
    logger.info("Прокси не задан — использую прямое подключение к Telegram API.")
    return None


bot = Bot(token=BOT_TOKEN, session=_build_session())
dp = Dispatcher()
try:
    scheduler = AsyncIOScheduler(timezone="Europe/Moscow")
except Exception:
    scheduler = AsyncIOScheduler()

llm = AsyncOpenAI(
    api_key=config.LLM_API_KEY,
    base_url=config.LLM_BASE_URL,
)

pending_topics = {}
pending_plans = {}
pending_articles = {}

# ========================================================================
# 2. БАЗА ДАННЫХ
# ========================================================================

_DB_SCHEMA = """
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
"""


async def db_init() -> None:
    db_dir = os.path.dirname(DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.executescript(_DB_SCHEMA)
            await db.commit()
        logger.info("База данных инициализирована: %s", DB_PATH)
    except Exception as exc:
        logger.error("Не удалось инициализировать БД: %s", exc)


async def save_topics(topics: list[dict]) -> None:
    if not topics:
        return
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            now = datetime.now().isoformat()
            await db.executemany(
                "INSERT INTO topics (date, title, source, status) VALUES (?, ?, ?, 'new')",
                [(now, t["title"], t["source"]) for t in topics],
            )
            await db.commit()
        logger.info("Сохранено тем в БД: %d", len(topics))
    except Exception as exc:
        logger.error("Ошибка сохранения тем в БД: %s", exc)


async def save_application(app: dict) -> None:
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO applications (name, role, skills, contact, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    app["name"],
                    app["role"],
                    app["skills"],
                    app["contact"],
                    datetime.now().isoformat(),
                ),
            )
            await db.commit()
        logger.info("Заявка сохранена в БД: %s (%s)", app.get("name"), app.get("role"))
    except Exception as exc:
        logger.error("Ошибка сохранения заявки в БД: %s", exc)

# ========================================================================
# 3. СБОР ТРЕНДОВ
# ========================================================================

async def fetch_hackernews_top(limit: int = 30) -> list[dict]:
    items = []
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(
                "https://hacker-news.firebaseio.com/v0/topstories.json"
            )
            r.raise_for_status()
            ids = r.json()[:limit]
            for sid in ids:
                try:
                    rr = await client.get(
                        f"https://hacker-news.firebaseio.com/v0/item/{sid}.json"
                    )
                    rr.raise_for_status()
                    data = rr.json()
                    if data and data.get("title"):
                        items.append({
                            "title": data["title"],
                            "url": data.get("url", f"https://news.ycombinator.com/item?id={sid}"),
                            "source": "HackerNews",
                            "score": data.get("score", 0),
                        })
                except Exception as exc:
                    logger.warning("HN: не удалось загрузить новость %s: %s", sid, exc)
                    continue
    except Exception as exc:
        logger.error("Не удалось получить топ HackerNews: %s", exc)
        return []
    logger.info("HackerNews: собрано новостей: %d", len(items))
    return items


async def fetch_github_trending(limit: int = 15) -> list[dict]:
    date_since = datetime.now().strftime("%Y-%m-%d")
    queries = ["messaging", "chat", "collaboration", "real-time", "communication"]
    items = []
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            for q in queries:
                try:
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
                    else:
                        logger.warning("GitHub API: HTTP %s для запроса '%s'", r.status_code, q)
                except Exception as exc:
                    logger.warning("GitHub API: ошибка для запроса '%s': %s", q, exc)
                    continue
    except Exception as exc:
        logger.error("Не удалось подключиться к GitHub API: %s", exc)
        return []
    logger.info("GitHub: собрано репозиториев: %d", len(items))
    return items[:limit]


async def fetch_google_news_rss(query: str = "IT trends messaging") -> list[dict]:
    try:
        url = f"https://news.google.com/rss/search?q={query}&hl=ru&gl=RU"
        feed = await asyncio.to_thread(feedparser.parse, url)
        items = []
        for entry in feed.entries[:10]:
            items.append({
                "title": entry.get("title", ""),
                "url": entry.get("link", ""),
                "source": "GoogleNews",
                "score": 0,
            })
        logger.info("GoogleNews RSS: получено записей: %d", len(items))
        return items
    except Exception as exc:
        logger.error("Ошибка Google News RSS: %s", exc)
        return []


async def collect_trends() -> list[dict]:
    tasks = [
        fetch_hackernews_top(30),
        fetch_github_trending(15),
        fetch_google_news_rss("IT trends messaging collaboration"),
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_items = []
    for idx, res in enumerate(results):
        if isinstance(res, list):
            all_items.extend(res)
        else:
            logger.warning("Источник №%d вернул ошибку: %s", idx + 1, res)
    seen = set()
    unique = []
    for item in all_items:
        url = item.get("url")
        if url and url not in seen:
            seen.add(url)
            unique.append(item)
    logger.info("Собрано трендов: %d (до дедупликации: %d)", len(unique), len(all_items))
    return unique

# ========================================================================
# 4. LLM: ФИЛЬТРАЦИЯ И РАНЖИРОВАНИЕ ТЕМ
# ========================================================================

async def llm_filter_topics(items: list[dict]) -> list[dict]:
    if not items:
        return []
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
    try:
        resp = await llm.chat.completions.create(
            model=config.LLM_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
        )
    except Exception as exc:
        logger.error("LLM-фильтрация тем не удалась: %s", exc)
        return items[:5]
    try:
        raw = resp.choices[0].message.content
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
        logger.info("LLM отобрал тем: %d", len(result))
        return result[:5]
    except Exception as exc:
        logger.warning("LLM вернул некорректный JSON, fallback на первые 5: %s", exc)
        return items[:5]

# ========================================================================
# 5. LLM: ГЕНЕРАЦИЯ ПЛАНОВ СТАТЕЙ
# ========================================================================

async def generate_plans(topic: dict, platform_style: str) -> list[dict]:
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
    try:
        resp = await llm.chat.completions.create(
            model=config.LLM_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
        )
    except Exception as exc:
        logger.error("LLM-генерация планов не удалась: %s", exc)
        return [
            {"variant": "А", "title": topic["title"], "points": ["вступление", "разбор", "выводы"]},
            {"variant": "Б", "title": topic["title"], "points": ["введение", "инструкция", "результат"]},
            {"variant": "В", "title": topic["title"], "points": ["тезис", "аргументы", "призыв"]},
        ]
    try:
        raw = resp.choices[0].message.content
        start = raw.find("[")
        end = raw.rfind("]") + 1
        plans = json.loads(raw[start:end])
        logger.info("Сгенерировано планов: %d", len(plans))
        return plans
    except Exception as exc:
        logger.warning("LLM вернул некорректный JSON для планов, fallback: %s", exc)
        return [
            {"variant": "А", "title": topic["title"], "points": ["вступление", "разбор", "выводы"]},
            {"variant": "Б", "title": topic["title"], "points": ["введение", "инструкция", "результат"]},
            {"variant": "В", "title": topic["title"], "points": ["тезис", "аргументы", "призыв"]},
        ]

# ========================================================================
# 6. LLM: ГЕНЕРАЦИЯ СТАТЬИ + ФАКТЧЕКИНГ
# ========================================================================

async def generate_article(topic: dict, plan: dict, platform_style: str) -> str:
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
    try:
        resp = await llm.chat.completions.create(
            model=config.LLM_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.6,
            max_tokens=4000,
        )
        content = resp.choices[0].message.content
        if not content:
            raise ValueError("LLM вернул пустой ответ")
        logger.info("Статья сгенерирована: %d символов", len(content))
        return content
    except Exception as exc:
        logger.error("Генерация статьи не удалась: %s", exc)
        return ""


async def fact_check(article: str) -> str:
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
    try:
        resp = await llm.chat.completions.create(
            model=config.LLM_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
        )
        return resp.choices[0].message.content
    except Exception as exc:
        logger.error("Фактчекинг не удался: %s", exc)
        return "⚠️ Фактчекинг временно недоступен (ошибка LLM). Проверь статью вручную."

# ========================================================================
# 7. ПУБЛИКАЦИЯ
# ========================================================================

_TAG_RE = re.compile(r"<[^>]+>")


def _strip_html(text: str) -> str:
    if not text:
        return ""
    return html.unescape(_TAG_RE.sub(" ", text)).strip()


async def publish_to_telegram(article_html: str) -> Optional[str]:
    try:
        plain = html.escape(_strip_html(article_html)[:500]) + "..."
        await bot.send_message(
            chat_id=config.CHANNEL_ID,
            text=plain,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
        logger.info("Анонс опубликован в канал %s", config.CHANNEL_ID)
        return "ok"
    except Exception as exc:
        logger.error("Ошибка публикации в TG-канал: %s", exc)
        return None


async def publish_to_telegraph(title: str, content_html: str) -> Optional[str]:
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(
                "https://api.telegra.ph/createAccount",
                data={"short_name": "IT Trends", "author_name": "Trend Bot"},
            )
            r.raise_for_status()
            token = r.json().get("result", {}).get("access_token")
            if not token:
                logger.error("Telegraph: не получен access_token")
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
            r2.raise_for_status()
            page_url = r2.json().get("result", {}).get("url")
            logger.info("Telegraph: страница создана: %s", page_url)
            return page_url
    except Exception as exc:
        logger.error("Ошибка публикации на Telegraph: %s", exc)
        return None

# ========================================================================
# 8. TELEGRAM-БОТ: ОБРАБОТЧИКИ
# ========================================================================

class LoggingMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[
            [Union[Message, CallbackQuery], dict[str, Any]],
            Awaitable[Any],
        ],
        event: Union[Message, CallbackQuery],
        data: dict[str, Any],
    ) -> Any:
        user = getattr(event, "from_user", None)
        user_desc = f"{user.full_name} (id={user.id})" if user else "?"
        if isinstance(event, Message):
            logger.info(
                "Получено сообщение от %s: %s",
                user_desc,
                (event.text or "")[:200],
            )
        else:
            logger.info("Получен callback от %s: %s", user_desc, event.data)

        try:
            return await handler(event, data)
        except Exception as exc:
            logger.exception("Ошибка при обработке события: %s", exc)
            try:
                if isinstance(event, Message):
                    await event.answer("⚠️ Что-то пошло не так. Попробуйте ещё раз.")
                elif isinstance(event, CallbackQuery):
                    await event.answer("⚠️ Ошибка при обработке. Попробуйте ещё раз.", show_alert=False)
            except Exception:
                pass
            return None


dp.message.middleware(LoggingMiddleware())
dp.callback_query.middleware(LoggingMiddleware())


@dp.errors()
async def errors_handler(event: ErrorEvent) -> None:
    logger.error("Ошибка обработки апдейта: %s", event.exception)


@dp.message(Command("start"))
async def cmd_start(msg: Message):
    if msg.from_user.id == config.ADMIN_ID:
        await msg.answer(
            "👋 Привет! Я бот трендов.\n\n"
            "/trends — собрать темы прямо сейчас\n"
            "/help — справка"
        )
    else:
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
    logger.info("Команда /trends от админа %s", msg.from_user.id)
    await msg.answer("⏳ Собираю тренды...")
    try:
        items = await collect_trends()
        if not items:
            await msg.answer("❌ Не удалось собрать тренды. Проверь подключение.")
            return
        top = await llm_filter_topics(items)
        if not top:
            await msg.answer("❌ LLM не отдал темы. Проверь API-ключ DeepSeek.")
            return
        await save_topics(top)
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
        logger.info("Темы отправлены админу: %d шт.", len(top))
    except Exception as exc:
        logger.exception("Ошибка в /trends: %s", exc)
        await msg.answer("⚠️ Ошибка при сборе трендов. Подробности в логах.")


@dp.callback_query(F.data.startswith("topic_"))
async def cb_topic(cb: CallbackQuery):
    try:
        idx = int(cb.data.split("_")[1])
    except (ValueError, IndexError):
        await cb.answer("Некорректные данные")
        return
    if not cb.message:
        await cb.answer("Сообщение недоступно")
        return
    try:
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
        pending_plans[sent.message_id] = {
            "topic": topic,
            "plans": plans,
            "platform": platform,
            "style": style,
        }
        logger.info("Планы сгенерированы для темы «%s»", topic["title"])
    except Exception as exc:
        logger.exception("Ошибка в cb_topic: %s", exc)
        await cb.answer("⚠️ Ошибка при генерации планов.", show_alert=False)


@dp.callback_query(F.data.startswith("plan_"))
async def cb_plan(cb: CallbackQuery):
    try:
        idx = int(cb.data.split("_")[1])
    except (ValueError, IndexError):
        await cb.answer("Некорректные данные")
        return
    if not cb.message:
        await cb.answer("Сообщение недоступно")
        return
    try:
        data = pending_plans.get(cb.message.message_id, {})
        if not data or idx >= len(data.get("plans", [])):
            await cb.answer("Данные устарели")
            return
        topic = data["topic"]
        plan = data["plans"][idx]
        platform = data["platform"]
        style = data["style"]
        await cb.answer("Генерирую статью...")
        article = await generate_article(topic, plan, style)
        if not article:
            await cb.message.answer("❌ Не удалось сгенерировать статью. Попробуй ещё раз.")
            return
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
        logger.info("Статья для платформы %s подготовлена (%d симв.)", platform, len(article))
    except Exception as exc:
        logger.exception("Ошибка в cb_plan: %s", exc)
        await cb.answer("⚠️ Ошибка при генерации статьи.", show_alert=False)


@dp.callback_query(F.data == "pub")
async def cb_publish(cb: CallbackQuery):
    data = pending_articles.get(cb.message.message_id, {}) if cb.message else {}
    if not data:
        await cb.answer("Данные устарели")
        return
    article = data["article"]
    topic = data["topic"]
    platform = data["platform"]
    try:
        if platform == "telegram":
            url = await publish_to_telegraph(topic["title"], article)
            res = await publish_to_telegram(article)
            if url:
                await cb.message.answer(f"✅ Опубликовано в Telegram-канал + Telegraph: {url}")
            elif res == "ok":
                await cb.message.answer("✅ Опубликовано в Telegram-канал (Telegraph: не удалось создать страницу).")
            else:
                await cb.message.answer("❌ Не удалось опубликовать. Подробности в логах.")
        else:
            await cb.message.answer(
                f"📄 Текст для {platform} готов. Скопируй и опубликуй вручную:\n\n{article[:3500]}"
            )
        logger.info("Публикация завершена для платформы %s", platform)
    except Exception as exc:
        logger.exception("Ошибка публикации: %s", exc)
        await cb.message.answer("⚠️ Ошибка при публикации. Подробности в логах.")


@dp.callback_query(F.data == "rework")
async def cb_rework(cb: CallbackQuery):
    await cb.message.answer(
        "✏️ Напиши, что исправить (ответь на это сообщение):"
    )

# ========================================================================
# 9. ОБРАБОТКА ЗАЯВОК ОТ ЧИТАТЕЛЕЙ
# ========================================================================

collab_state = {}

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
    logger.info("Читатель %s начал заявку: роль «%s»", cb.from_user.id, role)
    await cb.message.answer("Как вас зовут?")


@dp.message(F.text)
async def handle_collab(msg: Message):
    if msg.from_user.id == config.ADMIN_ID:
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
        await save_application(state)
        try:
            await bot.send_message(
                config.ADMIN_ID,
                f"🔔 Новая заявка!\n👤 {state['name']}\n💼 {state['role']}\n"
                f"🛠 {state['skills']}\n📫 {state['contact']}"
            )
        except Exception as exc:
            logger.error("Не удалось уведомить админа о заявке: %s", exc)
        await msg.answer("✅ Заявка отправлена! Мы свяжемся с вами.")
        collab_state.pop(msg.from_user.id, None)
        logger.info("Заявка читателя %s обработана полностью.", msg.from_user.id)

# ========================================================================
# 10. ПЛАНИРОВЩИК
# ========================================================================

async def daily_trends():
    logger.info("Ежедневная задача: сбор трендов начат")
    try:
        await bot.send_message(config.ADMIN_ID, "⏳ Ежедневный сбор трендов запущен...")
        items = await collect_trends()
        if not items:
            await bot.send_message(config.ADMIN_ID, "❌ Тренды не собраны сегодня.")
            return
        top = await llm_filter_topics(items)
        await save_topics(top)
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
        logger.info("Ежедневная задача завершена: отправлено %d тем", len(top))
    except Exception as exc:
        logger.exception("Ошибка в ежедневном сборе трендов: %s", exc)

# ========================================================================
# 11. ПРОВЕРКА ПРОКСИ
# ========================================================================

async def check_proxy_available(proxy_url: str, timeout: float = 5.0) -> bool:
    """Проверяет доступность прокси через trust_env (без aiohttp-socks)."""
    try:
        async with aiohttp.ClientSession(trust_env=True) as client:
            async with client.get(
                "https://api.telegram.org",
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as resp:
                logger.info("Прокси %s доступен (HTTP %s).", proxy_url, resp.status)
                return True
    except Exception as exc:
        logger.warning("Прокси %s недоступен: %s", proxy_url, exc)
        return False

# ========================================================================
# 12. ЗАПУСК
# ========================================================================

async def main():
    global bot

    await db_init()

    if PROXY_URL:
        if await check_proxy_available(PROXY_URL):
            logger.info("Прокси %s доступен — продолжаю с прокси.", PROXY_URL)
        else:
            logger.warning(
                "Прокси %s недоступен — переключаюсь на прямое подключение. "
                "Бот продолжит работу.",
                PROXY_URL,
            )
            try:
                await bot.session.close()
            except Exception:
                pass
            bot = Bot(token=BOT_TOKEN)

    try:
        hour, minute = config.TRENDS_TIME.split(":")
        scheduler.add_job(
            daily_trends,
            "cron",
            hour=int(hour),
            minute=int(minute),
            id="daily_trends",
            replace_existing=True,
        )
        scheduler.start()
        logger.info("Планировщик запущен: ежедневный сбор трендов в %s", config.TRENDS_TIME)
    except Exception as exc:
        logger.error("Не удалось настроить планировщик: %s", exc)

    try:
        me = await bot.get_me()
        logger.info("Подключение к Telegram API успешно: @%s", me.username)
    except Exception as exc:
        logger.error("Telegram API недоступен или токен неверный: %s", exc)

    logger.info("🚀 Бот запущен. Стартую polling...")
    try:
        await dp.start_polling(bot)
    finally:
        try:
            scheduler.shutdown(wait=False)
        except Exception:
            pass
        logger.info("Бот остановлен, планировщик завершён.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот остановлен пользователем.")
