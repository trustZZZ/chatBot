"""
Trend Scanner — веб-панель сбора и генерации контента (без Telegram-бота).

Собирает IT-тренды (HackerNews, GitHub Trending, Google News RSS),
ранжирует темы через LLM (DeepSeek), генерирует планы статей и сами статьи,
проводит фактчекинг и публикует результат на Telegraph.

Запуск (локально):  uvicorn bot:app --host 0.0.0.0 --port 8000
Запуск (docker):    docker compose up -d --build
Браузер:            http://<хост>:8000/
"""

import asyncio
import html
import json
import logging
import os
import re
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

import aiosqlite
import feedparser
import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from openai import AsyncOpenAI
from pydantic import BaseModel

import config

# ========================================================================
# 0. ЛОГИРОВАНИЕ
# ========================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.getLogger("trend_bot").setLevel(logging.DEBUG)
logging.getLogger("aiosqlite").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.INFO)
logger = logging.getLogger("trend_bot")


def _cleanup_proxy_env() -> None:
    """Убираем системные прокси-переменные из окружения процесса.

    httpx по умолчанию включает trust_env=True и сам подхватывает эти
    переменные, из-за чего прокси молча применился бы ко ВСЕМ запросам.
    Прокси управляется ТОЛЬКО через PROXY_URL в config.py.
    """
    for var in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
                "http_proxy", "https_proxy", "all_proxy"):
        os.environ.pop(var, None)


_cleanup_proxy_env()

# ========================================================================
# 1. ИНИЦИАЛИЗАЦИЯ
# ========================================================================
DB_PATH = config.DB_PATH


def _make_httpx_client(timeout: float = 20.0) -> httpx.AsyncClient:
    """HTTP-клиент: через PROXY_URL (sing-box), если он задан, иначе напрямую."""
    if config.PROXY_URL:
        return httpx.AsyncClient(proxy=config.PROXY_URL, timeout=timeout)
    return httpx.AsyncClient(timeout=timeout)


# LLM: OpenAI-совместимый клиент (DeepSeek). Проксируется так же, как всё
# остальное, через кастомный http_client. Если ключ не задан — llm=None,
# все LLM-функции деградируют до fallback-веток.
if config.LLM_API_KEY:
    _llm_http = httpx.AsyncClient(
        proxy=config.PROXY_URL if config.PROXY_URL else None,
        timeout=httpx.Timeout(120.0, connect=30.0),
    )
    llm: Optional[AsyncOpenAI] = AsyncOpenAI(
        api_key=config.LLM_API_KEY,
        base_url=config.LLM_BASE_URL,
        http_client=_llm_http,
    )
else:
    llm = None
    logger.warning("DEEPSEEK_API_KEY не задан — LLM-функции будут использовать fallback.")

# Состояние веб-панели (в памяти; БД хранит историю)
pending_items: list[dict] = []                 # последний сырой сбор трендов
plans_store: dict[int, list[dict]] = {}        # topic_id -> сгенерированные планы
factcheck_store: dict[int, str] = {}           # article_id -> отчёт фактчекинга
published_urls: dict[int, str] = {}            # article_id -> url Telegraph
state = {
    "last_collect": None,
    "last_collect_count": 0,
    "last_rank": None,
    "last_daily": None,
}

scheduler: Optional[AsyncIOScheduler] = None

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")

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


async def save_topics(topics: list[dict]) -> list[int]:
    """Сохраняет темы в БД и возвращает их id."""
    ids: list[int] = []
    if not topics:
        return ids
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            now = datetime.now().isoformat()
            for t in topics:
                cur = await db.execute(
                    "INSERT INTO topics (date, title, source, status) VALUES (?, ?, ?, 'new')",
                    (now, t["title"], t["source"]),
                )
                ids.append(int(cur.lastrowid))
            await db.commit()
        logger.info("Сохранено тем в БД: %d", len(topics))
    except Exception as exc:
        logger.error("Ошибка сохранения тем в БД: %s", exc)
    return ids


async def list_topics(limit: int = 50) -> list[dict]:
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT * FROM topics ORDER BY id DESC LIMIT ?", (limit,)
            )
            return [dict(r) for r in await cur.fetchall()]
    except Exception as exc:
        logger.error("Ошибка чтения тем: %s", exc)
        return []


async def get_topic(topic_id: int) -> Optional[dict]:
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM topics WHERE id = ?", (topic_id,))
            row = await cur.fetchone()
            return dict(row) if row else None
    except Exception as exc:
        logger.error("Ошибка чтения темы %s: %s", topic_id, exc)
        return None


async def save_article(topic_id: int, platform: str, content: str) -> int:
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute(
                "INSERT INTO articles (topic_id, platform, content, status, created_at) "
                "VALUES (?, ?, ?, 'draft', ?)",
                (topic_id, platform, content, datetime.now().isoformat()),
            )
            await db.commit()
            return int(cur.lastrowid)
    except Exception as exc:
        logger.error("Ошибка сохранения статьи: %s", exc)
        return 0


async def get_article(article_id: int) -> Optional[dict]:
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM articles WHERE id = ?", (article_id,))
            row = await cur.fetchone()
            return dict(row) if row else None
    except Exception as exc:
        logger.error("Ошибка чтения статьи %s: %s", article_id, exc)
        return None


async def list_articles(limit: int = 50) -> list[dict]:
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT a.*, t.title AS topic_title FROM articles a "
                "LEFT JOIN topics t ON a.topic_id = t.id "
                "ORDER BY a.id DESC LIMIT ?",
                (limit,),
            )
            return [dict(r) for r in await cur.fetchall()]
    except Exception as exc:
        logger.error("Ошибка чтения статей: %s", exc)
        return []


async def update_article_status(article_id: int, status: str) -> None:
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE articles SET status = ? WHERE id = ?", (status, article_id)
            )
            await db.commit()
    except Exception as exc:
        logger.error("Ошибка обновления статуса статьи: %s", exc)


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


async def list_applications(limit: int = 100) -> list[dict]:
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT * FROM applications ORDER BY id DESC LIMIT ?", (limit,)
            )
            return [dict(r) for r in await cur.fetchall()]
    except Exception as exc:
        logger.error("Ошибка чтения заявок: %s", exc)
        return []


async def delete_application(app_id: int) -> None:
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("DELETE FROM applications WHERE id = ?", (app_id,))
            await db.commit()
    except Exception as exc:
        logger.error("Ошибка удаления заявки %s: %s", app_id, exc)

# ========================================================================
# 3. СБОР ТРЕНДОВ
# ========================================================================

async def fetch_hackernews_top(limit: int = 30) -> list[dict]:
    items = []
    try:
        async with _make_httpx_client(15) as client:
            r = await client.get("https://hacker-news.firebaseio.com/v0/topstories.json")
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
        async with _make_httpx_client(15) as client:
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
    """Google News RSS через наш HTTP-клиент (прокси-совместимый), затем парсинг."""
    try:
        url = f"https://news.google.com/rss/search?q={query}&hl=ru&gl=RU"
        async with _make_httpx_client(20) as client:
            r = await client.get(url)
            r.raise_for_status()
        feed = await asyncio.to_thread(feedparser.parse, r.text)
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
    if llm is None:
        logger.warning("LLM не настроен — пропускаю фильтрацию, беру первые 5")
        return items[:5]
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
                it = dict(items[idx])
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

def _fallback_plans(title: str) -> list[dict]:
    return [
        {"variant": "А", "title": title, "points": ["вступление", "разбор", "выводы"]},
        {"variant": "Б", "title": title, "points": ["введение", "инструкция", "результат"]},
        {"variant": "В", "title": title, "points": ["тезис", "аргументы", "призыв"]},
    ]


async def generate_plans(topic: dict, platform_style: str) -> list[dict]:
    if llm is None:
        logger.warning("LLM не настроен — возвращаю шаблонные планы")
        return _fallback_plans(topic["title"])
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
        return _fallback_plans(topic["title"])
    try:
        raw = resp.choices[0].message.content
        start = raw.find("[")
        end = raw.rfind("]") + 1
        plans = json.loads(raw[start:end])
        logger.info("Сгенерировано планов: %d", len(plans))
        return plans
    except Exception as exc:
        logger.warning("LLM вернул некорректный JSON для планов, fallback: %s", exc)
        return _fallback_plans(topic["title"])

# ========================================================================
# 6. LLM: ГЕНЕРАЦИЯ СТАТЬИ + ФАКТЧЕКИНГ
# ========================================================================

async def generate_article(topic: dict, plan: dict, platform_style: str) -> str:
    if llm is None:
        logger.warning("LLM не настроен — статья не может быть сгенерирована")
        return ""
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
- В конце добавь призыв к сообществу: мы строим корпоративный мессенджер, ищем разработчиков и энтузиастов.
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
    if llm is None:
        return "⚠️ Фактчекинг недоступен: LLM не настроен (DEEPSEEK_API_KEY)."
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
# 7. ПУБЛИКАЦИЯ НА TELEGRAPH
# ========================================================================

_TAG_RE = re.compile(r"<[^>]+>")


def _strip_html(text: str) -> str:
    if not text:
        return ""
    return html.unescape(_TAG_RE.sub(" ", text)).strip()


_telegraph_token: Optional[str] = None


async def publish_to_telegraph(title: str, content_html: str) -> Optional[str]:
    global _telegraph_token
    try:
        async with _make_httpx_client(15) as client:
            if not _telegraph_token:
                r = await client.post(
                    "https://api.telegra.ph/createAccount",
                    data={"short_name": "IT Trends", "author_name": "Trend Scanner"},
                )
                r.raise_for_status()
                _telegraph_token = r.json().get("result", {}).get("access_token")
                if not _telegraph_token:
                    logger.error("Telegraph: не получен access_token")
                    return None
            r2 = await client.post(
                "https://api.telegra.ph/createPage",
                data={
                    "access_token": _telegraph_token,
                    "title": title[:256],
                    "author_name": "Trend Scanner",
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
# 8. ПЛАНИРОВЩИК
# ========================================================================

async def daily_trends() -> None:
    logger.info("Ежедневная задача: сбор трендов начат")
    state["last_daily"] = datetime.now().isoformat()
    try:
        items = await collect_trends()
        if not items:
            logger.warning("Ежедневная задача: тренды не собраны")
            return
        top = await llm_filter_topics(items)
        if top:
            await save_topics(top)
        logger.info("Ежедневная задача завершена: сохранено тем: %d", len(top))
    except Exception as exc:
        logger.exception("Ошибка в ежедневном сборе трендов: %s", exc)


def _install_asyncio_exception_handler() -> None:
    """Логируем необработанные исключения из фоновых asyncio-задач."""
    loop = asyncio.get_running_loop()

    def _handler(_loop: asyncio.AbstractEventLoop, context: dict) -> None:
        exc = context.get("exception")
        logger.critical(
            "НЕОБРАБОТАННОЕ исключение в asyncio-задаче: %s",
            context.get("message", context),
        )
        if exc:
            logger.critical("Полный трейсбек:", exc_info=exc)

    loop.set_exception_handler(_handler)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global scheduler
    _install_asyncio_exception_handler()
    await db_init()
    try:
        hour, minute = config.TRENDS_TIME.split(":")
        scheduler = AsyncIOScheduler(timezone="Europe/Moscow")
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
    yield
    if scheduler:
        try:
            scheduler.shutdown(wait=False)
        except Exception:
            pass
        logger.info("Планировщик остановлен.")


app = FastAPI(title="Trend Scanner", lifespan=lifespan)

# ========================================================================
# 9. WEB: СТРАНИЦЫ И API
# ========================================================================

class RankRequest(BaseModel):
    items: Optional[list[dict]] = None  # если не передано — берём последний сбор


class PlansRequest(BaseModel):
    platform: str = "habr"


class ArticleRequest(BaseModel):
    topic_id: int
    plan_index: int
    platform: str = "habr"


class ApplicationIn(BaseModel):
    name: str
    role: str
    skills: str
    contact: str


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


@app.get("/api/status")
async def api_status() -> dict:
    next_run = None
    if scheduler:
        job = scheduler.get_job("daily_trends")
        if job and job.next_run_time:
            next_run = job.next_run_time.isoformat()
    return {
        "scheduler_running": bool(scheduler and scheduler.running),
        "next_daily_run": next_run,
        "trends_time": config.TRENDS_TIME,
        "llm_configured": bool(config.LLM_API_KEY),
        "llm_model": config.LLM_MODEL,
        "proxy": config.PROXY_URL or None,
        "state": state,
    }


@app.get("/api/platforms")
async def api_platforms() -> dict:
    return {"platforms": config.PLATFORMS}


@app.post("/api/trends/collect")
async def api_collect() -> dict:
    global pending_items
    logger.info("Запрос на сбор трендов из веб-панели")
    items = await collect_trends()
    pending_items = items
    state["last_collect"] = datetime.now().isoformat()
    state["last_collect_count"] = len(items)
    return {"count": len(items), "items": items}


@app.post("/api/topics/rank")
async def api_rank(payload: RankRequest) -> dict:
    global pending_items
    items = payload.items if payload.items is not None else pending_items
    if not items:
        raise HTTPException(
            400,
            "Нет трендов для ранжирования. Сначала выполни «Собрать тренды».",
        )
    if llm is None:
        raise HTTPException(400, "LLM не настроен: задай DEEPSEEK_API_KEY в .env.")
    top = await llm_filter_topics(items)
    if not top:
        raise HTTPException(502, "LLM не вернул темы. Проверь ключ и сеть.")
    ids = await save_topics(top)
    pending_items = items
    state["last_rank"] = datetime.now().isoformat()
    topics = []
    for tid, t in zip(ids, top):
        topics.append({
            "id": tid,
            "title": t["title"],
            "source": t["source"],
            "score": t.get("score", 0),
            "reason": t.get("reason", ""),
            "url": t.get("url", ""),
            "date": datetime.now().isoformat(),
        })
    return {"topics": topics}


@app.get("/api/topics")
async def api_topics() -> dict:
    return {"topics": await list_topics(50)}


@app.post("/api/topics/{topic_id}/plans")
async def api_plans(topic_id: int, payload: PlansRequest) -> dict:
    topic = await get_topic(topic_id)
    if not topic:
        raise HTTPException(404, "Тема не найдена")
    pf = config.PLATFORM_MAP.get(payload.platform)
    if not pf:
        raise HTTPException(400, f"Неизвестная платформа: {payload.platform}")
    logger.info("Генерация планов для темы %s (платформа %s)", topic_id, payload.platform)
    plans = await generate_plans(topic, pf["style"])
    plans_store[topic_id] = plans
    return {
        "topic": topic,
        "platform": payload.platform,
        "platform_name": pf["name"],
        "style": pf["style"],
        "plans": plans,
    }


@app.post("/api/articles/generate")
async def api_generate(payload: ArticleRequest) -> dict:
    topic = await get_topic(payload.topic_id)
    if not topic:
        raise HTTPException(404, "Тема не найдена")
    plans = plans_store.get(payload.topic_id, [])
    if not plans:
        raise HTTPException(400, "Сначала сгенерируй планы для этой темы.")
    if not (0 <= payload.plan_index < len(plans)):
        raise HTTPException(400, "Некорректный индекс плана.")
    pf = config.PLATFORM_MAP.get(payload.platform)
    if not pf:
        raise HTTPException(400, f"Неизвестная платформа: {payload.platform}")
    plan = plans[payload.plan_index]
    logger.info("Генерация статьи для темы %s (план %s, платформа %s)",
                payload.topic_id, plan.get("variant"), payload.platform)
    article = await generate_article(topic, plan, pf["style"])
    if not article:
        raise HTTPException(502, "Не удалось сгенерировать статью. Проверь LLM-ключ и сеть.")
    report = await fact_check(article)
    article_id = await save_article(payload.topic_id, payload.platform, article)
    factcheck_store[article_id] = report
    return {
        "id": article_id,
        "topic_title": topic["title"],
        "platform": payload.platform,
        "platform_name": pf["name"],
        "plan_variant": plan.get("variant"),
        "plan_title": plan.get("title"),
        "article": article,
        "factcheck": report,
    }


@app.get("/api/articles")
async def api_articles() -> dict:
    articles = await list_articles(50)
    for a in articles:
        if a["id"] in factcheck_store:
            a["factcheck"] = factcheck_store[a["id"]]
        if a["id"] in published_urls:
            a["published_url"] = published_urls[a["id"]]
    return {"articles": articles}


@app.post("/api/articles/{article_id}/publish")
async def api_publish(article_id: int) -> dict:
    art = await get_article(article_id)
    if not art:
        raise HTTPException(404, "Статья не найдена")
    if article_id in published_urls:
        return {"url": published_urls[article_id], "already": True}
    topic = await get_topic(art["topic_id"]) if art.get("topic_id") else None
    title = (topic or {}).get("title", "Статья")
    url = await publish_to_telegraph(title, art["content"])
    if not url:
        raise HTTPException(502, "Не удалось опубликовать на Telegraph. Подробности в логах.")
    published_urls[article_id] = url
    await update_article_status(article_id, "published")
    return {"url": url}


@app.post("/api/applications")
async def api_add_application(payload: ApplicationIn) -> dict:
    data = payload.model_dump()
    await save_application(data)
    return {"ok": True}


@app.get("/api/applications")
async def api_applications() -> dict:
    return {"applications": await list_applications(100)}


@app.delete("/api/applications/{app_id}")
async def api_delete_application(app_id: int) -> dict:
    await delete_application(app_id)
    return {"ok": True}


# ========================================================================
# 10. ЗАПУСК
# ========================================================================

if __name__ == "__main__":
    import uvicorn

    logger.info(
        "Старт веб-панели на %s:%s (открой в браузере http://<хост>:%s/)",
        config.WEB_HOST, config.WEB_PORT, config.WEB_PORT,
    )
    uvicorn.run("bot:app", host=config.WEB_HOST, port=config.WEB_PORT, log_level="info")
