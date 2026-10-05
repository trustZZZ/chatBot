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
pending_items: list[dict] = []                 # последний сырой сбор трендов (для повторного использования)
pending_topics: dict = {"blocks": []}          # последние темы (структура с блоками) из сырого сбора
plans_store: dict[int, list[dict]] = {}        # topic_id -> сгенерированные планы
factcheck_store: dict[int, str] = {}           # article_id -> отчёт фактчекинга
published_urls: dict[int, str] = {}            # article_id -> url Telegraph
state = {
    "last_collect": None,
    "last_collect_count": 0,
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
        description TEXT,
        sources TEXT,
        source_count INTEGER DEFAULT 0,
        source TEXT,
        block TEXT,
        status TEXT DEFAULT 'new',
        audience TEXT,
        goal TEXT,
        cta TEXT,
        reasons TEXT,
        article_type TEXT
    );
    CREATE TABLE IF NOT EXISTS articles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        topic_id INTEGER,
        platform TEXT,
        content TEXT,
        status TEXT DEFAULT 'draft',
        created_at TEXT,
        final_content TEXT,
        expert_report TEXT,
        expert_rating INTEGER,
        expert_recommendation TEXT,
        FOREIGN KEY(topic_id) REFERENCES topics(id)
    );
"""


async def _ensure_topic_columns(db) -> None:
    """Миграция: добавляет новые колонки тем в уже существующую таблицу topics."""
    cur = await db.execute("PRAGMA table_info(topics)")
    cols = {row[1] for row in await cur.fetchall()}
    for name, decl in (
        ("description", "TEXT"),
        ("sources", "TEXT"),
        ("source_count", "INTEGER DEFAULT 0"),
        ("block", "TEXT"),
        ("audience", "TEXT"),
        ("goal", "TEXT"),
        ("cta", "TEXT"),
        ("reasons", "TEXT"),
        ("article_type", "TEXT"),
    ):
        if name not in cols:
            await db.execute(f"ALTER TABLE topics ADD COLUMN {name} {decl}")
            logger.info("Миграция: добавлена колонка topics.%s", name)


async def _ensure_article_columns(db) -> None:
    """Миграция: добавляет колонки экспертизы и финального текста в таблицу articles."""
    cur = await db.execute("PRAGMA table_info(articles)")
    cols = {row[1] for row in await cur.fetchall()}
    for name, decl in (
        ("final_content", "TEXT"),
        ("expert_report", "TEXT"),
        ("expert_rating", "INTEGER"),
        ("expert_recommendation", "TEXT"),
    ):
        if name not in cols:
            await db.execute(f"ALTER TABLE articles ADD COLUMN {name} {decl}")
            logger.info("Миграция: добавлена колонка articles.%s", name)


async def db_init() -> None:
    db_dir = os.path.dirname(DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.executescript(_DB_SCHEMA)
            await _ensure_topic_columns(db)
            await _ensure_article_columns(db)
            await db.commit()
        logger.info("База данных инициализирована: %s", DB_PATH)
    except Exception as exc:
        logger.error("Не удалось инициализировать БД: %s", exc)


def _parse_topic_row(row: dict) -> dict:
    """Превращает строку topics из БД в dict с распарсенными sources и reasons."""
    row = dict(row)
    for key in ("sources", "reasons"):
        try:
            row[key] = json.loads(row.get(key) or "[]")
        except (TypeError, ValueError):
            row[key] = []
    return row


async def save_topics(topics) -> list[int]:
    """Сохраняет темы в БД и возвращает их id.

    Принимает темы в формате extract_topics:
    - структуру с блоками: {"blocks": [{"block_name": ..., "themes": [...]}]}, либо
    - плоский список тем {topic, description, audience, goal, cta, reasons,
      article_type, sources (list[str]), source_count, block}.
    """
    ids: list[int] = []
    if not topics:
        return ids
    if isinstance(topics, dict):
        topics = flatten_blocks(topics)
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            now = datetime.now().isoformat()
            for t in topics:
                sources = t.get("sources") or []
                title = (t.get("topic") or t.get("title") or "").strip()
                if not title:
                    continue
                block = (t.get("block") or "").strip()
                reasons = [str(r).strip() for r in (t.get("reasons") or []) if str(r).strip()]
                cur = await db.execute(
                    "INSERT INTO topics "
                    "(date, title, description, sources, source_count, block, status, "
                    "audience, goal, cta, reasons, article_type) "
                    "VALUES (?, ?, ?, ?, ?, ?, 'new', ?, ?, ?, ?, ?)",
                    (
                        now,
                        title,
                        (t.get("description") or "").strip(),
                        json.dumps(sources, ensure_ascii=False),
                        int(t.get("source_count") or len(sources)),
                        block,
                        (t.get("audience") or "").strip(),
                        (t.get("goal") or "").strip(),
                        (t.get("cta") or "").strip(),
                        json.dumps(reasons, ensure_ascii=False),
                        (t.get("article_type") or "").strip(),
                    ),
                )
                ids.append(int(cur.lastrowid))
            await db.commit()
        logger.info("Сохранено тем в БД: %d", len(ids))
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
            rows = [_parse_topic_row(dict(r)) for r in await cur.fetchall()]
            return rows
    except Exception as exc:
        logger.error("Ошибка чтения тем: %s", exc)
        return []


async def get_topic(topic_id: int) -> Optional[dict]:
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM topics WHERE id = ?", (topic_id,))
            row = await cur.fetchone()
            return _parse_topic_row(dict(row)) if row else None
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


async def save_expertise(
    article_id: int,
    final_text: str,
    report: str,
    rating: int,
    recommendation: str,
) -> None:
    """Сохраняет результаты экспертизы статьи (финальный текст, отчёт, рейтинг, рекомендацию)."""
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE articles SET final_content = ?, expert_report = ?, "
                "expert_rating = ?, expert_recommendation = ? WHERE id = ?",
                (final_text, report, rating, recommendation, article_id),
            )
            await db.commit()
        logger.info(
            "Экспертиза сохранена для статьи %s (рейтинг %s)", article_id, rating
        )
    except Exception as exc:
        logger.error("Ошибка сохранения экспертизы статьи %s: %s", article_id, exc)


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
# 4. LLM: ИЗВЛЕЧЕНИЕ ОБЩИХ ТЕМ ИЗ СЫРЫХ ТРЕНДОВ
# ========================================================================

_TOPICS_PROMPT = """Ты — эксперт по трендам в IT и контент-маркетингу для B2B-продуктов. Целевая аудитория: небольшие команды, стартапы, разработчики, которым важны командная работа, скорость разработки, корпоративные мессенджеры и контроль задач. Продукт — корпоративный мессенджер с контролем задач (MVP).

На основе списка свежих IT-новостей из разных источников (русский, английский, китайский и другие языки) сформируй темы для статей НА РУССКОМ ЯЗЫКЕ. Независимо от языка источника, все темы, описания, названия блоков — ТОЛЬКО на русском. Английскими остаются только названия технологий (Python, React, Docker) и URL.

Темы НЕ копируют заголовки новостей — они формируются на основе интересов аудитории, текущих тенденций и бизнес-целей продукта.

Темы должны:
- Повышать конверсию для сервиса корпоративного мессенджера (подписка, демо, регистрация)
- Привлекать разработчиков и технических специалистов (показывать экспертизу, процессы, стек)
- Опираться на реальные тренды из собранных источников

Темы делятся на блоки (минимум 5):
1. Блок 'Обучающие' — 2-3 темы (туториалы, гайды, разбор паттернов программирования, основы архитектуры)
2. Блок 'Информирующие' — 2-3 темы (новости, тренды, обновления в IT-мире)
3-5. Дополнительные блоки — названия формируешь ты (например, 'Архитектура', 'Продуктовый менеджмент', 'DevOps', 'Командная работа'). В каждом 2-3 темы.

Всего блоков не больше 5. В каждом блоке 2-3 темы.

Для каждой темы укажи:
- topic: заголовок темы (продающий, но без кликбейта, на русском)
- description: описание (о чём статья, для кого, какой результат получит читатель, на русском)
- audience: целевая аудитория (например, 'тимлиды и CTO', 'мидл-разработчики на React/Node.js')
- goal: основная цель статьи (например, 'показать ценность интеграции мессенджера с таск-трекером')
- cta: ключевой CTA в конце статьи (например, 'запросить демо', 'присоединиться к команде')
- reasons: 3-4 тезиса, почему тема актуальна сейчас (укажи тренд или событие: релиз, исследование, рост интереса)
- article_type: тип статьи (кейс, инструкция, сравнение, обзор, FAQ, разбор ошибки и т.д.)
- sources: массив оригинальных URL статей-источников, на которых основана тема (НЕ переводятся)

Верни СТРОГО JSON:
{"blocks": [{"block_name": "Обучающие", "themes": [{"topic": "...", "description": "...", "audience": "...", "goal": "...", "cta": "...", "reasons": ["...", "..."], "article_type": "...", "sources": ["url1", "url2"]}]}, {"block_name": "Информирующие", "themes": [...]}]}

Не используй общие формулировки вроде 'разберём, как это работает'. Каждый пункт должен быть конкретным и измеримым."""


def _norm_url(url: str) -> str:
    return (url or "").strip().rstrip("/")


def _fallback_topics(raw_items: list[dict]) -> dict:
    """Fallback без LLM: возвращает 5 блоков тем с русскими названиями.

    Всегда отдаёт блоки «Обучающие», «Информирующие», «Архитектура»,
    «Командная работа» и «Продуктовый менеджмент» (по 2 темы в каждом),
    чтобы веб-панель работала единообразно с LLM-веткой. Каждая тема содержит
    полный набор полей (audience, goal, cta, reasons, article_type, sources).
    В sources подставляются реальные URL собранных статей (первые найденные),
    чтобы темы можно было использовать для генерации статьи и фактчекинга.
    """
    urls = [u for u in (_norm_url(it.get("url", "")) for it in raw_items) if u]

    def _sources(start: int, count: int) -> list[str]:
        return urls[start:start + count]

    def _theme(*, topic: str, description: str, audience: str, goal: str, cta: str,
               reasons: list[str], article_type: str, start: int, count: int) -> dict:
        srcs = _sources(start, count)
        return {
            "topic": topic,
            "description": description,
            "audience": audience,
            "goal": goal,
            "cta": cta,
            "reasons": reasons,
            "article_type": article_type,
            "sources": srcs,
            "source_count": len(srcs),
        }

    blocks = [
        {
            "block_name": "Обучающие",
            "themes": [
                _theme(
                    topic="Паттерн Event Sourcing в корпоративных мессенджерах: как хранить историю чата",
                    description=("Пошаговое руководство: как Event Sourcing решает проблемы синхронизации "
                                 "чатов и контроля задач, с готовой схемой событий и примерами модели хранения. "
                                 "Читатель получит рабочую архитектуру событий для real-time чата."),
                    audience="бэкенд-разработчики на Python/Go и тимлиды",
                    goal="показать инженерную экспертизу команды и привлечь разработчиков в проект",
                    cta="присоединиться к команде",
                    reasons=[
                        "Темы событийных архитектур стабильно входят в топ HackerNews",
                        "Рост спроса на real-time коммуникацию в B2B-сегменте",
                    ],
                    article_type="инструкция",
                    start=0,
                    count=2,
                ),
                _theme(
                    topic="Туториал: интеграция таск-трекера с чатом через webhooks",
                    description=("Практический туториал с кодом: как связать задачи Jira или Trello "
                                 "с корпоративным чатом через webhooks, чтобы уведомления шли в один канал. "
                                 "Результат — рабочий прототип интеграции за вечер."),
                    audience="мидл-разработчики на JavaScript/Python",
                    goal="показать ценность интеграции мессенджера с таск-трекером",
                    cta="запросить демо",
                    reasons=[
                        "GitHub и Jira регулярно обновляют webhook-API",
                        "Растёт число распределённых команд, которым нужен единый канал задач",
                    ],
                    article_type="туториал",
                    start=1,
                    count=2,
                ),
            ],
        },
        {
            "block_name": "Информирующие",
            "themes": [
                _theme(
                    topic="Тренды корпоративных мессенджеров 2026: что выбирают команды",
                    description=("Обзор актуальных трендов: от AI-ассистентов до встроенных таск-трекеров. "
                                 "Для тимлидов — критерии выбора инструмента, для продактов — направления "
                                 "развития продукта."),
                    audience="CTO, тимлиды и product-менеджеры",
                    goal="позиционировать наш мессенджер как современное решение с контролем задач",
                    cta="подписаться на обновления",
                    reasons=[
                        "Свежие отчёты о Slack, Teams и российских корпоративных мессенджерах",
                        "Рост интереса к импортозамещению корпоративных сервисов",
                    ],
                    article_type="обзор",
                    start=2,
                    count=2,
                ),
                _theme(
                    topic="Что нового в AI-функциях коммуникационных платформ за месяц",
                    description=("Сводка релизов AI-функций в мессенджерах и трекерах задач за последний месяц: "
                                 "авторезюме встреч, умные ответы, семантический поиск. Для тех, кто следит "
                                 "за конкурентной средой."),
                    audience="product-менеджеры и аналитики",
                    goal="показать экспертизу в анализе рынка и выделить наш стек на фоне конкурентов",
                    cta="запросить демо",
                    reasons=[
                        "Крупные платформы анонсируют AI-фичи ежемесячно",
                        "Растут запросы на автоматизацию рутины в командных инструментах",
                    ],
                    article_type="дайджест",
                    start=3,
                    count=2,
                ),
            ],
        },
        {
            "block_name": "Архитектура",
            "themes": [
                _theme(
                    topic="Монолит vs микросервисы для MVP мессенджера: считаем стоимость",
                    description=("Разбор с цифрами: сколько стоят две схемы архитектуры для MVP на 10 000 "
                                 "пользователей, какие риски у каждой и когда переход на микросервисы оправдан."),
                    audience="архитекторы и CTO стартапов",
                    goal="показать экспертизу в проектировании и подвести к обсуждению нашего стека",
                    cta="присоединиться к команде",
                    reasons=[
                        "Инженеры крупных мессенджеров публикуют мемуары о цене микросервисов",
                        "В стартапах растёт интерес к lean-подходам к архитектуре",
                    ],
                    article_type="разбор ошибок",
                    start=4,
                    count=2,
                ),
                _theme(
                    topic="WebSocket vs SSE в real-time чатах: выбираем транспорт",
                    description=("Сравнение WebSocket, SSE и WebRTC DataChannel: задержки, нагрузка на сервер, "
                                 "поддержка в браузерах. Читатель получит таблицу выбора под свой сценарий."),
                    audience="мидл и сеньор-разработчики бэкенда и фронтенда",
                    goal="продемонстрировать глубокое знание стека real-time коммуникаций",
                    cta="запросить демо",
                    reasons=[
                        "Вопрос выбора транспорта регулярно обсуждается в профильных сообществах",
                        "Статистика CDN фиксирует рост real-time трафика",
                    ],
                    article_type="сравнение",
                    start=5,
                    count=2,
                ),
            ],
        },
        {
            "block_name": "Командная работа",
            "themes": [
                _theme(
                    topic="Контроль задач без тирании трекеров: практика команд до 20 человек",
                    description=("Кейс: какие практики управления задачами работают в небольших командах, как "
                                 "совмещать чат и трекер и не терять контекст. Результат — чек-лист внедрения."),
                    audience="тимлиды и владельцы небольших продуктовых команд",
                    goal="показать, что наш мессенджер решает реальную боль контроля задач",
                    cta="зарегистрироваться в бета-версию",
                    reasons=[
                        "Исследования фиксируют потери времени на переключение между инструментами",
                        "Команды сокращают число рабочих инструментов",
                    ],
                    article_type="кейс",
                    start=6,
                    count=2,
                ),
                _theme(
                    topic="Удалёнка и синхронизация: как чат-боты экономят часы в неделю",
                    description=("FAQ-разбор: какие рутинные операции можно отдать боту в корпоративном чате, "
                                 "как это влияет на скорость ответа команды и какие метрики улучшаются за месяц."),
                    audience="руководители команд и HR-tech специалисты",
                    goal="привлечь аудиторию, которая выбирает инструменты для команды",
                    cta="запросить демо",
                    reasons=[
                        "Доля удалённой занятости в IT продолжает расти",
                        "Компании публикуют кейсы автоматизации командной рутины",
                    ],
                    article_type="FAQ",
                    start=7,
                    count=2,
                ),
            ],
        },
        {
            "block_name": "Продуктовый менеджмент",
            "themes": [
                _theme(
                    topic="Метрики вовлечённости в корпоративном мессенджере: что мерять на MVP",
                    description=("Разбор метрик: DAU/WAU, время до первого действия, доля чатов с задачами. "
                                 "Для продактов — система метрик с целевыми значениями и способами их достижения."),
                    audience="product-менеджеры и аналитики",
                    goal="показать продуктовое мышление команды и вовлечь продактов в диалог",
                    cta="подписаться на обновления",
                    reasons=[
                        "Растёт число публикаций о growth-метриках B2B SaaS",
                        "Опыт крупных мессенджеров подтверждает важность ранних метрик",
                    ],
                    article_type="разбор метрик",
                    start=8,
                    count=2,
                ),
                _theme(
                    topic="От идеи до MVP за 90 дней: план запуска командного продукта",
                    description=("Пошаговый план запуска MVP корпоративного мессенджера: этапы, ресурсы, "
                                 "критерии готовности. Читатель получит шаблон дорожной карты и список типичных ошибок."),
                    audience="фаундеры стартапов и продакты",
                    goal="выстроить доверие к команде продукта и привлечь ранних пользователей",
                    cta="зарегистрироваться в бета-версию",
                    reasons=[
                        "Тренд на быстрые запуски MVP в B2B-сегменте",
                        "Растёт число инди-стартапов вокруг командных инструментов",
                    ],
                    article_type="дорожная карта",
                    start=9,
                    count=2,
                ),
            ],
        },
    ]
    return {"blocks": blocks}


def flatten_blocks(blocks_data: dict) -> list[dict]:
    """Разворачивает структуру {blocks: [...]} в плоский список тем с полем block."""
    flat: list[dict] = []
    for b in (blocks_data or {}).get("blocks") or []:
        if not isinstance(b, dict):
            continue
        block_name = (b.get("block_name") or "").strip()
        for th in (b.get("themes") or []):
            if not isinstance(th, dict):
                continue
            item = dict(th)
            item.setdefault("block", block_name)
            flat.append(item)
    return flat


def _extract_json(raw: str) -> str:
    """Извлекает из ответа LLM самый внешний JSON-объект или массив."""
    positions = {ch: i for i, ch in enumerate(raw) if ch in "[{"}
    if not positions:
        raise ValueError("JSON не найден в ответе LLM")
    pos, opener = min(positions.items(), key=lambda kv: kv[1])
    closer = "]" if opener == "[" else "}"
    end = raw.rfind(closer)
    if end <= pos:
        raise ValueError("JSON не завершён в ответе LLM")
    return raw[pos:end + 1]


def _parse_themes(items: list, known: dict[str, dict]) -> list[dict]:
    """Валидирует список тем из LLM: оставляет темы с реальными источниками.

    known — словарь нормализованных URL собранных статей: url -> исходный item.
    """
    result: list[dict] = []
    for p in items[:10]:
        if not isinstance(p, dict):
            continue
        topic = (p.get("topic") or "").strip()
        if not topic:
            continue
        sources: list[str] = []
        seen: set[str] = set()
        for s in (p.get("sources") or []):
            s_norm = _norm_url(s)
            if s_norm and s_norm in known and s_norm not in seen:
                seen.add(s_norm)
                sources.append(known[s_norm]["url"])
        if not sources:
            # тема без реальных ссылок на статьи бесполезна — пропускаем
            continue
        result.append({
            "topic": topic,
            "description": (p.get("description") or "").strip(),
            "audience": (p.get("audience") or "").strip(),
            "goal": (p.get("goal") or "").strip(),
            "cta": (p.get("cta") or "").strip(),
            "reasons": [str(r).strip() for r in (p.get("reasons") or []) if str(r).strip()],
            "article_type": (p.get("article_type") or "").strip(),
            "sources": sources,
            "source_count": len(sources),
        })
    return result


async def extract_topics(raw_items: list[dict]) -> dict:
    """Группирует сырые статьи из collect_trends в блоки тем через LLM.

    Возвращает структуру:
    {"blocks": [{"block_name": "...", "themes": [{topic, description, audience, goal, cta,
                                                  reasons, article_type, sources, source_count}, ...]}, ...]}
    sources — реальные ссылки на статьи, из которых выведена тема
    (они нужны позже для генерации статьи и фактчекинга).

    Совместимость: если LLM вернул плоский массив тем, он оборачивается
    в один блок «Темы».
    """
    if not raw_items:
        return {"blocks": []}
    if llm is None:
        logger.warning("LLM не настроен — темы формируются по источникам (fallback)")
        return _fallback_topics(raw_items)
    items_text = "\n".join(
        f"{i + 1}. {it['title']} | {it.get('url', '')} | источник: {it.get('source', '')}"
        for i, it in enumerate(raw_items[:60])
    )
    prompt = f"{_TOPICS_PROMPT}\n\nСписок новостей:\n{items_text}"
    try:
        resp = await llm.chat.completions.create(
            model=config.LLM_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.4,
        )
    except Exception as exc:
        logger.error("LLM-извлечение тем не удалось: %s", exc)
        return _fallback_topics(raw_items)
    try:
        raw = resp.choices[0].message.content
        parsed = json.loads(_extract_json(raw))
        known = {_norm_url(it.get("url", "")): it for it in raw_items}

        # Совместимость: плоский массив тем → один блок «Темы»
        if isinstance(parsed, list):
            themes = _parse_themes(parsed, known)
            if not themes:
                raise ValueError("плоский массив без валидных тем")
            logger.info("LLM выделил тем (плоский массив → блок «Темы»): %d", len(themes))
            return {"blocks": [{"block_name": "Темы", "themes": themes}]}

        if not isinstance(parsed, dict):
            raise ValueError("LLM вернул не JSON-объект")

        blocks_raw = parsed.get("blocks")
        if not isinstance(blocks_raw, list):
            # Ещё одна форма совместимости: {"themes": [...]} без блоков
            themes = _parse_themes(parsed.get("themes") or [], known)
            if not themes:
                raise ValueError("LLM не вернул блоки тем")
            return {"blocks": [{"block_name": "Темы", "themes": themes}]}

        blocks: list[dict] = []
        for b in blocks_raw[:5]:  # всего блоков не больше 5
            if not isinstance(b, dict):
                continue
            block_name = (b.get("block_name") or "").strip() or "Темы"
            themes = _parse_themes(b.get("themes") or [], known)
            if not themes:
                continue
            blocks.append({"block_name": block_name, "themes": themes[:3]})
        if not blocks:
            raise ValueError("ни одного блока с валидными темами")
        logger.info("LLM выделил блоков тем: %d", len(blocks))
        return {"blocks": blocks}
    except Exception as exc:
        logger.warning("LLM вернул некорректный ответ, fallback по источникам: %s", exc)
        return _fallback_topics(raw_items)

# ========================================================================
# 5. LLM: ГЕНЕРАЦИЯ ПЛАНОВ СТАТЕЙ
# ========================================================================

def _fallback_plan(topic: dict) -> dict:
    """Шаблонный план без LLM: содержательная структура с тезисами и бизнес-логикой."""
    title = topic.get("title") or "Статья"
    sources = [u for u in (topic.get("sources") or []) if isinstance(u, str)]
    return {
        "title": title,
        "summary": (
            "Обзор темы для целевой аудитории: ключевые проблемы, "
            "аргументы из источников и практические выводы."
        ),
        "conversion_goal": "Запрос демо продукта через CTA в конце статьи.",
        "sections": [
            {
                "heading": "Суть проблемы и почему это важно сейчас",
                "purpose": "Захватить внимание и обозначить боль аудитории",
                "points": [
                    "Ключевая проблема и её масштаб для аудитории",
                    "Почему тема актуальна именно сейчас",
                    "Что читатель получит после прочтения",
                ],
                "content_type": "цифры и факты из источников",
                "estimated_length": "2-3 абзаца",
                "product_link": "Сопоставить боль аудитории с возможностью решить её в мессенджере",
            },
            {
                "heading": "Как это работает: разбор и примеры",
                "purpose": "Дать аргументированный разбор с опорой на источники",
                "points": [
                    "Разбор тезиса с примерами из источников",
                    "Сравнение подходов (если применимо)",
                    "Конкретный кейс или данные",
                ],
                "content_type": "разбор, сравнение, кейс",
                "estimated_length": "4-5 абзацев",
                "product_link": "Показать, где продукт закрывает описанную потребность",
            },
            {
                "heading": "Что делать: практические шаги",
                "purpose": "Перевести выводы в действия читателя",
                "points": [
                    "Пошаговый сценарий применения",
                    "Чек-лист действий",
                    "Возможные ошибки и как их избежать",
                ],
                "content_type": "инструкция, чек-лист",
                "estimated_length": "3-4 абзаца",
                "product_link": "Связать шаги с функциями мессенджера (интеграции, API, автоматизация)",
            },
            {
                "heading": "Выводы",
                "purpose": "Закрепить главную мысль и подвести к CTA",
                "points": [
                    "Главный вывод в одном предложении",
                    "Призыв к целевому действию (CTA)",
                ],
                "content_type": "резюме",
                "estimated_length": "1-2 абзаца",
                "product_link": "Прямой призыв попробовать продукт",
            },
        ],
        "objections": [
            {
                "objection": "Не хочется внедрять ещё один инструмент",
                "counter": "Показать лёгкую интеграцию и быстрый старт без миграций",
            },
            {
                "objection": "Опасения по безопасности данных",
                "counter": "Опираться на источники о защите данных и архитектуре продукта",
            },
        ],
        "sources_to_use": sources,
    }


async def generate_plans(topic: dict, platform_style: str) -> dict:
    """Генерирует детальный план статьи в новой структуре.

    Возвращает ОДИН план с полями title, summary, conversion_goal, sections,
    objections, sources_to_use (вместо старого формата [variant, title, points]).
    """
    if llm is None:
        logger.warning("LLM не настроен — возвращаю шаблонный план")
        return _fallback_plan(topic)
    sources_list = "\n".join(f"- {u}" for u in (topic.get("sources") or []))
    prompt = f"""Ты — главный редактор технологического издания. Тебе даны: заголовок темы, описание, целевая аудитория, цель статьи, CTA, тип статьи и источники. Твоя задача — составить детальный план статьи, который можно сразу отдать писателю и проверить по смыслу. План и все пункты — строго на русском языке.

Данные темы:
- Заголовок: {topic['title']}
- Описание: {topic.get("description") or ""}
- Аудитория: {topic.get("audience") or ""}
- Цель: {topic.get("goal") or ""}
- CTA: {topic.get("cta") or ""}
- Тип статьи: {topic.get("article_type") or ""}
- Источники:
{sources_list}
- Стиль для платформы: {platform_style}

План должен включать:

1. Краткое описание статьи (1-2 предложения): о чём, для кого, какой результат получит читатель.
2. Целевая метрика конверсии: что должно произойти после прочтения (регистрация, запрос демо, отклик на вакансию и т.п.) и где это будет предложено (CTA).
3. Структура по разделам с заголовками H2. Для каждого раздела укажи:
   - Цель раздела (зачем он нужен в статье)
   - 3-5 ключевых тезисов (что именно будет раскрыто)
   - Тип контента: цифры/кейс/код/сравнение/опрос и т.п.
   - Ожидаемый объём (в абзацах)
   - Связь с продуктом (мессенджером): где и как показать ценность (интеграция, безопасность, скорость, удобство, API, автоматизация)
4. Блок 'Риски и возражения': 2-3 возражения аудитории (например, 'боятся утечки данных', 'не хотят ещё один мессенджер') и как каждый будет закрыт в статье.
5. Список источников, которые нужно использовать (из списка выше).

Верни СТРОГО JSON:
{{
  "title": "заголовок плана на русском",
  "summary": "краткое описание (1-2 предложения) на русском",
  "conversion_goal": "метрика конверсии на русском",
  "sections": [
    {{
      "heading": "H2 заголовок раздела на русском",
      "purpose": "цель раздела на русском",
      "points": ["тезис 1", "тезис 2", "тезис 3"],
      "content_type": "тип контента на русском",
      "estimated_length": "объём в абзацах",
      "product_link": "связь с продуктом на русском"
    }}
  ],
  "objections": [
    {{"objection": "возражение на русском", "counter": "как закрыто на русском"}}
  ],
  "sources_to_use": ["url1", "url2"]
}}

Никаких общих фраз без содержания. Каждый раздел должен быть понятен по смыслу без чтения всей статьи."""
    try:
        resp = await llm.chat.completions.create(
            model=config.LLM_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
        )
    except Exception as exc:
        logger.error("LLM-генерация плана не удалась: %s", exc)
        return _fallback_plan(topic)
    try:
        raw = resp.choices[0].message.content
        plan = json.loads(_extract_json(raw))
        if not isinstance(plan, dict):
            raise ValueError("LLM вернул не JSON-объект")
        plan.setdefault("title", topic["title"])
        plan.setdefault("summary", "")
        plan.setdefault("conversion_goal", "")
        plan.setdefault("sections", [])
        plan.setdefault("objections", [])
        plan.setdefault("sources_to_use", topic.get("sources") or [])
        logger.info("Сгенерирован детальный план: %d разделов, %d возражений",
                    len(plan["sections"]), len(plan["objections"]))
        return plan
    except Exception as exc:
        logger.warning("LLM вернул некорректный JSON для плана, fallback: %s", exc)
        return _fallback_plan(topic)

# ========================================================================
# 6. LLM: ГЕНЕРАЦИЯ СТАТЬИ + ФАКТЧЕКИНГ
# ========================================================================

def _plan_to_text(plan: dict, fallback_title: str = "") -> str:
    """Преобразует план (новый или старый формат) в текстовый блок для промпта статьи."""
    lines: list[str] = []
    title = (plan.get("title") or "").strip() or fallback_title
    if title:
        lines.append(f"Заголовок статьи: {title}")
    summary = (plan.get("summary") or "").strip()
    if summary:
        lines.append(f"Краткое описание: {summary}")
    conversion = (plan.get("conversion_goal") or "").strip()
    if conversion:
        lines.append(f"Целевая метрика конверсии: {conversion}")
    sections = plan.get("sections") or []
    for i, s in enumerate(sections, 1):
        if not isinstance(s, dict):
            continue
        heading = (s.get("heading") or "").strip() or f"Раздел {i}"
        lines.append(f"\n{i}. {heading}")
        purpose = (s.get("purpose") or "").strip()
        if purpose:
            lines.append(f"   Цель раздела: {purpose}")
        for p in (s.get("points") or []):
            pts = str(p).strip()
            if pts:
                lines.append(f"   - {pts}")
        ctype = (s.get("content_type") or "").strip()
        if ctype:
            lines.append(f"   Тип контента: {ctype}")
        est = (s.get("estimated_length") or "").strip()
        if est:
            lines.append(f"   Ожидаемый объём: {est}")
        plink = (s.get("product_link") or "").strip()
        if plink:
            lines.append(f"   Связь с продуктом: {plink}")
    objections = plan.get("objections") or []
    if objections:
        lines.append("\nРиски и возражения (и как они закрыты в статье):")
        for o in objections:
            if not isinstance(o, dict):
                continue
            ob = (o.get("objection") or "").strip()
            counter = (o.get("counter") or "").strip()
            if ob or counter:
                lines.append(f"   - {ob} → {counter}")
    # Старый формат: только пункты
    if not sections and plan.get("points"):
        lines.append("\nПункты плана:")
        for i, p in enumerate(plan["points"], 1):
            pts = str(p).strip()
            if pts:
                lines.append(f"  {i}. {pts}")
    return "\n".join(lines) or "Статья без плана."


async def generate_article(topic_id: int, plan: dict, platform_style: str) -> str:
    """Генерирует статью по плану (формат sections + objections); тема с полями
    title, description, audience, goal, cta, article_type, sources загружается
    из БД по topic_id. platform_style зарезервирован для совместимости вызовов."""
    if llm is None:
        logger.warning("LLM не настроен — статья не может быть сгенерирована")
        return ""
    topic = await get_topic(topic_id)
    if not topic:
        logger.error("Тема %s не найдена в БД — генерация статьи невозможна", topic_id)
        return ""
    topic_title = topic.get("title") or ""
    topic_description = (topic.get("description") or "").strip()
    audience = (topic.get("audience") or "").strip()
    goal = (topic.get("goal") or "").strip()
    cta = (topic.get("cta") or "").strip()
    article_type = (topic.get("article_type") or "").strip()
    # Источники: приоритет у списка из плана (sources_to_use), иначе — из темы
    plan_sources = plan.get("sources_to_use") or []
    sources = [u for u in (plan_sources or topic.get("sources") or []) if u]
    sources_list = "\n".join(f"{i + 1}. {u}" for i, u in enumerate(sources))
    plan_json = _plan_to_text(plan, topic_title)
    prompt = f"""Ты — опытный технический писатель и редактор русскоязычного IT-блога. Тебе дан план статьи. Твоя задача — написать статью на русском языке, которая следует структуре плана и раскрывает каждый тезис.

Данные:
- Тема: {topic_title}
- Описание: {topic_description}
- Аудитория: {audience}
- Цель: {goal}
- CTA: {cta}
- Тип статьи: {article_type}
- Источники (используй ссылки на эти ресурсы в тексте):
{sources_list}

План статьи:
{plan_json}

Требования к статье:
1. Статья пишется НА РУССКОМ ЯЗЫКЕ. Английскими остаются только технические термины (названия продуктов, языков программирования, библиотек) и URL.
2. Следуй структуре плана: каждый раздел плана — раздел статьи с заголовком H2.
3. Раскрывай каждый тезис из плана конкретными примерами, цифрами, сравнениями.
4. Если данных нет — используй реалистичные примеры, но помечай 'пример'.
5. Показывай ценность продукта (корпоративный мессенджер) через сценарии использования, а не через рекламу.
6. Закрывай возражения из блока 'Риски и возражения' в соответствующих разделах.
7. Включай CTA в конце статьи и, при необходимости, промежуточные призывы.
8. ОБЯЗАТЕЛЬНО ссылайся на источники в формате [1], [2] и т.д. В конце статьи — список источников с URL.
9. Каждое утверждение должно опираться на источник или помечаться как пример.
10. Объём: 1500-3000 слов.
11. Не используй общие фразы без смысла ('мы живём в эпоху цифровых технологий').
12. Стиль: профессиональный, но понятный разработчикам и менеджерам. Тон — экспертный, без навязчивости.
13. Формат: HTML-разметка (h2, h3, p, ul, li, strong, a). Заголовки — h2, подзаголовки — h3.
14. В конце добавь призыв к сообществу: мы строим корпоративный мессенджер, ищем разработчиков и энтузиастов.

Верни HTML-текст статьи."""
    try:
        resp = await llm.chat.completions.create(
            model=config.LLM_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.6,
            max_tokens=8000,
        )
        content = resp.choices[0].message.content
        if not content:
            raise ValueError("LLM вернул пустой ответ")
        logger.info("Статья сгенерирована: %d символов", len(content))
        return content
    except Exception as exc:
        logger.error("Генерация статьи не удалась: %s", exc)
        return ""


def _parse_expert_json(raw: str, fallback_text: str) -> dict:
    """Парсит JSON-ответ экспертизы LLM; устойчив к markdown-обёрткам ```json …```."""
    text = (raw or "").strip()
    fence = re.search(r"```(?:json)?\s*(.*?)```", text, re.DOTALL)
    if fence:
        text = fence.group(1).strip()
    if not text.startswith("{"):
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end > start:
            text = text[start:end + 1]
    try:
        data = json.loads(text)
    except (TypeError, ValueError):
        logger.warning("Экспертиза: ответ не является валидным JSON, возвращён исходный текст")
        return {
            "report": "⚠️ Ответ экспертизы не удалось разобрать (JSON). Проверь статью вручную.",
            "rating": 0,
            "recommendation": "нужно запросить данные",
            "final_text": fallback_text or "",
        }
    try:
        rating = int(data.get("rating") or 0)
    except (TypeError, ValueError):
        rating = 0
    return {
        "report": str(data.get("report") or "").strip(),
        "rating": max(0, min(rating, 10)),
        "recommendation": str(data.get("recommendation") or "нужно запросить данные").strip(),
        "final_text": str(data.get("final_text") or fallback_text or ""),
    }


async def fact_check(article: str, sources: Optional[list[str]] = None) -> dict:
    """Экспертиза статьи: возвращает исправленную финальную версию с форматированием.

    Возвращает dict: {report, rating, recommendation, final_text}.
    """
    default = {
        "report": "⚠️ Экспертиза недоступна: LLM не настроен (DEEPSEEK_API_KEY).",
        "rating": 0,
        "recommendation": "нужно запросить данные",
        "final_text": article or "",
    }
    if llm is None:
        logger.warning("LLM не настроен — экспертиза недоступна")
        return default
    sources = sources or []
    sources_list = "\n".join(f"{i + 1}. {u}" for i, u in enumerate(sources))
    if not sources_list.strip():
        sources_list = "нет источников"
    prompt = f"""Ты — старший редактор и технический эксперт русскоязычного IT-блога. Тебе дана статья и список реальных источников. Твоя задача — проверить статью и вернуть ИСПРАВЛЕННУЮ финальную версию.

Статья:
{article}

Список РЕАЛЬНЫХ источников (ссылки в статье должны быть только из этого списка):
{sources_list}

Проверь:
1. Фактические ошибки и несоответствия (особенно в описании продукта, API, безопасности, производительности).
2. Логические нестыковки и 'дыры' в аргументации.
3. Соответствие цели статьи и CTA.
4. Релевантность источников и цифр (если указаны).
5. Стилистику, орфографию, пунктуацию.
6. Чёткость и силу CTA.
7. Каждая ссылка [N] ведёт на РЕАЛЬНЫЙ источник из списка, а не на выдуманный URL.

После проверки:
- Исправь все ошибки прямо в тексте.
- В начало добавь краткий отчёт о проверке (что было исправлено, 3-5 пунктов).
- Верни ИСПРАВЛЕННУЮ финальную версию статьи с форматированием (h2, h3, p, ul, li, strong, a).
- Финальный текст должен быть готов к копированию и вставке в блог.
- Текст на русском, английскими остаются только технические термины и URL.

Верни JSON:
{{
  "report": "краткий отчёт о проверке на русском (3-5 пунктов)",
  "rating": 8,
  "recommendation": "готово к публикации" | "требует доработки" | "нужно запросить данные",
  "final_text": "исправленный финальный HTML-текст статьи"
}}

Не пересказывай статью. Только конкретные правки и финальный текст."""
    try:
        resp = await llm.chat.completions.create(
            model=config.LLM_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
            max_tokens=8000,
        )
        content = resp.choices[0].message.content
        if not content:
            raise ValueError("LLM вернул пустой ответ")
        return _parse_expert_json(content, article)
    except Exception as exc:
        logger.error("Экспертиза не удалась: %s", exc)
        default["report"] = "⚠️ Экспертиза временно недоступна (ошибка LLM). Проверь статью вручную."
        return default

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
        data = await extract_topics(items)
        blocks = data.get("blocks") or []
        total = sum(len(b.get("themes") or []) for b in blocks)
        if total:
            await save_topics(data)  # save_topics принимает структуру с блоками
        logger.info("Ежедневная задача завершена: блоков: %d, тем: %d", len(blocks), total)
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

class SelectTopicsRequest(BaseModel):
    topics: list[dict]  # темы: topic, description, audience, goal, cta, reasons, article_type, sources, block


class PlansRequest(BaseModel):
    platform: str = "habr"


class ArticleRequest(BaseModel):
    topic_id: int
    plan_index: int
    platform: str = "habr"


class ArticleRequestV2(BaseModel):
    topic_id: int
    platform: str = "habr"
    plan_title: str
    plan_summary: str = ""
    plan_sections: list[dict] = []   # [{heading, purpose, points, content_type, estimated_length, product_link}]
    plan_objections: list[dict] = []  # [{objection, counter}]
    plan_sources: list[str] = []


class ScheduleRequest(BaseModel):
    time: str  # формат "HH:MM"


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


_TIME_RE = re.compile(r"^([01]\d|2[0-3]):([0-5]\d)$")


def _parse_time(value: str) -> tuple[int, int]:
    """Валидирует время HH:MM (часы 00-23, минуты 00-59), возвращает (hours, minutes)."""
    if not isinstance(value, str):
        raise HTTPException(422, "Время должно быть строкой в формате HH:MM.")
    m = _TIME_RE.match(value.strip())
    if not m:
        raise HTTPException(
            422,
            "Некорректное время: ожидается HH:MM (часы 00-23, минуты 00-59).",
        )
    return int(m.group(1)), int(m.group(2))


def _next_daily_run() -> Optional[str]:
    """Время следующего запуска задачи daily_trends (ISO) или None."""
    if not scheduler:
        return None
    job = scheduler.get_job("daily_trends")
    if job and job.next_run_time:
        return job.next_run_time.isoformat()
    return None


@app.get("/api/settings/schedule")
async def api_get_schedule() -> dict:
    """Текущее время ежедневного сбора трендов и время следующего запуска."""
    return {
        "time": config.TRENDS_TIME,
        "next_daily_run": _next_daily_run(),
        "scheduler_running": bool(scheduler and scheduler.running),
    }


@app.post("/api/settings/schedule")
async def api_set_schedule(payload: ScheduleRequest) -> dict:
    """Меняет время ежедневного сбора трендов без перезапуска контейнера."""
    hour, minute = _parse_time(payload.time)
    time_str = f"{hour:02d}:{minute:02d}"
    if not scheduler:
        raise HTTPException(503, "Планировщик не запущен.")
    try:
        scheduler.reschedule_job(
            "daily_trends", trigger="cron", hour=hour, minute=minute
        )
    except Exception as exc:
        logger.error("Не удалось перепланировать daily_trends: %s", exc)
        raise HTTPException(500, "Не удалось обновить расписание в планировщике.")
    config.TRENDS_TIME = time_str
    logger.info("Расписание ежедневного автосбора обновлено: %s", time_str)
    return {"ok": True, "time": time_str, "next_daily_run": _next_daily_run()}


@app.get("/api/platforms")
async def api_platforms() -> dict:
    return {"platforms": config.PLATFORMS}


@app.post("/api/trends/collect")
async def api_collect() -> dict:
    global pending_items, pending_topics
    logger.info("Запрос на сбор трендов из веб-панели")
    items = await collect_trends()
    if not items:
        raise HTTPException(502, "Не удалось собрать тренды. Проверь логи и прокси.")
    pending_items = items  # сырые тренды сохраняем для возможного повторного использования
    data = await extract_topics(items)
    pending_topics = data
    blocks = data.get("blocks") or []
    count = sum(len(b.get("themes") or []) for b in blocks)
    state["last_collect"] = datetime.now().isoformat()
    state["last_collect_count"] = count
    return {"count": count, "blocks": blocks}


@app.post("/api/topics/select")
async def api_select(payload: SelectTopicsRequest) -> dict:
    """Сохраняет выбранные пользователем темы в БД (секция «Темы дня»).

    Каждая тема может содержать поле block (название блока, из которого она
    выбрана); если блока нет — сохраняется как «Темы».
    """
    if not payload.topics:
        raise HTTPException(400, "Ни одна тема не выбрана.")
    clean = []
    for t in payload.topics:
        if not isinstance(t, dict):
            continue
        sources = [s for s in (t.get("sources") or []) if isinstance(s, str)]
        topic = (t.get("topic") or t.get("title") or "").strip()
        if not topic:
            continue
        clean.append({
            "topic": topic,
            "description": (t.get("description") or "").strip(),
            "audience": (t.get("audience") or "").strip(),
            "goal": (t.get("goal") or "").strip(),
            "cta": (t.get("cta") or "").strip(),
            "reasons": [str(r).strip() for r in (t.get("reasons") or []) if str(r).strip()],
            "article_type": (t.get("article_type") or "").strip(),
            "sources": sources,
            "source_count": len(sources),
            "block": (t.get("block") or "").strip(),
        })
    if not clean:
        raise HTTPException(400, "Выбранные темы пустые.")
    ids = await save_topics(clean)
    return {"saved": len(ids), "ids": ids}


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
    logger.info("Генерация плана для темы %s (платформа %s)", topic_id, payload.platform)
    plan = await generate_plans(topic, pf["style"])
    plans_store[topic_id] = plan
    return {
        "topic": topic,
        "platform": payload.platform,
        "platform_name": pf["name"],
        "style": pf["style"],
        "plan": plan,
    }


@app.post("/api/articles/generate")
async def api_generate(payload: ArticleRequest) -> dict:
    topic = await get_topic(payload.topic_id)
    if not topic:
        raise HTTPException(404, "Тема не найдена")
    plan = plans_store.get(payload.topic_id)
    if not plan:
        raise HTTPException(400, "Сначала сгенерируй план для этой темы.")
    pf = config.PLATFORM_MAP.get(payload.platform)
    if not pf:
        raise HTTPException(400, f"Неизвестная платформа: {payload.platform}")
    logger.info("Генерация статьи для темы %s (план из планов, платформа %s)",
                payload.topic_id, payload.platform)
    article = await generate_article(payload.topic_id, plan, pf["style"])
    if not article:
        raise HTTPException(502, "Не удалось сгенерировать статью. Проверь LLM-ключ и сеть.")
    expert = await fact_check(article, topic.get("sources") or [])
    article_id = await save_article(payload.topic_id, payload.platform, article)
    factcheck_store[article_id] = expert.get("report") or ""
    return {
        "id": article_id,
        "topic_title": topic["title"],
        "platform": payload.platform,
        "platform_name": pf["name"],
        "plan_variant": "custom",
        "plan_title": plan.get("title"),
        "article": article,
        "factcheck": expert.get("report") or "",
    }


@app.post("/api/articles/generate-v2")
async def api_generate_v2(payload: ArticleRequestV2) -> dict:
    """Генерация статьи по отредактированному плану в новом формате
    (plan_title, plan_summary, plan_sections, plan_objections, plan_sources).
    Фактчекинг здесь не запускается — это отдельный шаг экспертизы."""
    topic = await get_topic(payload.topic_id)
    if not topic:
        raise HTTPException(404, "Тема не найдена")
    pf = config.PLATFORM_MAP.get(payload.platform)
    if not pf:
        raise HTTPException(400, f"Неизвестная платформа: {payload.platform}")
    plan_title = (payload.plan_title or "").strip()
    if not plan_title:
        raise HTTPException(400, "Заголовок плана (plan_title) обязателен.")
    if not payload.plan_sections:
        raise HTTPException(400, "План пуст: добавь хотя бы один раздел (plan_sections).")
    plan = {
        "title": plan_title,
        "summary": (payload.plan_summary or "").strip(),
        "sections": [s for s in payload.plan_sections if isinstance(s, dict)],
        "objections": [o for o in payload.plan_objections if isinstance(o, dict)],
        "sources_to_use": [u for u in (payload.plan_sources or []) if u],
    }
    logger.info("Генерация статьи v2 для темы %s (новый формат плана, платформа %s)",
                payload.topic_id, payload.platform)
    article = await generate_article(payload.topic_id, plan, pf["style"])
    if not article:
        raise HTTPException(502, "Не удалось сгенерировать статью. Проверь LLM-ключ и сеть.")
    article_id = await save_article(payload.topic_id, payload.platform, article)
    return {
        "id": article_id,
        "topic_title": topic["title"],
        "platform": payload.platform,
        "plan_title": plan["title"],
        "article": article,
    }


@app.get("/api/articles")
async def api_articles() -> dict:
    articles = await list_articles(50)
    for a in articles:
        # Фронтенд работает с ключом article (в БД колонка content)
        if "content" in a and not a.get("article"):
            a["article"] = a["content"]
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


@app.post("/api/articles/{article_id}/expertise")
async def api_expertise(article_id: int) -> dict:
    """Экспертиза статьи: отдельный шаг после генерации.

    Загружает статью и источники темы, вызывает fact_check(), сохраняет
    final_text/report/rating/recommendation в БД и возвращает их клиенту.
    """
    art = await get_article(article_id)
    if not art:
        raise HTTPException(404, "Статья не найдена")
    topic = await get_topic(art["topic_id"]) if art.get("topic_id") else None
    sources = (topic or {}).get("sources") or []
    logger.info("Запуск экспертизы статьи %s (источников: %d)", article_id, len(sources))
    result = await fact_check(art["content"] or "", sources)
    final_text = result.get("final_text") or art["content"] or ""
    report = result.get("report") or ""
    rating = int(result.get("rating") or 0)
    recommendation = result.get("recommendation") or "нужно запросить данные"
    await save_expertise(article_id, final_text, report, rating, recommendation)
    await update_article_status(article_id, "expertised")
    return {
        "article_id": article_id,
        "report": report,
        "rating": rating,
        "recommendation": recommendation,
        "final_text": final_text,
    }


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
