"""
Trend Scanner — веб-панель сбора и генерации контента (без Telegram-бота).

Собирает IT-тренды (HackerNews, GitHub Trending, Google News RSS),
ранжирует темы через LLM (AITUNNEL / DeepSeek), генерирует планы статей
и сами статьи, проводит фактчекинг и публикует результат на Telegraph.

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


# LLM: OpenAI-совместимый клиент AITUNNEL (api.aitunnel.ru), поверх моделей
# DeepSeek и др. Ключ — строго из config.LLM_API_KEY (= DEEPSEEK_API_KEY из
# окружения, без хардкода). AITUNNEL — российский провайдер, доступен напрямую,
# поэтому ходим БЕЗ прокси: sing-box на 127.0.0.1:8080 отвечает 405 на CONNECT
# и валит все LLM-запросы. Если ключ не задан — llm=None, все LLM-функции
# деградируют до fallback-веток.
if config.LLM_API_KEY:
    llm: Optional[AsyncOpenAI] = AsyncOpenAI(
        api_key=config.LLM_API_KEY,
        base_url=config.LLM_BASE_URL,
        timeout=120.0,
        max_retries=2,
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
        article_type TEXT,
        platform TEXT DEFAULT 'blog',
        tone TEXT,
        max_product_mention TEXT,
        volume TEXT,
        relevance_theses TEXT
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
        ("platform", "TEXT DEFAULT 'blog'"),
        ("tone", "TEXT"),
        ("max_product_mention", "TEXT"),
        ("volume", "TEXT"),
        ("relevance_theses", "TEXT"),
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
    """Превращает строку topics из БД в dict с распарсенными JSON-полями.

    Парсятся sources, reasons и relevance_theses (JSON-массивы). Если колонка
    ещё не добавлена миграцией в старой БД — значение остаётся списком [].
    """
    row = dict(row)
    for key in ("sources", "reasons", "relevance_theses"):
        try:
            row[key] = json.loads(row.get(key) or "[]")
        except (TypeError, ValueError):
            row[key] = []
    return row


async def save_topics(topics) -> list[int]:
    """Сохраняет темы в БД и возвращает их id.

    Принимает темы в формате extract_topics:
    - структуру с блоками: {"blocks": [{"block_name": ..., "themes": [...]}]}, либо
    - плоский список тем {topic, description, platform, audience, goal, cta, tone,
      max_product_mention, volume, reasons, relevance_theses, article_type,
      sources (list[str]), source_count, block}.

    Поля reasons и relevance_theses взаимозаменяемы: если тема пришла только
    с одним из них, оно дублируется во вторую колонку для обратной совместимости
    со старыми клиентами панели.
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
                reasons = [str(r).strip() for r in (t.get("reasons") or t.get("relevance_theses") or []) if str(r).strip()]
                theses = [str(r).strip() for r in (t.get("relevance_theses") or t.get("reasons") or []) if str(r).strip()]
                cur = await db.execute(
                    "INSERT INTO topics "
                    "(date, title, description, sources, source_count, block, status, "
                    "audience, goal, cta, reasons, article_type, "
                    "platform, tone, max_product_mention, volume, relevance_theses) "
                    "VALUES (?, ?, ?, ?, ?, ?, 'new', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
                        (t.get("platform") or "blog").strip(),
                        (t.get("tone") or "").strip(),
                        (t.get("max_product_mention") or "").strip(),
                        (t.get("volume") or "").strip(),
                        json.dumps(theses, ensure_ascii=False),
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

_TOPICS_PROMPT = """Ты — эксперт по трендам в IT и контент-маркетингу для B2B SaaS. Продукт — QuickCom, корпоративный мессенджер с интеграциями (Jira, GitHub, CI/CD) и мультитенантностью. Формат: build in public — фаундер открыто показывает процесс, грабли и решения. MVP сгенерирован DeepSeek, вручную доработан. Это признаётся открыто как сильная сторона.

На основе свежих IT-новостей сформируй темы для статей НА РУССКОМ ЯЗЫКЕ. Независимо от языка источника — только русский. Английскими — только названия технологий и URL.

ЦЕЛИ: 1) комьюнити, 2) соавторы, 3) тренд, 4) доверие, 5) демо (позже).

АУДИТОРИИ:
- CIO/CTO и тимлиды — внедрение, безопасность (152-ФЗ), альтернативы Slack/Teams
- Мидл-разработчики (React, Node.js, Docker, FastAPI) — соавторство, стек, AI-код
- HR по найму IT — стек и культура
- Zero-coder/low-code — «как из идеи сделать продукт с AI»

ПЛОЩАДКИ:
- blog — полная версия, все CTA, SEO, 6000-12000 знаков
- habr — код, архитектура, trade-off, AI-код. Продукт — 1 абзац. Код-фёрст. 8000-12000 знаков
- vc — история фаундера, без выдуманных метрик, продукт 10%, без «купить». 5000-8000 знаков
- tproger — туториалы, AI-инструменты, «я тоже учусь». 6000-10000 знаков
- tenchat — одна мысль, 3-5 абзацев, 500-1000 знаков
- telegram — дневник, анонс, вакансия, до 1000 знаков

AI-КОД КЛАСТЕР (1-2 темы обязательно):
- Что AI сгенерировал правильно, а что переписал
- Как тестировать AI-код через pytest
- Где AI экономит время, где создаёт долг
- Diff-разбор ошибок AI

5 блоков (7-10 тем):
1. Обучающие (1-2) — tproger, blog
2. Технические разборы (2-3) — habr, blog
3. Истории фаундера (1-2) — vc, blog
4. Экспертные посты (1-2) — tenchat
5. Анонсы и найм (1-2) — telegram

Поля каждой темы:
- topic — заголовок (русский, без кликбейта)
- platform — blog/habr/vc/tproger/tenchat/telegram
- audience — одна из 4 аудиторий
- goal — цель из 5 целей
- cta — из CTA-матрицы под площадку
- tone — тон статьи
- max_product_mention — лимит упоминания продукта
- volume — объём в знаках
- relevance_theses — 3-4 тезиса актуальности
- article_type — тип статьи
- sources — массив URL

JSON:
{
  "blocks": [
    {
      "block_name": "Обучающие",
      "themes": [
        {
          "topic": "...",
          "platform": "...",
          "audience": "...",
          "goal": "...",
          "cta": "...",
          "tone": "...",
          "max_product_mention": "...",
          "volume": "...",
          "relevance_theses": ["...", "...", "..."],
          "article_type": "...",
          "sources": ["url1"]
        }
      ]
    }
  ]
}

Без общих формулировок. Обязательно 1-2 темы про AI-генерацию кода."""


def _norm_url(url: str) -> str:
    return (url or "").strip().rstrip("/")


def _fallback_topics(raw_items: list[dict]) -> dict:
    """Fallback без LLM: возвращает 5 блоков тем QuickCom по стратегии build in public.

    Блоки — «Обучающие», «Технические разборы», «Истории фаундера»,
    «Экспертные посты» и «Анонсы и найм» (суммарно 7 тем), чтобы веб-панель
    работала единообразно с LLM-веткой. Каждая тема содержит полный набор полей
    (platform, audience, goal, cta, tone, max_product_mention, volume,
    relevance_theses, article_type, sources). В sources подставляются реальные
    URL собранных статей (первые найденные), чтобы темы можно было использовать
    для генерации статьи и фактчекинга. Обязательно 1-2 темы из AI-код кластера.
    """
    urls = [u for u in (_norm_url(it.get("url", "")) for it in raw_items) if u]

    def _sources(start: int, count: int) -> list[str]:
        return urls[start:start + count]

    def _theme(*, topic: str, description: str, platform: str, audience: str,
               goal: str, cta: str, tone: str, max_product_mention: str,
               volume: str, relevance_theses: list[str], article_type: str,
               start: int, count: int) -> dict:
        srcs = _sources(start, count)
        return {
            "topic": topic,
            "description": description,
            "platform": platform,
            "audience": audience,
            "goal": goal,
            "cta": cta,
            "tone": tone,
            "max_product_mention": max_product_mention,
            "volume": volume,
            "reasons": relevance_theses,          # обратная совместимость
            "relevance_theses": relevance_theses,
            "article_type": article_type,
            "sources": srcs,
            "source_count": len(srcs),
        }

    blocks = [
        {
            "block_name": "Обучающие",
            "themes": [
                _theme(
                    topic="Тестирование AI-сгенерированного кода: что проверять через pytest",
                    description=("Пошаговая инструкция: как тестировать AI-сгенерированный код через pytest — "
                                 "какие тесты писать в первую очередь, где AI экономит время, а где создаёт долг. "
                                 "Разбор на примере FastAPI-бэкенда мессенджера, собранного с помощью DeepSeek."),
                    platform="tproger",
                    audience="мидл-разработчики на React/Node.js",
                    goal="прощупать тренд AI-кода и показать стек проекта через обучающий контент",
                    cta="применить подход в своём проекте",
                    tone="обучающий, пошаговый, «я тоже учусь»",
                    max_product_mention="один из инструментов",
                    volume="6000-10000 знаков",
                    relevance_theses=[
                        "pytest остаётся стандартом тестирования Python-бэкендов",
                        "AI-генерация кода без тестов незаметно создаёт технический долг",
                        "Растёт спрос на практики проверки и ревью AI-кода",
                    ],
                    article_type="инструкция",
                    start=0,
                    count=2,
                ),
            ],
        },
        {
            "block_name": "Технические разборы",
            "themes": [
                _theme(
                    topic="Как DeepSeek сгенерировал MVP мессенджера и что пришлось переписать вручную",
                    description=("Технический разбор с кодом в формате build in public: что DeepSeek сгенерировал "
                                 "правильно, что пришлось переписывать вручную, diff-разбор ошибок AI в проде "
                                 "и что дала ручная доработка MVP."),
                    platform="habr",
                    audience="мидл-разработчики на React/Node.js",
                    goal="привлечь соавторов через прозрачный разбор AI-генерации кода и укрепить доверие",
                    cta="посмотреть diff исправлений",
                    tone="технический, код-фёрст",
                    max_product_mention="1 абзац в выводах",
                    volume="8000-12000 знаков",
                    relevance_theses=[
                        "AI-генерация кода стала массовой, но доверие падает без разбора ошибок",
                        "Растёт число публикаций о качестве AI-кода на Habr и HackerNews",
                        "Команды ищут практики ревью и тестирования AI-сгенерированного кода",
                    ],
                    article_type="технический разбор с кодом",
                    start=1,
                    count=2,
                ),
                _theme(
                    topic="Почему open-source мессенджеры — не всегда лучший выбор для команды",
                    description=("Trade-off: сравнение open-source мессенджеров (Mattermost, Rocket.Chat) и SaaS — "
                                 "стоимость владения, интеграции с Jira и GitHub, мультитенантность, безопасность "
                                 "и требования 152-ФЗ."),
                    platform="blog",
                    audience="CIO/CTO и тимлиды",
                    goal="закрепить доверие и подтолкнуть к обсуждению архитектуры через честный trade-off",
                    cta="обсудить архитектуру",
                    tone="аналитический, trade-off",
                    max_product_mention="контекст решения, не более 20% текста",
                    volume="6000-12000 знаков",
                    relevance_theses=[
                        "Open-source мессенджеры требуют затрат на поддержку и доработку",
                        "Ужесточение требований к безопасности данных (152-ФЗ) меняет критерии выбора",
                        "Команды сравнивают TCO open-source и SaaS-решений",
                    ],
                    article_type="сравнение",
                    start=2,
                    count=2,
                ),
                _theme(
                    topic="Мультитенантность в FastAPI: изоляция данных в SaaS-мессенджере",
                    description=("Технический разбор с кодом: как реализована мультитенантность в QuickCom — "
                                 "middleware определения тенанта, SQLAlchemy-сессии с фильтрацией, "
                                 "Alembic-миграции для схем."),
                    platform="habr",
                    audience="мидл-разработчики на React/Node.js",
                    goal="показать инженерную глубину проекта и привлечь соавторов через карьерную страницу",
                    cta="обсудить решение",
                    tone="технический, код-фёрст",
                    max_product_mention="1 абзац в выводах",
                    volume="8000-12000 знаков",
                    relevance_theses=[
                        "Мультитенантность регулярно попадает в топ Habr и HackerNews",
                        "Ошибки изоляции арендаторов приводят к утечкам данных — тема безопасности растёт",
                        "SaaS-команды ищут практики изоляции данных между клиентами",
                    ],
                    article_type="технический разбор с кодом",
                    start=3,
                    count=2,
                ),
            ],
        },
        {
            "block_name": "Истории фаундера",
            "themes": [
                _theme(
                    topic="Нет опыта в IT, но есть рабочий MVP. Как я строю мессенджер в открытую",
                    description=("История фаундера build in public: программирование как хобби, 4 хакатона в роли "
                                 "тимлида, отсутствие коммерческого опыта, рабочий MVP на сервере. Без выдуманных "
                                 "метрик — только то, что планирую измерять."),
                    platform="vc",
                    audience="zero-coder/low-code",
                    goal="собрать комьюнити и найти первых тестеров через честную историю без выдуманных метрик",
                    cta="рассказать о проекте коллегам",
                    tone="личная история, без пафоса",
                    max_product_mention="в контексте, не более 10% текста",
                    volume="5000-8000 знаков",
                    relevance_theses=[
                        "Растёт интерес к историям solo-фаундеров, строящих продукт в открытую",
                        "AI-генерация кода снижает порог входа в разработку MVP",
                        "Формат build in public собирает обратную связь и первых пользователей",
                    ],
                    article_type="история фаундера",
                    start=4,
                    count=2,
                ),
            ],
        },
        {
            "block_name": "Экспертные посты",
            "themes": [
                _theme(
                    topic="Инсайт с zero-coder митапа: AI-генерация меняет подход к MVP",
                    description=("Экспертный пост: одна мысль о том, как AI-генерация кода меняет подход к созданию "
                                 "MVP, и что это значит для тех, кто начинает с нуля. Одна мысль, 3-5 абзацев, "
                                 "без копипаста."),
                    platform="tenchat",
                    audience="zero-coder/low-code",
                    goal="собрать комьюнити через нетворкинг и обсуждение тренда AI-генерации",
                    cta="обсудить сценарии",
                    tone="экспертный пост, одна мысль",
                    max_product_mention="упоминание одной фразой в конце",
                    volume="500-1000 знаков",
                    relevance_theses=[
                        "AI-генерация кода — главная тема zero-coder митапов",
                        "Порог входа в разработку MVP продолжает снижаться",
                        "Растёт интерес к формату build in public среди начинающих команд",
                    ],
                    article_type="экспертный пост",
                    start=5,
                    count=2,
                ),
            ],
        },
        {
            "block_name": "Анонсы и найм",
            "themes": [
                _theme(
                    topic="Ищем соавтора на React: стек, процессы, формат build in public",
                    description=("Краткий анонс: ищем соавтора на React (Storybook, Webpack, clsx), показываем "
                                 "процессы (Code Review, CI/CD) и формат build in public. Стек и открытые задачи — "
                                 "конкретно, без общих фраз."),
                    platform="telegram",
                    audience="мидл-разработчики на React/Node.js",
                    goal="найти соавтора через открытую вакансию с конкретикой стека и задач",
                    cta="взять открытую задачу",
                    tone="дневник разработки, краткий анонс",
                    max_product_mention="упоминание в последнем абзаце",
                    volume="до 1000 знаков",
                    relevance_theses=[
                        "Строить в открытую привлекает разработчиков, которым интересен процесс",
                        "Формат build in public создаёт доверие у потенциальных соавторов",
                        "Конкретика стека и задач повышает отклик на вакансию",
                    ],
                    article_type="вакансия",
                    start=6,
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
    opener, pos = min(positions.items(), key=lambda kv: kv[1])
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
            "platform": (p.get("platform") or "blog").strip(),
            "audience": (p.get("audience") or "").strip(),
            "goal": (p.get("goal") or "").strip(),
            "cta": (p.get("cta") or "").strip(),
            "tone": (p.get("tone") or "").strip(),
            "max_product_mention": (p.get("max_product_mention") or "").strip(),
            "volume": (p.get("volume") or "").strip(),
            "reasons": [str(r).strip() for r in (p.get("reasons") or p.get("relevance_theses") or []) if str(r).strip()],
            "relevance_theses": [str(r).strip() for r in (p.get("relevance_theses") or p.get("reasons") or []) if str(r).strip()],
            "article_type": (p.get("article_type") or "").strip(),
            "sources": sources,
            "source_count": len(sources),
        })
    return result


async def extract_topics(raw_items: list[dict]) -> dict:
    """Группирует сырые статьи из collect_trends в блоки тем через LLM.

    Возвращает структуру:
    {"blocks": [{"block_name": "...", "themes": [{topic, description, platform,
                                                  audience, goal, cta, tone, max_product_mention,
                                                  volume, reasons, relevance_theses, article_type,
                                                  sources, source_count}, ...]}, ...]}
    sources — реальные ссылки на статьи, из которых выведена тема
    (они нужны позже для генерации статьи и фактчекинга).

    Контент-стратегия QuickCom (build in public): ровно 5 блоков —
    «Обучающие», «Технические разборы», «Истории фаундера»,
    «Экспертные посты», «Анонсы и найм» — суммарно 7-10 тем,
    из них обязательно 1-2 темы из AI-код кластера.

    Совместимость: если LLM вернул плоский массив тем, он оборачивается
    в один блок «Темы». Берём до 5 блоков, общее число тем ограничиваем 10.
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
        for b in blocks_raw[:5]:  # контент-стратегия: ровно 5 блоков
            if not isinstance(b, dict):
                continue
            block_name = (b.get("block_name") or "").strip() or "Темы"
            themes = _parse_themes(b.get("themes") or [], known)
            if not themes:
                continue
            blocks.append({"block_name": block_name, "themes": themes[:3]})
        if not blocks:
            raise ValueError("ни одного блока с валидными темами")
        # Стратегия: суммарно 7-10 тем — ограничиваем общий бюджет 10 темами
        budget = 10
        for b in blocks:
            if budget <= 0:
                b["themes"] = []
                continue
            b["themes"] = b["themes"][:budget]
            budget -= len(b["themes"])
        blocks = [b for b in blocks if b["themes"]]
        logger.info("LLM выделил блоков тем: %d", len(blocks))
        return {"blocks": blocks}
    except Exception as exc:
        logger.warning("LLM вернул некорректный ответ, fallback по источникам: %s", exc)
        return _fallback_topics(raw_items)

# ========================================================================
# 5. LLM: ГЕНЕРАЦИЯ ПЛАНОВ СТАТЕЙ
# ========================================================================

def _platform_objections(platform_key: str) -> list[dict]:
    """Возражения по умолчанию, адаптированные под площадку публикации."""
    objections = {
        "habr": [
            {"objection": "Это не масштабируется",
             "counter": "Разобрать архитектуру: горизонтальное масштабирование, изоляция данных, нагрузочные тесты"},
            {"objection": "Уже есть решение в open-source",
             "counter": "Сравнить с open-source: скорость внедрения, поддержка, мультитенантность, безопасность"},
            {"objection": "Зачем свой мессенджер",
             "counter": "Обосновать через контроль данных, интеграции по API и автоматизацию процессов"},
        ],
        "vc": [
            {"objection": "Ещё один инструмент — лишний шум",
             "counter": "Показать измеримые метрики до/после: время, стоимость, ROI"},
            {"objection": "Безопасность данных",
             "counter": "Опираться на источники о защите данных и изоляции в мультитенантной архитектуре"},
            {"objection": "Сложно внедрить",
             "counter": "Описать быстрый старт, обучение команды и миграцию без простоя бизнеса"},
        ],
        "tproger": [
            {"objection": "Это слишком сложно для новичка",
             "counter": "Разбить на пошаговую инструкцию с проверкой результата после каждого шага"},
            {"objection": "Зачем нужен этот шаг",
             "counter": "Объяснить назначение шага и что пойдёт не так, если его пропустить"},
        ],
        "tenchat": [
            {"objection": "Это очевидно",
             "counter": "Подать ключевую мысль через свежие данные или неочевидный факт"},
            {"objection": "Нет времени читать",
             "counter": "Формат 500-1000 знаков: одна мысль, 2-3 аргумента, вывод"},
        ],
        "telegram": [
            {"objection": "Пост слишком длинный",
             "counter": "Уложиться в 1000 знаков: суть, стек, CTA"},
            {"objection": "Непонятно, зачем откликаться",
             "counter": "Чётко указать стек и условия, дать прямой CTA"},
        ],
    }
    return objections.get(platform_key, [])


def _fallback_plan(topic: dict, platform_key: str = "blog") -> dict:
    """Шаблонный план без LLM: содержательная структура, адаптированная под площадку.

    Для tenchat и telegram список источников пустой (не формат площадки).
    """
    title = topic.get("title") or "Статья"
    no_sources = platform_key in ("tenchat", "telegram")
    sources = (
        []
        if no_sources
        else [u for u in (topic.get("sources") or []) if isinstance(u, str)]
    )
    objections = _platform_objections(platform_key) or [
        {
            "objection": "Не хочется внедрять ещё один инструмент",
            "counter": "Показать лёгкую интеграцию и быстрый старт без миграций",
        },
        {
            "objection": "Опасения по безопасности данных",
            "counter": "Опираться на источники о защите данных и архитектуре продукта",
        },
    ]
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
        "objections": objections,
        "sources_to_use": sources,
    }


async def generate_plans(topic: dict, platform_key: str) -> dict:
    """Генерирует детальный план статьи под конкретную площадку публикации.

    Возвращает ОДИН план с полями title, summary, conversion_goal, sections,
    objections, sources_to_use. Для tenchat и telegram sources_to_use — всегда [].
    """
    if llm is None:
        logger.warning("LLM не настроен — возвращаю шаблонный план")
        return _fallback_plan(topic, platform_key)
    topic_title = topic.get("title") or topic.get("topic") or ""
    sources_list = "\n".join(f"- {u}" for u in (topic.get("sources") or []))
    prompt = f"""Ты — главный редактор технологического издания. Составь детальный план статьи под площадку. Строго на русском.

ДАННЫЕ ТЕМЫ:
- Заголовок: {topic_title}
- Описание: {topic.get("description") or ""}
- Платформа: {platform_key}
- Аудитория: {topic.get("audience") or ""}
- Цель: {topic.get("goal") or ""}
- CTA: {topic.get("cta") or ""}
- Тон: {topic.get("tone") or ""}
- Лимит упоминания продукта: {topic.get("max_product_mention") or ""}
- Объём: {topic.get("volume") or ""}
- Тип статьи: {topic.get("article_type") or ""}
- Источники: {sources_list}

КОНТЕКСТ ПРОЕКТА:
- QuickCom — корпоративный мессенджер, build in public
- MVP сгенерирован DeepSeek, вручную доработан — признаётся открыто
- Фаундер — единственный разработчик, нет коммерческого опыта
- 4 хакатона в роли тимлида
- Стек: React, Storybook, Webpack, FastAPI, SQLAlchemy, PostgreSQL, Alembic, pytest, Celery, Redis, MinIO, Docker, Nginx, CI/CD

ПРАВИЛА ПО ПЛОЩАДКАМ:

habr:
- Структура: проблема → разбор решения с кодом → альтернативы (trade-off) → выводы
- Код в каждом разделе (python, typescript, sql, yaml, diff для AI-кода)
- Продукт — только в выводах, 1 абзац
- Фокус: архитектура, грабли, производительность, безопасность, AI-код
- Возражения — технические: «не масштабируется», «уже есть open-source», «зачем свой мессенджер», «AI-код ненадёжный»

vc:
- Структура: проблема (личная история) → что делал → что получилось → чему научился → что планирую
- БЕЗ выдуманных метрик. Вместо цифр: «что планирую измерять и почему»
- Продукт — в контексте, не более 10%
- Без «купить»
- Возражения — бизнес: «ещё один инструмент — шум», «безопасность», «сложно внедрить», «нет опыта — зачем доверять»

tproger:
- Структура: задача → пошаговая инструкция → код с комментариями → проверка → что улучшить
- Продукт — через задачи
- Возражения: «слишком сложно», «зачем этот шаг», «AI всё сделает сам»

tenchat:
- Структура: одна мысль → 2-3 аргумента → вывод
- 3-5 абзацев, 500-1000 знаков
- Продукт не упоминается (или одна фраза в конце)
- Без списка источников

blog:
- Структура: полная версия (техника + продукт + CTA + хроника)
- Продукт естественно вплетён
- SEO: ключевые слова в H2
- Возражения — все типы

telegram:
- Краткий анонс или вакансия
- Для вакансии: стек, формат build in public, ссылка на MVP
- До 1000 знаков

ПЛАН ВКЛЮЧАЕТ:
1. Краткое описание (1-2 предложения)
2. Метрика конверсии: что после прочтения (CTA из матрицы)
3. Разделы H2:
   - Цель раздела
   - 3-5 тезисов (конкретных)
   - Тип контента: код/diff/цифры/trade-off/кейс/хроника
   - Объём (в абзацах)
   - Связь с продуктом (в рамках лимита)
4. Блок «Риски и возражения» (2-3 + контраргумент) — под площадку
5. Список источников (кроме tenchat и telegram)

JSON:
{{
  "title": "...",
  "summary": "...",
  "conversion_goal": "...",
  "sections": [
    {{
      "heading": "H2 заголовок",
      "purpose": "цель раздела",
      "points": ["тезис 1", "тезис 2", "тезис 3"],
      "content_type": "тип контента",
      "estimated_length": "объём",
      "product_link": "связь с продуктом"
    }}
  ],
  "objections": [
    {{"objection": "...", "counter": "..."}}
  ],
  "sources_to_use": ["url1", "url2"]
}}

Для tenchat и telegram — sources_to_use: [] (пустой массив).
Без общих фраз."""
    try:
        resp = await llm.chat.completions.create(
            model=config.LLM_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
        )
    except Exception as exc:
        logger.error("LLM-генерация плана не удалась: %s", exc)
        return _fallback_plan(topic, platform_key)
    try:
        raw = resp.choices[0].message.content
        parsed = json.loads(_extract_json(raw))
        if not isinstance(parsed, dict):
            raise ValueError("LLM вернул не JSON-объект")
        sections = []
        for s in parsed.get("sections") or []:
            if not isinstance(s, dict):
                continue
            sections.append({
                "heading": str(s.get("heading") or "").strip(),
                "purpose": str(s.get("purpose") or "").strip(),
                "points": [str(p).strip() for p in (s.get("points") or []) if str(p).strip()],
                "content_type": str(s.get("content_type") or "").strip(),
                "estimated_length": str(s.get("estimated_length") or "").strip(),
                "product_link": str(s.get("product_link") or "").strip(),
            })
        objections = []
        for o in parsed.get("objections") or []:
            if not isinstance(o, dict):
                continue
            objections.append({
                "objection": str(o.get("objection") or "").strip(),
                "counter": str(o.get("counter") or "").strip(),
            })
        plan = {
            "title": str(parsed.get("title") or "").strip() or topic_title,
            "summary": str(parsed.get("summary") or "").strip(),
            "conversion_goal": str(parsed.get("conversion_goal") or "").strip(),
            "sections": sections,
            "objections": objections,
            "sources_to_use": [],
        }
        if platform_key not in ("tenchat", "telegram"):
            plan["sources_to_use"] = [
                u for u in (parsed.get("sources_to_use") or topic.get("sources") or [])
                if u
            ]
        logger.info("Сгенерирован детальный план: %d разделов, %d возражений",
                    len(plan["sections"]), len(plan["objections"]))
        return plan
    except Exception as exc:
        logger.warning("LLM вернул некорректный JSON для плана, fallback: %s", exc)
        return _fallback_plan(topic, platform_key)

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


async def generate_article(
    topic_id: int,
    plan: dict,
    platform: str = "",
    tone: str = "",
    max_product_mention: str = "",
    volume: str = "",
) -> str:
    """Генерирует статью по плану (формат sections + objections) с учётом
    площадки, стека и бизнес-целей QuickCom.

    Тема загружается из БД через get_topic(topic_id): используются все поля,
    включая platform, tone, max_product_mention, volume. Явно переданные
    аргументы platform/tone/max_product_mention/volume имеют приоритет над
    полями темы. План сериализуется в читаемый текст через _plan_to_text().
    Фактчекинг здесь НЕ вызывается — это отдельный шаг экспертизы."""
    if llm is None:
        logger.warning("LLM не настроен — статья не может быть сгенерирована")
        return ""
    topic = await get_topic(topic_id)
    if not topic:
        logger.error("Тема %s не найдена в БД — генерация статьи невозможна", topic_id)
        return ""
    topic_title = (topic.get("title") or topic.get("topic") or "").strip()
    topic_description = (topic.get("description") or "").strip()
    audience = (topic.get("audience") or "").strip()
    goal = (topic.get("goal") or "").strip()
    cta = (topic.get("cta") or "").strip()
    article_type = (topic.get("article_type") or "").strip()
    # Параметры площадки: приоритет у явно переданных аргументов,
    # иначе — из полей темы (get_topic возвращает все колонки topics)
    platform = (platform or topic.get("platform") or "blog").strip()
    tone = (tone or topic.get("tone") or "").strip()
    max_product_mention = (max_product_mention or topic.get("max_product_mention") or "").strip()
    volume = (volume or topic.get("volume") or "").strip()
    # Источники: приоритет у списка из плана (sources_to_use), иначе — из темы
    plan_sources = plan.get("sources_to_use") or []
    sources = [u for u in (plan_sources or topic.get("sources") or []) if u]
    sources_list = "\n".join(f"{i + 1}. {u}" for i, u in enumerate(sources))
    plan_json = _plan_to_text(plan, topic_title)
    prompt = f'''Ты — опытный технический писатель. Напиши статью на русском по плану.

ДАННЫЕ:
- Тема: {topic_title}
- Описание: {topic_description}
- Платформа: {platform}
- Аудитория: {audience}
- Цель: {goal}
- CTA: {cta}
- Тон: {tone}
- Лимит упоминания продукта: {max_product_mention}
- Объём: {volume}
- Тип статьи: {article_type}
- Источники: {sources_list}

ПЛАН:
{plan_json}

КОНТЕКСТ ПРОЕКТА:
- QuickCom — корпоративный мессенджер, build in public
- MVP сгенерирован DeepSeek, вручную доработан — признаётся открыто
- Фаундер — единственный разработчик, нет коммерческого опыта
- 4 хакатона в роли тимлида
- Стек: React, Storybook, Webpack, clsx, FastAPI, SQLAlchemy, PostgreSQL, Alembic, pytest, Celery, Redis, MinIO, Docker, Nginx, CI/CD

ПРАВИЛА ПО ПЛОЩАДКАМ:

habr:
- Технический фокус, код-фёрст
- Код в блоках (python, typescript, yaml, sql, diff для AI-кода)
- Продукт — только в выводах, 1 абзац, «я выбрал потому что»
- Честность про AI-генерацию — прямо в тексте
- Запрет: «уникальное решение», «инновационный подход», реклама
- 8000-12000 знаков
- В конце — ссылка на карьерную страницу в профиле (не в тексте)

vc:
- История фаундера, без пафоса, от первого лица
- БЕЗ выдуманных метрик. Формат: «что планирую измерять и почему»
- Продукт — в контексте, не более 10%
- Без «купить», «оформить», «приобрести»
- 5000-8000 знаков
- Структура: проблема → что делал → что получилось → чему научился → планы

tproger:
- Обучающий фокус, пошагово
- Код с комментариями на каждом шаге
- Продукт — через задачи
- Тон: «я тоже учусь — вот что сработало»
- 6000-10000 знаков
- В конце: «примените в своём проекте»

tenchat:
- Одна ключевая мысль, 3-5 абзацев
- Экспертный тон, без воды
- Продукт не упоминается (или одной фразой в конце)
- 500-1000 знаков
- Призыв: «подписаться», «обсудить», «пригласить на демо»

blog:
- Полная версия: техника + продукт + хроника + CTA
- Продукт естественно вплетён в сценарии
- CTA: «попробовать MVP», «обсудить архитектуру», «посмотреть код»
- SEO: ключевые слова в H2/H3
- 6000-12000 знаков

telegram:
- Краткий анонс или вакансия
- Для вакансии: стек (React/Node/Docker/FastAPI/CI-CD), формат build in public, ссылка на MVP
- Для анонса: 2-3 предложения + ссылка
- До 1000 знаков

ОБЩИЕ ТРЕБОВАНИЯ:
1. Статья на русском. Английскими — только технические термины и URL.
2. Следуй плану: каждый раздел — H2.
3. Раскрывай тезисы конкретными примерами, кодом, diff-описаниями, trade-off.
4. Если данных нет — реалистичные примеры с пометкой «пример» или «что планирую».
5. Честность про AI-генерацию — прямо в тексте, как сильная сторона.
6. Без выдуманных метрик. Формат: «что планирую измерять и почему».
7. Ссылки на источники [1], [2] — список в конце (кроме tenchat и telegram).
8. Запрет: «в эпоху цифровизации», «в современном мире», «ни для кого не секрет».
9. Формат: Markdown (# H1, ## H2, ### H3, списки, **жирный**, код).
10. Для habr/tproger/blog: в конце — «строю QuickCom в формате build in public, ищу соавторов».

Верни Markdown-текст статьи.'''
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
    """Парсит JSON-ответ экспертизы LLM; устойчив к markdown-обёрткам ```json …```.

    Закрывающий fence берётся ПОСЛЕДНИЙ, потому что внутри final_text могут
    быть вложенные код-блоки (```python …```). Дополнительно пробуется
    _extract_json(), если json.loads на извлечённом фрагменте не сработал.
    """
    text = (raw or "").strip()
    fence = re.search(r"```(?:json)?\s*(.*)```", text, re.DOTALL)
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
        try:
            data = json.loads(_extract_json(raw))
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


async def fact_check(
    article: str,
    sources: Optional[list[str]] = None,
    platform: str = "",
    audience: str = "",
    goal: str = "",
    cta: str = "",
    max_product_mention: str = "",
    tone: str = "",
) -> dict:
    """Финальная экспертиза статьи с учётом площадки и бизнес-целей.

    Принимает текст статьи и параметры темы (platform, audience, goal, cta,
    max_product_mention, tone, sources). Возвращает dict:
    {report, rating, recommendation, final_text}, где final_text — исправленная
    финальная Markdown-версия статьи, готовая к публикации.
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
    prompt = f"""Ты — старший редактор и технический эксперт. Проверь статью и верни ИСПРАВЛЕННУЮ финальную версию.

СТАТЬЯ:
{article}

ДАННЫЕ:
- Платформа: {platform}
- Аудитория: {audience}
- Цель: {goal}
- CTA: {cta}
- Лимит упоминания продукта: {max_product_mention}
- Тон: {tone}

ИСТОЧНИКИ (ссылки [N] — только из этого списка):
{sources_list}

КОНТЕКСТ ПРОЕКТА:
- QuickCom — корпоративный мессенджер, build in public
- MVP сгенерирован DeepSeek, вручную доработан — признаётся открыто
- Фаундер — единственный разработчик, нет коммерческого опыта
- Стек: React, FastAPI, PostgreSQL, Docker, pytest, Alembic, Celery, Redis, MinIO

ПРОВЕРЬ:

1. Соответствие площадке:
   - habr: нет рекламы, продукт только в выводах, код в блоках, честность про AI
   - vc: нет выдуманных метрик (только «что планирую измерять»), нет «купить», продукт в контексте
   - tproger: пошаговая структура, код с комментариями, тон «я тоже учусь»
   - tenchat: одна мысль, 3-5 абзацев, до 1000 знаков
   - blog: полная версия, CTA в конце, продукт вплетён в сценарии
   - telegram: краткий формат, до 1000 знаков

2. Лимит упоминания продукта:
   - QuickCom упоминается не чаще max_product_mention
   - Если превышен — сократи

3. Тон и стиль:
   - Соответствует ли тону tone
   - Нет ли машинного стиля: повторяющиеся конструкции, шаблонные фразы, одинаковое начало абзацев, канцелярит
   - Нет ли общих фраз без смысла

4. Честность про AI-генерацию:
   - Упомянуто ли, что код AI-сгенерирован и вручную доработан
   - Это подано как сильная сторона, а не как оправдание

5. Метрики:
   - Нет ли выдуманных цифр, ROI, конверсий
   - Если метрики есть — они помечены «пример» или «что планирую измерять»

6. Факты и логика:
   - Техническая корректность (FastAPI, SQLAlchemy, Docker, мультитенантность, Alembic)
   - Ссылки [N] ведут на реальные источники

7. CTA:
   - Соответствует CTA-матрице (habr — без прямого CTA в тексте; vc — без «купить»; tenchat — подписка/демо; blog — попробовать MVP/обсудить/код; telegram — фидбек/задача/митап)
   - Расположение: в конце (или в конце + промежуточный для blog)

8. Стилистика, орфография, пунктуация.

ПОСЛЕ ПРОВЕРКИ:
- Исправь все ошибки прямо в тексте
- Сократи лишние упоминания продукта
- Убери машинный стиль: разнообразь начала абзацев, убери шаблонные конструкции
- Убери выдуманные метрики, замени на «что планирую измерять»
- Добавь честность про AI-генерацию, если её нет
- В начало добавь отчёт (3-5 пунктов: что исправлено)

JSON:
{{
  "report": "отчёт (3-5 пунктов)",
  "rating": 8,
  "recommendation": "готово к публикации" | "требует доработки" | "нужно запросить данные",
  "final_text": "исправленный Markdown-текст"
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
    platform: str = "blog"
    tone: str = ""                    # приоритет над полем темы в generate_article()
    max_product_mention: str = ""     # приоритет над полем темы в generate_article()
    volume: str = ""                  # приоритет над полем темы в generate_article()
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
    выбрана); если блока нет — сохраняется как «Темы». Принимаются все поля
    формата extract_topics: topic, description, platform, audience, goal, cta,
    tone, max_product_mention, volume, reasons/relevance_theses, article_type,
    sources.
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
        reasons = [str(r).strip() for r in (t.get("reasons") or t.get("relevance_theses") or []) if str(r).strip()]
        theses = [str(r).strip() for r in (t.get("relevance_theses") or t.get("reasons") or []) if str(r).strip()]
        clean.append({
            "topic": topic,
            "description": (t.get("description") or "").strip(),
            "platform": (t.get("platform") or "blog").strip(),
            "audience": (t.get("audience") or "").strip(),
            "goal": (t.get("goal") or "").strip(),
            "cta": (t.get("cta") or "").strip(),
            "tone": (t.get("tone") or "").strip(),
            "max_product_mention": (t.get("max_product_mention") or "").strip(),
            "volume": (t.get("volume") or "").strip(),
            "reasons": reasons,
            "relevance_theses": theses,
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
    plan = await generate_plans(topic, payload.platform)
    plans_store[topic_id] = plan
    return {
        "topic": topic,
        "platform": payload.platform,
        "platform_name": pf["name"],
        "style": pf["style"],
        "tone": (topic.get("tone") or "").strip(),
        "max_product_mention": (topic.get("max_product_mention") or "").strip(),
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
    article = await generate_article(
        payload.topic_id,
        plan,
        platform=payload.platform,
        tone=(topic.get("tone") or ""),
        max_product_mention=(topic.get("max_product_mention") or ""),
        volume=(topic.get("volume") or ""),
    )
    if not article:
        raise HTTPException(502, "Не удалось сгенерировать статью. Проверь LLM-ключ и сеть.")
    expert = await fact_check(
        article,
        topic.get("sources") or [],
        platform=payload.platform,
        audience=topic.get("audience") or "",
        goal=topic.get("goal") or "",
        cta=topic.get("cta") or "",
        max_product_mention=topic.get("max_product_mention") or "",
        tone=topic.get("tone") or "",
    )
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
    Статья сохраняется со статусом 'draft'; фактчекинг здесь не запускается —
    это отдельный шаг экспертизы."""
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
    article = await generate_article(
        payload.topic_id,
        plan,
        platform=payload.platform,
        tone=(payload.tone or topic.get("tone") or ""),
        max_product_mention=(payload.max_product_mention or topic.get("max_product_mention") or ""),
        volume=(payload.volume or topic.get("volume") or ""),
    )
    if not article:
        raise HTTPException(502, "Не удалось сгенерировать статью. Проверь LLM-ключ и сеть.")
    article_id = await save_article(payload.topic_id, payload.platform, article)
    return {
        "id": article_id,
        "topic_title": topic["title"],
        "platform": payload.platform,
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

    Загружает статью и тему (platform, audience, goal, cta,
    max_product_mention, tone, sources), вызывает fact_check() со всеми
    полями темы, сохраняет final_text/report/rating/recommendation в БД
    и возвращает их клиенту.
    """
    art = await get_article(article_id)
    if not art:
        raise HTTPException(404, "Статья не найдена")
    topic = await get_topic(art["topic_id"]) if art.get("topic_id") else None
    sources = (topic or {}).get("sources") or []
    logger.info("Запуск экспертизы статьи %s (источников: %d)", article_id, len(sources))
    result = await fact_check(
        art["content"] or "",
        sources,
        platform=(art.get("platform") or (topic or {}).get("platform") or ""),
        audience=(topic or {}).get("audience") or "",
        goal=(topic or {}).get("goal") or "",
        cta=(topic or {}).get("cta") or "",
        max_product_mention=(topic or {}).get("max_product_mention") or "",
        tone=(topic or {}).get("tone") or "",
    )
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
