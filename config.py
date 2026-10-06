import os

from dotenv import load_dotenv

# Загрузка .env из папки проекта (для локального запуска).
# В Docker переменные приходят из docker-compose и НЕ перезаписываются.
load_dotenv()

# === Веб-сервер ===
WEB_HOST = os.getenv("WEB_HOST", "0.0.0.0")
WEB_PORT = int(os.getenv("WEB_PORT", "8000"))

# === LLM (AITUNNEL — OpenAI-совместимый прокси DeepSeek и др.) ===
# Ключ строго из переменной окружения DEEPSEEK_API_KEY (формат sk-aitunnel-...),
# НЕ хардкодить. Base URL — AITUNNEL, НЕ api.deepseek.com.
LLM_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
LLM_BASE_URL = os.getenv("LLM_BASE_URL", "https://api.aitunnel.ru/v1")
LLM_MODEL = os.getenv("LLM_MODEL", "deepseek-chat")

# === Расписание ежедневного автосбора (МСК) ===
TRENDS_TIME = os.getenv("TRENDS_TIME", "09:00")

# === Прокси для внешних запросов (sing-box на хосте: 127.0.0.1:8080) ===
# При network_mode: "host" из контейнера виден 127.0.0.1 хоста,
# host.docker.internal НЕ резолвится — не использовать!
# Пустое значение (PROXY_URL=) или отсутствие переменной — прямое подключение.
PROXY_URL = os.getenv("PROXY_URL", "")

# === База данных ===
DB_PATH = os.getenv("DB_PATH", "/app/db/bot.db")

# === Платформы публикации (выбор в веб-панели) ===
PLATFORMS = [
    {"key": "habr", "name": "Habr", "style": "академический, технический разбор с кодом и схемами"},
    {"key": "vc", "name": "VC.ru", "style": "кейс с метриками до/после и ROI, бизнес-угол"},
    {"key": "tproger", "name": "Tproger", "style": "пошаговая инструкция с кодом и комментариями, типичные ошибки"},
    {"key": "tenchat", "name": "TenChat", "style": "короткий пост: одна мысль, 2-3 аргумента, вывод (500-1000 знаков)"},
    {"key": "blog", "name": "Блог", "style": "полная статья: техника + продукт + CTA, SEO-заголовки H2"},
    {"key": "telegram", "name": "Telegram-канал", "style": "краткий анонс (вакансия или ссылка на статью), до 1000 знаков"},
    {"key": "dzen", "name": "Дзен", "style": "лёгкий, доступный, с подзаголовками и жизненными примерами"},
    {"key": "pikabu", "name": "Pikabu", "style": "IT-лайфхак, дружелюбный, доступный"},
]
PLATFORM_MAP = {p["key"]: p for p in PLATFORMS}
