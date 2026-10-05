import os

from dotenv import load_dotenv

# Загрузка .env из папки проекта (для локального запуска).
# В Docker переменные приходят из docker-compose и НЕ перезаписываются.
load_dotenv()

# === Веб-сервер ===
WEB_HOST = os.getenv("WEB_HOST", "0.0.0.0")
WEB_PORT = int(os.getenv("WEB_PORT", "8000"))

# === LLM (DeepSeek через OpenAI-совместимый API) ===
LLM_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
LLM_BASE_URL = os.getenv("LLM_BASE_URL", "https://api.deepseek.com/v1")
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
    {"key": "telegram", "name": "Telegram-канал", "style": "короткий аналитический пост со ссылкой на полную статью"},
    {"key": "dzen", "name": "Дзен", "style": "лёгкий, доступный, с подзаголовками и жизненными примерами"},
    {"key": "vc", "name": "VC.ru", "style": "экспертное мнение, тренд-аналитика, бизнес-угол"},
    {"key": "pikabu", "name": "Pikabu", "style": "IT-лайфхак, дружелюбный, доступный"},
]
PLATFORM_MAP = {p["key"]: p for p in PLATFORMS}
