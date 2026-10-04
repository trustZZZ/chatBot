import os

# === Telegram ===
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHANNEL_ID = os.getenv("CHANNEL_ID", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))  # твой chat_id

# === LLM (DeepSeek через OpenAI-совместимый API) ===
LLM_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
LLM_BASE_URL = os.getenv("LLM_BASE_URL", "https://api.deepseek.com/v1")
LLM_MODEL = os.getenv("LLM_MODEL", "deepseek-chat")

# === Расписание ===
TRENDS_TIME = os.getenv("TRENDS_TIME", "09:00")  # время рассылки тем

# === Ключевые слова для фильтрации трендов ===
TARGET_KEYWORDS = [
    "messenger", "chat", "communication", "collaboration",
    "enterprise", "team", "real-time", "websocket", "E2EE",
    "encryption", "protocol", "security", "API", "open source",
    "messaging", "Slack", "Teams", "Discord", "Matrix",
    "self-hosted", "on-premise", "federation", "IT trends",
    "startup", "developer tools", "devops", "infrastructure",
]

# === Стиль по платформам (ротация по дням недели) ===
PLATFORM_STYLES = {
    0: {"platform": "habr", "style": "академический, технический разбор с кодом и схемами"},
    1: {"platform": "telegram", "style": "короткий аналитический пост с ссылкой на полную статью"},
    2: {"platform": "dzen", "style": "лёгкий, доступный, с подзаголовками и жизненными примерами"},
    3: {"platform": "vc", "style": "экспертное мнение, тренд-аналитика, бизнес-угол"},
    4: {"platform": "telegram", "style": "практический гайд, туториал"},
    5: {"platform": "pikabu", "style": "IT-лайфхак, дружелюбный, доступный"},
    6: {"platform": "rest", "style": "выходной"},
}

# === Призывы к действию (CTA) ===
CTA_TEMPLATES = {
    "developer": "Мы строим корпоративный мессенджер с открытым кодом. Ищем разработчиков. Напиши боту → @your_bot",
    "discussion": "Что думаете о подходе? Давайте обсудим в комментариях или в канале → @your_channel",
    "community": "Строим комьюнити вокруг IT-инструментов. Есть идея? Пиши → @your_bot",
}
