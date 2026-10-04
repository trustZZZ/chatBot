"""
Диагностика прокси и доступа к Telegram API.

Запуск на сервере (внутри контейнера или на хосте):
    python debug_proxy.py
    docker exec trend-bot python debug_proxy.py

Проверяет getMe точно так же, как это делает бот при старте:
    - PROXY_URL задан  -> ProxyConnector(PROXY_URL) (aiohttp-socks)
    - PROXY_URL пустой -> прямое подключение
"""
import asyncio
import logging
import os

import aiohttp
from aiohttp_socks import ProxyConnector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("debug")


def _read_env(key: str) -> str:
    """Читаем переменную из окружения, при провале — из .env рядом."""
    value = os.getenv(key)
    if value:
        return value.strip().strip('"').strip("'")
    try:
        with open(".env", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.startswith(key + "="):
                    return line.split("=", 1)[1].strip().strip('"').strip("'")
    except Exception:
        pass
    return ""


async def main() -> None:
    token = _read_env("BOT_TOKEN")
    proxy = _read_env("PROXY_URL")

    if not token:
        logger.error("Не найден BOT_TOKEN (env или .env)")
        return

    url = f"https://api.telegram.org/bot{token}/getMe"
    connector = ProxyConnector.from_url(proxy) if proxy else None
    logger.info("Тест Telegram API: %s", f"через прокси {proxy}" if proxy else "напрямую (прокси не задан)")

    try:
        async with aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=15)) as session:
            async with session.get(url) as resp:
                logger.info("HTTP статус: %s", resp.status)
                text = await resp.text()
                logger.info("Ответ: %s", text[:500])
    except asyncio.TimeoutError:
        logger.error(
            "Запрос завис и прерван по таймауту. Прокси %s не отвечает или "
            "обрывает соединение (проверь sing-box и 127.0.0.1:8080 на хосте).",
            proxy or "<нет>",
        )
    except Exception:
        logger.exception("Ошибка при запросе к Telegram API")
    finally:
        if connector:
            await connector.close()


if __name__ == "__main__":
    asyncio.run(main())
