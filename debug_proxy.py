import asyncio
import aiohttp
import os
import logging

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s | %(levelname)-8s | %(message)s")
logger = logging.getLogger("debug")

async def test_polling():
    token = os.getenv("BOT_TOKEN")
    if not token:
        try:
            with open(".env") as f:
                for line in f:
                    if line.startswith("BOT_TOKEN="):
                        token = line.split("=", 1)[1].strip().strip('"').strip("'")
                        break
        except Exception:
            logger.error("Не удалось найти BOT_TOKEN")
            return

    url = f"https://api.telegram.org/bot{token}/getUpdates?timeout=5"
    logger.info(f"Отправляю запрос к Telegram API (через trust_env, timeout=5 сек)")

    try:
        async with aiohttp.ClientSession(trust_env=True) as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                logger.info(f"HTTP статус: {resp.status}")
                text = await resp.text()
                logger.info(f"Ответ: {text[:500]}")
    except asyncio.TimeoutError:
        logger.error("Запрос завис и был прерван по таймауту — прокси обрывает long-polling!")
    except Exception as e:
        logger.exception(f"Ошибка при запросе: {e}")

if __name__ == "__main__":
    asyncio.run(test_polling())
