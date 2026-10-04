import asyncio
import aiohttp
import os
import logging

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s | %(levelname)-8s | %(message)s")
logger = logging.getLogger("debug")

async def test_polling():
    token = os.getenv("BOT_TOKEN")
    if not token:
        # Пробуем вытащить из .env, если env не проброшен
        try:
            with open(".env") as f:
                for line in f:
                    if line.startswith("BOT_TOKEN="):
                        token = line.split("=", 1)[1].strip()
                        break
        except Exception:
            logger.error("Не удалось найти BOT_TOKEN ни в env, ни в .env")
            return

    url = f"https://api.telegram.org/bot{token}/getUpdates?timeout=5"
    logger.info(f"Отправляю запрос к {url} с timeout=5 сек (через trust_env)")

    try:
        async with aiohttp.ClientSession(trust_env=True) as session:
            async with session.get(url, timeout=10) as resp:
                logger.info(f"HTTP статус: {resp.status}")
                text = await resp.text()
                logger.info(f"Ответ: {text[:500]}")
    except asyncio.TimeoutError:
        logger.error("Запрос завис и был прерван по таймауту (соединение оборвано прокси или не доходит)")
    except Exception as e:
        logger.exception(f"Ошибка при запросе: {e}")

if __name__ == "__main__":
    asyncio.run(test_polling())
