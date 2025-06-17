import os
import gspread
from aiogram import Bot, Dispatcher, executor, types
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from datetime import datetime
import json
from aiohttp import web
import asyncio
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Инициализация бота
TOKEN = os.getenv("TELEGRAM_TOKEN")
GOOGLE_SHEET_URL = os.getenv("GOOGLE_SHEET_URL")
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON")

if not all([TOKEN, GOOGLE_SHEET_URL, GOOGLE_CREDS_JSON]):
    logger.error("Не хватает обязательных переменных окружения!")
    raise ValueError("Требуются TELEGRAM_TOKEN, GOOGLE_SHEET_URL и GOOGLE_CREDS_JSON")

bot = Bot(token=TOKEN)
dp = Dispatcher(bot)
dp.middleware.setup(LoggingMiddleware())

# Создание credentials.json
try:
    with open("credentials.json", "w") as f:
        if GOOGLE_CREDS_JSON.startswith('{'):
            json.dump(json.loads(GOOGLE_CREDS_JSON), f)
        else:
            f.write(GOOGLE_CREDS_JSON)
    logger.info("Файл credentials.json создан")
except Exception as e:
    logger.error(f"Ошибка создания credentials.json: {e}")
    raise

# Подключение к Google Sheets
def get_sheet():
    try:
        gc = gspread.service_account(filename="credentials.json")
        return gc.open_by_url(GOOGLE_SHEET_URL).sheet1
    except Exception as e:
        logger.error(f"Ошибка Google Sheets: {e}")
        return None

# Обработчики Telegram
@dp.message_handler(commands=['start'])
async def start(message: types.Message):
    await message.answer("Салют, закидывай ссылки того, что считаешь годным. Только не бананы, иначе бан.")

@dp.message_handler(content_types=types.ContentTypes.TEXT)
async def save_message(message: types.Message):
    sheet = get_sheet()
    if not sheet:
        await message.answer("❌ Ошибка подключения к таблице")
        return

    user = message.from_user
    
    try:
        sheet.append_row([
            user.username or "Нет username",
            str(user.id),
            message.text,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ])
        await message.answer("✅ Подумаем над твоим предложением")
    except Exception as e:
        logger.error(f"Ошибка сохранения: {e}")
        await message.answer("❌ Ошибка при сохранении")

# Веб-сервер для Render
async def web_server():
    app = web.Application()
    app.router.add_get('/', lambda request: web.Response(text="Bot is running"))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', int(os.getenv('PORT', 10000)))
    await site.start()
    return runner

async def on_startup(dp):
    logger.info("Бот запущен")

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    
    # Запуск веб-сервера
    runner = loop.run_until_complete(web_server())
    
    # Запуск бота
    try:
        executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
    finally:
        loop.run_until_complete(runner.cleanup())
