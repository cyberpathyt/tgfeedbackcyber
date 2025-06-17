import os
import gspread
from aiogram import Bot, Dispatcher, executor, types
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from datetime import datetime
import json
from dotenv import load_dotenv

# Загрузка .env файла (только для локальной разработки)
load_dotenv()

# Получение переменных окружения
TOKEN = os.getenv("TELEGRAM_TOKEN")
GOOGLE_SHEET_URL = os.getenv("GOOGLE_SHEET_URL")

# Инициализация бота
bot = Bot(token=TOKEN)
dp = Dispatcher(bot)
dp.middleware.setup(LoggingMiddleware())

# Создание credentials.json из переменной окружения (если нужно)
if os.getenv("GOOGLE_CREDS_JSON"):
    with open("credentials.json", "w") as f:
        f.write(os.getenv("GOOGLE_CREDS_JSON"))

# Подключение к Google Таблице
def get_sheet():
    try:
        gc = gspread.service_account(filename="credentials.json")
        return gc.open_by_url(GOOGLE_SHEET_URL).sheet1
    except Exception as e:
        print(f"Ошибка подключения к Google Sheets: {e}")
        return None

# Команда /start
@dp.message_handler(commands=['start'])
async def start(message: types.Message):
    await message.answer("Салют, закидывай ссылки того, что считаешь годным. Только не бананы, иначе бан.")

# Сохранение сообщений
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
            user.id,
            message.text,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ])
        await message.answer("✅ Подумаем над твоим предложением")
    except Exception as e:
        print(f"Ошибка при сохранении: {e}")
        await message.answer("❌ Произошла ошибка при сохранении")

if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
