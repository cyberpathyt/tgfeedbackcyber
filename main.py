import os
import gspread
from aiogram import Bot, Dispatcher, executor, types
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from datetime import datetime
import json
from dotenv import load_dotenv

# Инициализация окружения
load_dotenv()  # Для локальной разработки

# Конфигурация
TOKEN = os.getenv("TELEGRAM_TOKEN")
GOOGLE_SHEET_URL = os.getenv("GOOGLE_SHEET_URL")
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON")

# Проверка обязательных переменных
if not TOKEN:
    raise ValueError("Не указан TELEGRAM_TOKEN в переменных окружения")
if not GOOGLE_SHEET_URL:
    raise ValueError("Не указан GOOGLE_SHEET_URL в переменных окружения")

# Инициализация бота
bot = Bot(token=TOKEN)
dp = Dispatcher(bot)
dp.middleware.setup(LoggingMiddleware())

# Создание credentials.json для Google Sheets
if GOOGLE_CREDS_JSON:
    try:
        with open("credentials.json", "w") as f:
            if GOOGLE_CREDS_JSON.startswith('{'):
                json.dump(json.loads(GOOGLE_CREDS_JSON), f)
            else:
                f.write(GOOGLE_CREDS_JSON)
    except Exception as e:
        print(f"Ошибка создания credentials.json: {e}")

# Подключение к Google Таблице
def get_sheet():
    try:
        gc = gspread.service_account(filename="credentials.json")
        return gc.open_by_url(GOOGLE_SHEET_URL).sheet1
    except Exception as e:
        print(f"Ошибка подключения к Google Sheets: {e}")
        return None

# Обработчики сообщений
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
        print(f"Ошибка при сохранении: {e}")
        await message.answer("❌ Произошла ошибка при сохранении")

if __name__ == "__main__":
    print("Бот запускается...")  # Для логов Render
    executor.start_polling(dp, skip_updates=True)
