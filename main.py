import os
import gspread
from aiogram import Bot, Dispatcher, executor, types
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from datetime import datetime
import json
from dotenv import load_dotenv
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Инициализация окружения
load_dotenv()

# Конфигурация
TOKEN = os.getenv("TELEGRAM_TOKEN")
GOOGLE_SHEET_URL = os.getenv("GOOGLE_SHEET_URL")
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON")

# Проверка обязательных переменных
if not all([TOKEN, GOOGLE_SHEET_URL, GOOGLE_CREDS_JSON]):
    logger.error("Не хватает обязательных переменных окружения!")
    raise ValueError("Требуются TELEGRAM_TOKEN, GOOGLE_SHEET_URL и GOOGLE_CREDS_JSON")

# Инициализация бота
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
    logger.info("Файл credentials.json успешно создан")
except Exception as e:
    logger.error(f"Ошибка создания credentials.json: {e}")
    raise

# Подключение к Google Таблице
def get_sheet():
    try:
        gc = gspread.service_account(filename="credentials.json")
        sheet = gc.open_by_url(GOOGLE_SHEET_URL).sheet1
        logger.info("Успешное подключение к Google Sheets")
        return sheet
    except Exception as e:
        logger.error(f"Ошибка подключения к Google Sheets: {e}")
        return None

# Обработчики сообщений
@dp.message_handler(commands=['start'])
async def start(message: types.Message):
    logger.info(f"Новый пользователь: {message.from_user.id}")
    await message.answer("Салют, закидывай ссылки того, что считаешь годным. Только не бананы, иначе бан.")

@dp.message_handler(content_types=types.ContentTypes.TEXT)
async def save_message(message: types.Message):
    user = message.from_user
    logger.info(f"Сообщение от {user.id}: {message.text[:50]}...")
    
    sheet = get_sheet()
    if not sheet:
        await message.answer("❌ Ошибка подключения к таблице. Админ уже в курсе.")
        return

    try:
        sheet.append_row([
            user.username or "Нет username",
            str(user.id),
            message.text,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ])
        logger.info("Данные успешно сохранены в таблицу")
        await message.answer("✅ Подумаем над твоим предложением")
    except Exception as e:
        logger.error(f"Ошибка при сохранении: {e}")
        await message.answer("❌ Произошла ошибка при сохранении")

if __name__ == "__main__":
    logger.info("Бот запускается...")
    try:
        executor.start_polling(dp, skip_updates=True)
    except Exception as e:
        logger.error(f"Фатальная ошибка: {e}")
        raise
