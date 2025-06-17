import gspread
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from datetime import datetime
import os

# Настройки
TOKEN = "8037089421:AAHlBf6OpTz3PWZ5LCdbOehDnvYIvp-8QZw"  # Замените на свой
GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/11Immztp4UdAeJnFjGqHk69UNAT-4I1UTaLhujsMsd74/edit?gid=0#gid=0"  # Вставьте свою
CREDS_FILE = "credentials.json"  # Файл с ключами Google

# Инициализация бота
bot = Bot(token=TOKEN)
dp = Dispatcher(bot)
dp.middleware.setup(LoggingMiddleware())

# Подключение к Google Таблице
def get_sheet():
    gc = gspread.service_account(filename=CREDS_FILE)
    return gc.open_by_url(GOOGLE_SHEET_URL).sheet1

# Команда /start
@dp.message_handler(commands=['start'])
async def start(message: types.Message):
    await message.answer("Салют, закидывай ссылки того, что считаешь годным. Только не бананы, иначе бан.")

# Сохранение сообщений
@dp.message_handler(content_types=types.ContentTypes.TEXT)
async def save_message(message: types.Message):
    sheet = get_sheet()
    user = message.from_user
    
    sheet.append_row([
        user.username or "Нет username",
        user.id,
        message.text,
        datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ])
    
    await message.answer("✅ Подумаем над твоим предложением")

if _name_ == '_main_':
    from aiogram import executor
    executor.start_polling(dp, skip_updates=True)