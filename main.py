python
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
                # Если это JSON-строка
                json.dump(json.loads(GOOGLE_CREDS_JSON), f)
            else:
                # Если это содержимое файла
                f.write(GOOGLE_CREDS_JSON)
    except Exception as e:
        print(f"Ошибка создания credentials.json: {e}")

# Подключение к Google Таблице с кешированием
_sheet_instance = None

def get_sheet():
    global _sheet_instance
    if _sheet_instance is not None:
        return _sheet_instance
        
    try:
        gc = gspread.service_account(filename="credentials.json")
        _sheet_instance = gc.open_by_url(GOOGLE_SHEET_URL).sheet1
        return _sheet_instance
    except Exception as e:
        print(f"Ошибка подключения к Google Sheets: {e}")
        return None

# Обработчики сообщений
@dp.message_handler(commands=['start'])
async def start(message: types.Message):
    await message.answer("Салют, закидывай ссылки того, что считаешь годным. Только не бананы, иначе бан.")

@dp.message_handler(content_types=types.ContentTypes.TEXT)
async def save_message(message: types.Message):
    try:
        sheet = get_sheet()
        if not sheet:
            await message.answer("❌ Ошибка подключения к таблице. Админ уже в курсе.")
            return

        user = message.from_user
        row_data = [
            user.username or "Нет username",
            str(user.id),
            message.text,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ]
        
        sheet.append_row(row_data)
        await message.answer("✅ Подумаем над твоим предложением")
        
    except gspread.exceptions.APIError as e:
        print(f"API Error: {e}")
        await message.answer("❌ Гугл таблицы временно недоступны. Попробуй позже.")
    except Exception as e:
        print(f"Неожиданная ошибка: {e}")
        await message.answer("❌ Что-то пошло не так. Мы уже работаем над этим.")

# Запуск бота
if __name__ == "__main__":
    print("Бот запускается...")  # Для логов Render
    try:
        executor.start_polling(
            dp,
            skip_updates=True,
            on_startup=lambda _: print("Бот успешно запущен"),
            on_shutdown=lambda _: print("Бот остановлен")
        )
    except Exception as e:
        print(f"Фатальная ошибка при запуске: {e}")
