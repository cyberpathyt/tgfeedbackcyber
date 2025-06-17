import os
import gspread
import re
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, executor, types
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from aiogram.dispatcher.filters import BoundFilter
from aiogram.utils.exceptions import Throttled
from collections import Counter
import logging
from fastapi import FastAPI
import uvicorn
import asyncio
from contextlib import asynccontextmanager
from fastapi.responses import JSONResponse

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Инициализация бота
bot = Bot(token=os.getenv('TELEGRAM_TOKEN'))
dp = Dispatcher(bot)
dp.middleware.setup(LoggingMiddleware())

# Улучшенный фильтр YouTube ссылок
class YouTubeFilter(BoundFilter):
    async def check(self, message: types.Message) -> bool:
        if not message.text:
            return False
            
        patterns = [
            r'(https?://)?(www\.)?youtube\.com/watch\?v=([^&%\s]+)',
            r'(https?://)?(www\.)?youtu\.be/([^&\s]+)',
            r'(https?://)?(www\.)?youtube\.com/shorts/([^&\s]+)',
            r'(https?://)?(www\.)?youtube\.com/embed/([^&\s]+)'
        ]
        
        for pattern in patterns:
            if re.search(pattern, message.text, re.IGNORECASE):
                return True
        return False

dp.filters_factory.bind(YouTubeFilter)

# Подключение к Google Sheets
def get_sheet():
    try:
        gc = gspread.service_account(filename="credentials.json")
        return gc.open_by_url(os.getenv('GOOGLE_SHEET_URL')).sheet1
    except Exception as e:
        logger.error(f"Google Sheets error: {e}")
        raise

def is_recent(date_str, days=30):
    try:
        date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
        return (datetime.now() - date) < timedelta(days=days)
    except ValueError:
        return False

def get_user_rank(user_id):
    try:
        sheet = get_sheet()
        data = sheet.get_all_records()
        counts = Counter(str(row['User ID']) for row in data)
        sorted_users = sorted(counts.items(), key=lambda x: x[1], reverse=True)
        return [i+1 for i, (uid, _) in enumerate(sorted_users) if uid == str(user_id)][0]
    except Exception as e:
        logger.error(f"Rank calculation error: {e}")
        return 0

# Антиспам
@dp.throttled(rate=30)
async def anti_spam(message: types.Message, throttled: Throttled):
    if throttled.exceeded_count <= 2:
        await message.reply("⚠️ Пожалуйста, не отправляйте сообщения слишком часто.")

# Обработчики
@dp.message_handler(commands=['stats'])
async def send_stats(message: types.Message):
    try:
        stats = await generate_stats(message.from_user.id)
        await message.answer(stats, parse_mode='HTML')
    except Exception as e:
        logger.error(f"Stats error: {e}")
        await message.answer("❌ Не удалось получить статистику")

@dp.message_handler(YouTubeFilter())
async def handle_youtube(message: types.Message):
    try:
        await anti_spam(message, None)
        
        sheet = get_sheet()
        user = message.from_user
        
        # Логируем полученную ссылку для отладки
        logger.info(f"Received YouTube link: {message.text} from {user.username or user.id}")
        
        sheet.append_row([
            user.username or "Аноним",
            user.id,
            message.text,
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ])
        
        stats = await generate_stats(user.id)
        await message.answer(f"✅ Ссылка принята!\n{stats}", parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"YouTube handler error: {e}")
        await message.answer("❌ Произошла ошибка при обработке ссылки")

@dp.message_handler(content_types=types.ContentTypes.TEXT)
async def handle_other(message: types.Message):
    logger.info(f"Received non-YouTube message: {message.text}")
    await message.answer("🚫 Я принимаю только YouTube-ссылки!")
    await message.delete()

async def generate_stats(user_id):
    try:
        sheet = get_sheet()
        data = sheet.get_all_records()
        user_data = [row for row in data if str(row['User ID']) == str(user_id)]
        monthly = len([d for d in user_data if is_recent(d['Date'])])
        
        return f"""
📊 <b>Ваша статистика</b>:
├ Всего предложений: {len(user_data)}
├ За месяц: {monthly}
└ Рейтинг: {get_user_rank(user_id)} место
"""
    except Exception as e:
        logger.error(f"Generate stats error: {e}")
        return "📊 Не удалось получить статистику"

# Запуск бота
async def run_bot():
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("Bot polling started successfully")
        await dp.start_polling()
    except Exception as e:
        logger.error(f"Bot polling failed: {e}")
        raise
    finally:
        await dp.storage.close()
        await dp.storage.wait_closed()
        await bot.session.close()

# Lifespan для FastAPI
@asynccontextmanager
async def lifespan(app: FastAPI):
    bot_task = asyncio.create_task(run_bot())
    logger.info("Application started")
    
    yield
    
    bot_task.cancel()
    try:
        await bot_task
    except asyncio.CancelledError:
        logger.info("Bot task cancelled")
    logger.info("Application stopped")

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def health_check():
    return JSONResponse(content={"status": "ok", "bot": "running"})

if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    uvicorn.run(app, host="0.0.0.0", port=port)
