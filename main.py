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

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Инициализация FastAPI
app = FastAPI()

# Инициализация бота
bot = Bot(token=os.getenv('TELEGRAM_TOKEN'))
dp = Dispatcher(bot)
dp.middleware.setup(LoggingMiddleware())

# Фильтр YouTube ссылок
class YouTubeFilter(BoundFilter):
    async def check(self, message: types.Message) -> bool:
        if not message.text:
            return False
        return bool(re.search(
            r'(https?://)?(www\.)?(youtube|youtu|youtube-nocookie)\.(com|be)/(watch\?v=|embed/|v/|.+\?v=)?([^&=%\?]{11})',
            message.text.lower()
        ))

dp.filters_factory.bind(YouTubeFilter)

# Подключение к Google Sheets
def get_sheet():
    gc = gspread.service_account(filename="credentials.json")
    return gc.open_by_url(os.getenv('GOOGLE_SHEET_URL')).sheet1

def is_recent(date_str, days=30):
    date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
    return (datetime.now() - date) < timedelta(days=days)

def get_user_rank(user_id):
    sheet = get_sheet()
    data = sheet.get_all_records()
    counts = Counter(str(row['User ID']) for row in data)
    sorted_users = sorted(counts.items(), key=lambda x: x[1], reverse=True)
    return [i+1 for i, (uid, _) in enumerate(sorted_users) if uid == str(user_id)][0]

# Антиспам
@dp.throttled(rate=30)  # 30 секунд между сообщениями
async def anti_spam(message: types.Message, throttled: Throttled):
    if throttled.exceeded_count <= 2:
        await message.reply("⚠️ Ну не флуди, а.")

# Обработчики
@dp.message_handler(commands=['stats'])
async def send_stats(message: types.Message):
    stats = await generate_stats(message.from_user.id)
    await message.answer(stats, parse_mode='HTML')

@dp.message_handler(YouTubeFilter())
async def handle_youtube(message: types.Message):
    try:
        await anti_spam(message, None)
        
        sheet = get_sheet()
        user = message.from_user
        
        sheet.append_row([
            user.username or "Аноним",
            user.id,
            message.text,
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ])
        
        stats = await generate_stats(user.id)
        await message.answer(f"✅ Принято\n{stats}", parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"Error: {e}")
        await message.answer("❌ Не принято")

@dp.message_handler(content_types=types.ContentTypes.TEXT)
async def handle_other(message: types.Message):
    await message.answer("🚫 Принимаются только YouTube-ссылки!")
    await message.delete()

async def generate_stats(user_id):
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

# Запуск бота
async def start_bot():
    await dp.start_polling()

# FastAPI endpoint для проверки работоспособности
@app.get("/")
async def root():
    return {"status": "Bot is running"}

# Запуск приложения
async def main():
    # Создаем задачу для бота
    bot_task = asyncio.create_task(start_bot())
    # Запускаем FastAPI
    config = uvicorn.Config(app, host="0.0.0.0", port=10000)
    server = uvicorn.Server(config)
    await server.serve()

if __name__ == "__main__":
    asyncio.run(main())
