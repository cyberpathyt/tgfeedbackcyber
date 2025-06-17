import os
import gspread
import matplotlib.pyplot as plt
from io import BytesIO
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types, executor
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from aiogram.dispatcher.filters import BoundFilter
from aiogram.utils.exceptions import Throttled
from dotenv import load_dotenv
import logging

# Настройки
load_dotenv()
TOKEN = os.getenv('TELEGRAM_TOKEN')
ADMINS = os.getenv('ADMINS', '').split(',')
THROTTLE_RATE = 30  # секунд между сообщениями

# Инициализация
bot = Bot(token=TOKEN)
dp = Dispatcher(bot)
dp.middleware.setup(LoggingMiddleware())
logger = logging.getLogger(__name__)

# Фильтр для YouTube ссылок
class YouTubeFilter(BoundFilter):
    async def check(self, message: types.Message) -> bool:
        return 'youtube.com' in message.text.lower() or 'youtu.be' in message.text.lower()

dp.filters_factory.bind(YouTubeFilter)

# Антиспам
@dp.message_handler(commands=['stats'], throttle_rate=THROTTLE_RATE)
@dp.message_handler(YouTubeFilter(), throttle_rate=THROTTLE_RATE)
async def anti_spam(message: types.Message, throttled: Throttled):
    if throttled.exceeded_count <= 2:
        await message.reply("⚠️ Слишком часто! Подождите немного.")

# Подключение к Google Sheets
def get_sheet():
    gc = gspread.service_account(filename="credentials.json")
    return gc.open_by_url(os.getenv('GOOGLE_SHEET_URL')).sheet1

# Генерация графиков
async def generate_stats_image(user_id: int):
    sheet = get_sheet()
    all_data = sheet.get_all_records()
    
    # Фильтрация данных пользователя
    user_data = [row for row in all_data if str(row['User ID']) == str(user_id)]
    monthly_data = [row for row in user_data if datetime.now() - datetime.strptime(row['Date'], '%Y-%m-%d %H:%M:%S') < timedelta(days=30)]
    
    # Создание графика
    plt.figure(figsize=(10, 6))
    
    # График активности
    dates = [datetime.strptime(row['Date'], '%Y-%m-%d %H:%M:%S') for row in user_data]
    plt.hist(dates, bins=30, alpha=0.7, label='Ваша активность')
    
    plt.title('Ваша активность в боте')
    plt.xlabel('Дата')
    plt.ylabel('Количество предложений')
    plt.legend()
    
    # Сохранение в буфер
    buf = BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    plt.close()
    return buf

# Команда /stats
@dp.message_handler(commands=['stats'])
async def show_stats(message: types.Message):
    try:
        # Только для админов
        if str(message.from_user.id) not in ADMINS:
            await message.answer("⛔ Эта команда только для администраторов")
            return
            
        image = await generate_stats_image(message.from_user.id)
        await message.answer_photo(photo=image, caption="📊 Ваша статистика")
        
    except Exception as e:
        logger.error(f"Stats error: {e}")
        await message.answer("❌ Ошибка генерации статистики")

# Обработчик сообщений
@dp.message_handler(YouTubeFilter())
async def handle_youtube_link(message: types.Message):
    try:
        user = message.from_user
        sheet = get_sheet()
        
        # Запись в таблицу
        sheet.append_row([
            user.username or "Аноним",
            user.id,
            message.text,
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ])
        
        # Генерация персональной статистики
        all_data = sheet.get_all_records()
        user_entries = len([row for row in all_data if str(row['User ID']) == str(user.id)])
        monthly_entries = len([row for row in all_data if str(row['User ID']) == str(user.id) and 
                             datetime.now() - datetime.strptime(row['Date'], '%Y-%m-%d %H:%M:%S') < timedelta(days=30)])
        
        # Рейтинг пользователя
        all_users = {}
        for row in all_data:
            uid = str(row['User ID'])
            all_users[uid] = all_users.get(uid, 0) + 1
        
        sorted_users = sorted(all_users.items(), key=lambda x: x[1], reverse=True)
        user_rank = [i+1 for i, (uid, _) in enumerate(sorted_users) if uid == str(user.id)][0]
        
        # Отправка статистики
        stats_text = f"""
✅ Ссылка сохранена!

📊 <b>Ваша статистика</b>:
├ Всего предложений: {user_entries}
├ За месяц: {monthly_entries}
└ Рейтинг: {user_rank}-е место из {len(all_users)}

Спасибо за вашу активность! 🚀
        """
        await message.answer(stats_text, parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"Error saving link: {e}")
        await message.answer("❌ Ошибка сохранения. Попробуйте позже.")

# Обработчик не-YouTube ссылок
@dp.message_handler(content_types=types.ContentTypes.TEXT)
async def handle_non_youtube(message: types.Message):
    await message.answer("🚫 Это не YouTube-ссылка! Присылайте только ссылки на YouTube.")
    await message.delete()

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)
