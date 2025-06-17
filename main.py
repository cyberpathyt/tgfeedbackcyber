import os
import gspread
import re
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types
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
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Инициализация бота
bot = Bot(token=os.getenv('TELEGRAM_TOKEN'))
dp = Dispatcher(bot)
dp.middleware.setup(LoggingMiddleware())

# Улучшенный фильтр YouTube ссылок
class YouTubeFilter(BoundFilter):
    async def check(self, message: types.Message) -> bool:
        if not message.text or message.text.startswith('/'):
            return False
            
        patterns = [
            r'(?:https?://)?(?:www\.)?youtube\.com/watch\?v=([^&\s]+)',
            r'(?:https?://)?(?:www\.)?youtu\.be/([^?\s]+)',
            r'(?:https?://)?(?:www\.)?youtube\.com/shorts/([^?\s]+)',
            r'(?:https?://)?(?:www\.)?youtube\.com/embed/([^?\s]+)',
            r'(?:https?://)?(?:www\.)?youtube\.com/live/([^?\s]+)'
        ]
        
        return any(re.search(pattern, message.text, re.IGNORECASE) for pattern in patterns)

dp.filters_factory.bind(YouTubeFilter)

# Подключение к Google Sheets
def get_sheet():
    try:
        gc = gspread.service_account(filename="credentials.json")
        sheet_url = os.getenv('GOOGLE_SHEET_URL')
        if not sheet_url:
            raise ValueError("GOOGLE_SHEET_URL не установен")
            
        sh = gc.open_by_url(sheet_url)
        worksheet = sh.sheet1
        logger.info(f"Успешное подключение к таблице: {sh.title}")
        return worksheet
    except Exception as e:
        logger.error(f"Ошибка доступа к Google Sheets: {e}")
        raise

def is_recent(date_str: str, days: int = 30) -> bool:
    if not date_str:
        return False
        
    for fmt in ('%Y-%m-%d %H:%M:%S', '%d.%m.%Y %H:%M:%S', '%m/%d/%Y %H:%M:%S'):
        try:
            date = datetime.strptime(date_str, fmt)
            return (datetime.now() - date) < timedelta(days=days)
        except ValueError:
            continue
    return False

def get_user_rank(user_id: int) -> int:
    try:
        sheet = get_sheet()
        records = sheet.get_all_records()
        
        if not records or 'User ID' not in records[0]:
            logger.error("Неверная структура таблицы: отсутствует колонка 'User ID'")
            return 0
            
        counts = Counter(str(row['User ID']) for row in records)
        sorted_users = sorted(counts.items(), key=lambda x: (-x[1], x[0]))
        
        user_id_str = str(user_id)
        for rank, (uid, _) in enumerate(sorted_users, 1):
            if uid == user_id_str:
                return rank
        return 0
    except Exception as e:
        logger.error(f"Ошибка расчета рейтинга: {e}")
        return 0

# Антиспам
@dp.throttled(rate=30)
async def anti_spam(message: types.Message, throttled: Throttled):
    if throttled.exceeded_count <= 2:
        await message.reply("⚠️ Пожалуйста, не отправляйте сообщения слишком часто.")

# Обработчик команды /stats
@dp.message_handler(commands=['stats'])
async def send_stats(message: types.Message):
    try:
        user = message.from_user
        logger.info(f"Обработка /stats для пользователя {user.id} ({user.username})")
        
        stats = await generate_stats(user.id)
        await message.answer(stats, parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"Ошибка в /stats: {e}", exc_info=True)
        await message.answer(
            "❌ Не удалось получить статистику\n"
            "Попробуйте позже или проверьте логи",
            parse_mode='HTML'
        )

async def generate_stats(user_id: int) -> str:
    try:
        sheet = get_sheet()
        records = sheet.get_all_records()
        
        if not records:
            return "📊 В базе пока нет данных"
            
        user_data = [row for row in records if str(row.get('User ID', '')) == str(user_id)]
        monthly = sum(1 for d in user_data if is_recent(d.get('Date', '')))
        rank = get_user_rank(user_id)
        
        return (
            f"📊 <b>Ваша статистика</b>:\n"
            f"├ Всего предложений: <code>{len(user_data)}</code>\n"
            f"├ За последние 30 дней: <code>{monthly}</code>\n"
            f"└ Ваш рейтинг: <code>{rank}</code> место"
        )
    except Exception as e:
        logger.error(f"Ошибка генерации статистики: {e}")
        return "📊 Ошибка при получении статистики"

# Обработчик YouTube ссылок
@dp.message_handler(YouTubeFilter())
async def handle_youtube(message: types.Message):
    try:
        await anti_spam(message, None)
        
        user = message.from_user
        sheet = get_sheet()
        
        # Извлекаем чистую ссылку без параметров
        url = message.text.split('?')[0].split('&')[0]
        logger.info(f"Новая YouTube ссылка от {user.id}: {url}")
        
        # Добавляем запись в таблицу
        sheet.append_row([
            user.username or "Аноним",
            user.id,
            url,
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ])
        
        stats = await generate_stats(user.id)
        await message.answer(f"✅ Ссылка успешно сохранена!\n{stats}", parse_mode='HTML')
        
    except gspread.exceptions.APIError as e:
        logger.error(f"Ошибка Google Sheets: {e}")
        await message.answer("❌ Ошибка доступа к таблице. Попробуйте позже.")
    except Exception as e:
        logger.error(f"Ошибка обработки ссылки: {e}", exc_info=True)
        await message.answer(
            "❌ Произошла ошибка при сохранении ссылки\n"
            "Попробуйте другую ссылку или повторите позже"
        )

# Обработчик команды /test
@dp.message_handler(commands=['test'])
async def test_command(message: types.Message):
    try:
        # Проверка подключения к Google Sheets
        sheet = get_sheet()
        records = sheet.get_all_records()
        
        # Проверка текущего пользователя
        user = message.from_user
        user_data = [row for row in records if str(row.get('User ID', '')) == str(user.id)]
        
        await message.answer(
            f"🛠 <b>Тест системы</b>\n"
            f"User ID: <code>{user.id}</code>\n"
            f"Username: <code>{user.username or 'не указан'}</code>\n"
            f"Таблица: <code>{sheet.title}</code>\n"
            f"Всего записей: <code>{len(records)}</code>\n"
            f"Ваших записей: <code>{len(user_data)}</code>\n"
            f"Статус: <b>работает нормально</b>",
            parse_mode='HTML'
        )
    except Exception as e:
        logger.error(f"Ошибка в команде /test: {e}")
        await message.answer(
            f"❌ <b>Тест не пройден</b>\n"
            f"Ошибка: <code>{str(e)}</code>\n"
            f"Проверьте логи для подробностей",
            parse_mode='HTML'
        )

# Обработчик прочих текстовых сообщений
@dp.message_handler(content_types=types.ContentTypes.TEXT)
async def handle_text(message: types.Message):
    if message.text.startswith('/'):
        await message.answer("❌ Неизвестная команда")
    else:
        await message.answer("🚫 Я принимаю только YouTube-ссылки")
        await message.delete()

# Улучшенный запуск бота
async def run_bot_safely():
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("Бот запущен в режиме polling")
        await dp.start_polling()
    except Exception as e:
        logger.critical(f"Ошибка запуска бота: {e}")
        raise

# Lifespan для FastAPI
@asynccontextmanager
async def lifespan(app: FastAPI):
    bot_task = asyncio.create_task(run_bot_safely())
    logger.info("Сервис запущен")
    
    yield
    
    bot_task.cancel()
    try:
        await bot_task
    except asyncio.CancelledError:
        logger.info("Задача бота корректно остановлена")
    except Exception as e:
        logger.error(f"Ошибка при остановке бота: {e}")
    
    await dp.storage.close()
    await dp.storage.wait_closed()
    await bot.session.close()
    logger.info("Сервис остановлен")

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def health_check():
    return JSONResponse(content={"status": "ok", "bot": "running"})

if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    uvicorn.run(app, host="0.0.0.0", port=port)
