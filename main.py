import os
import gspread
import re
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher.filters import BoundFilter
from aiogram.utils.exceptions import Throttled
from collections import Counter
import logging
from fastapi import FastAPI, Request
import uvicorn
import asyncio
from contextlib import asynccontextmanager
from fastapi.responses import JSONResponse

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('bot.log')
    ]
)
logger = logging.getLogger(__name__)

# Инициализация бота с явным указанием хранилища
storage = MemoryStorage()
bot = Bot(token=os.getenv('TELEGRAM_TOKEN'))
Bot.set_current(bot)
dp = Dispatcher(bot, storage=storage)

class Config:
    """Класс для проверки конфигурации"""
    def __init__(self):
        self.required_vars = [
            'TELEGRAM_TOKEN',
            'GOOGLE_SHEET_URL',
            'GS_TYPE',
            'GS_PROJECT_ID',
            'GS_PRIVATE_KEY_ID',
            'GS_PRIVATE_KEY',
            'GS_CLIENT_EMAIL',
            'GS_CLIENT_ID'
        ]
        self.check_config()
        
    def check_config(self):
        """Проверяет наличие всех необходимых переменных окружения"""
        missing_vars = [var for var in self.required_vars if not os.getenv(var)]
        if missing_vars:
            error_msg = f"Отсутствуют обязательные переменные окружения: {', '.join(missing_vars)}"
            logger.critical(error_msg)
            raise EnvironmentError(error_msg)
        logger.info("Все необходимые переменные окружения найдены")

config = Config()

class YouTubeFilter(BoundFilter):
    """Фильтр для YouTube ссылок"""
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

class GoogleSheetsManager:
    """Менеджер для работы с Google Sheets"""
    def __init__(self):
        self.credentials = self._get_credentials()
        self.sheet_url = os.getenv('GOOGLE_SHEET_URL')
        
    def _get_credentials(self) -> dict:
        """Создает credentials из переменных окружения"""
        return {
            "type": os.getenv('GS_TYPE'),
            "project_id": os.getenv('GS_PROJECT_ID'),
            "private_key_id": os.getenv('GS_PRIVATE_KEY_ID'),
            "private_key": os.getenv('GS_PRIVATE_KEY').replace('\\n', '\n'),
            "client_email": os.getenv('GS_CLIENT_EMAIL'),
            "client_id": os.getenv('GS_CLIENT_ID'),
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": os.getenv('GS_CLIENT_CERT_URL', '')
        }
        
    def get_sheet(self):
        """Возвращает рабочий лист таблицы"""
        try:
            gc = gspread.service_account_from_dict(self.credentials)
            sh = gc.open_by_url(self.sheet_url)
            return sh.sheet1
        except Exception as e:
            logger.error(f"Ошибка доступа к Google Sheets: {e}")
            raise

sheets_manager = GoogleSheetsManager()

def is_recent(date_str: str, days: int = 30) -> bool:
    """Проверяет, является ли дата не старше указанного количества дней"""
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
    """Возвращает рейтинг пользователя"""
    try:
        sheet = sheets_manager.get_sheet()
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

# Новая реализация антиспама без использования throttled
async def check_spam(message: types.Message):
    """Проверка на спам с простой задержкой"""
    user_id = message.from_user.id
    last_message = storage.data.get(f'spam_{user_id}')
    
    if last_message and (datetime.now() - last_message).seconds < 2:
        await message.reply("⚠️ Пожалуйста, не отправляйте сообщения слишком часто.")
        return True
    
    storage.data[f'spam_{user_id}'] = datetime.now()
    return False

@dp.message_handler(commands=['start', 'help'])
async def send_help(message: types.Message):
    """Обработчик команд start и help"""
    if await check_spam(message):
        return
        
    help_text = (
        "🤖 <b>YouTube Links Collector Bot</b>\n\n"
        "Просто отправьте мне ссылку на YouTube видео, и я сохраню её в базу.\n\n"
        "<b>Доступные команды:</b>\n"
        "/stats - Ваша статистика\n"
        "/test - Проверка работы бота"
    )
    await message.answer(help_text, parse_mode='HTML')

@dp.message_handler(commands=['stats'])
async def send_stats(message: types.Message):
    """Обработчик команды stats"""
    if await check_spam(message):
        return
        
    try:
        user = message.from_user
        logger.info(f"Обработка /stats для пользователя {user.id} ({user.username})")
        
        stats = await generate_stats(user.id)
        await message.answer(stats, parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"Ошибка в /stats: {e}")
        await message.answer("❌ Не удалось получить статистику. Попробуйте позже.")

async def generate_stats(user_id: int) -> str:
    """Генерирует статистику пользователя"""
    try:
        sheet = sheets_manager.get_sheet()
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

@dp.message_handler(YouTubeFilter())
async def handle_youtube(message: types.Message):
    """Обработчик YouTube ссылок"""
    if await check_spam(message):
        return
        
    try:
        user = message.from_user
        sheet = sheets_manager.get_sheet()
        url = message.text.split('?')[0].split('&')[0]
        
        sheet.append_row([
            user.username or "Аноним",
            user.id,
            url,
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ])
        
        stats = await generate_stats(user.id)
        await message.answer(f"✅ Ссылка сохранена!\n{stats}", parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"Ошибка обработки ссылки: {e}")
        await message.answer("❌ Ошибка при сохранении ссылки")

@dp.message_handler(commands=['test'])
async def test_command(message: types.Message):
    """Проверка работоспособности бота"""
    if await check_spam(message):
        return
        
    try:
        sheet = sheets_manager.get_sheet()
        records = sheet.get_all_records()
        user = message.from_user
        
        response = (
            f"🛠 <b>Тест системы</b>\n"
            f"• Бот: <code>{(await bot.get_me()).username}</code>\n"
            f"• Таблица: <code>{sheet.title}</code>\n"
            f"• Записей: <code>{len(records)}</code>\n"
            f"• Ваш ID: <code>{user.id}</code>\n"
            f"Статус: <b>работает</b> ✅"
        )
        await message.answer(response, parse_mode='HTML')
    except Exception as e:
        logger.error(f"Ошибка в /test: {e}")
        await message.answer("❌ Тест не пройден. Проверьте логи.")

@dp.message_handler(content_types=types.ContentTypes.TEXT)
async def handle_text(message: types.Message):
    """Обработчик текстовых сообщений"""
    if await check_spam(message):
        return
        
    if message.text.startswith('/'):
        await message.answer("❌ Неизвестная команда. Используйте /help")
    else:
        await message.answer("🚫 Я принимаю только YouTube-ссылки")
        await message.delete()

async def setup_webhook():
    """Настройка webhook"""
    try:
        webhook_url = f"{os.getenv('RENDER_EXTERNAL_URL')}/webhook"
        await bot.delete_webhook()
        await bot.set_webhook(webhook_url)
        logger.info(f"Webhook установлен: {webhook_url}")
    except Exception as e:
        logger.error(f"Ошибка настройки webhook: {e}")
        raise

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    await setup_webhook()
    logger.info("Сервис запущен")
    yield
    await bot.session.close()
    logger.info("Сервис остановлен")

app = FastAPI(lifespan=lifespan)

@app.post("/webhook")
async def handle_webhook(request: Request):
    """Обработчик webhook"""
    try:
        update = await request.json()
        telegram_update = types.Update(**update)
        await dp.process_update(telegram_update)
    except Exception as e:
        logger.error(f"Ошибка обработки webhook: {e}")
    return {"status": "ok"}

@app.get("/")
async def health_check():
    """Проверка работоспособности"""
    return JSONResponse(content={"status": "ok", "bot": "running"})

if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    uvicorn.run(app, host="0.0.0.0", port=port)
