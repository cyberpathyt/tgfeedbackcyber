import os
import gspread
import re
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types, exceptions
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from aiogram.dispatcher.filters import BoundFilter
from aiogram.utils.exceptions import Throttled
from collections import Counter
import logging
from fastapi import FastAPI, HTTPException
import uvicorn
import asyncio
from contextlib import asynccontextmanager
from fastapi.responses import JSONResponse
from typing import Dict, Any

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

class Config:
    """Класс для проверки и хранения конфигурации"""
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

# Инициализация бота
bot = Bot(token=os.getenv('TELEGRAM_TOKEN'))
dp = Dispatcher(bot)
dp.middleware.setup(LoggingMiddleware())

class YouTubeFilter(BoundFilter):
    """Фильтр для YouTube ссылок с улучшенным распознаванием"""
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
        
    def _get_credentials(self) -> Dict[str, Any]:
        """Получает учетные данные из переменных окружения"""
        return {
            "type": os.getenv('GS_TYPE'),
            "project_id": os.getenv('GS_PROJECT_ID'),
            "private_key_id": os.getenv('GS_PRIVATE_KEY_ID'),
            "private_key": os.getenv('GS_PRIVATE_KEY').replace('\\n', '\n'),
            "client_email": os.getenv('GS_CLIENT_EMAIL'),
            "client_id": os.getenv('GS_CLIENT_ID'),
            "auth_uri": os.getenv('GS_AUTH_URI', 'https://accounts.google.com/o/oauth2/auth'),
            "token_uri": os.getenv('GS_TOKEN_URI', 'https://oauth2.googleapis.com/token'),
            "auth_provider_x509_cert_url": os.getenv('GS_AUTH_PROVIDER_CERT_URL', 
                                                   'https://www.googleapis.com/oauth2/v1/certs'),
            "client_x509_cert_url": os.getenv('GS_CLIENT_CERT_URL')
        }
        
    def get_sheet(self):
        """Возвращает рабочий лист таблицы"""
        try:
            logger.info(f"Подключение к Google Sheets: {self.sheet_url}")
            gc = gspread.service_account_from_dict(self.credentials)
            sh = gc.open_by_url(self.sheet_url)
            worksheet = sh.sheet1
            logger.info(f"Успешное подключение к таблице: {sh.title}")
            return worksheet
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

@dp.throttled(rate=30)
async def anti_spam(message: types.Message, throttled: Throttled):
    """Антиспам защита"""
    if throttled.exceeded_count <= 2:
        await message.reply("⚠️ Пожалуйста, не отправляйте сообщения слишком часто.")

@dp.message_handler(commands=['start', 'help'])
async def send_help(message: types.Message):
    """Обработчик команд start и help"""
    help_text = (
        "🤖 <b>YouTube Links Collector Bot</b>\n\n"
        "Просто отправьте мне ссылку на YouTube видео, и я сохраню её в базу.\n\n"
        "<b>Доступные команды:</b>\n"
        "/stats - Ваша статистика\n"
        "/test - Проверка работы бота\n\n"
        "Бот автоматически удаляет все сообщения, кроме YouTube-ссылок."
    )
    await message.answer(help_text, parse_mode='HTML')

@dp.message_handler(commands=['stats'])
async def send_stats(message: types.Message):
    """Обработчик команды stats"""
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
    try:
        await anti_spam(message, None)
        
        user = message.from_user
        sheet = sheets_manager.get_sheet()
        
        url = message.text.split('?')[0].split('&')[0]
        logger.info(f"Новая YouTube ссылка от {user.id}: {url}")
        
        sheet.append_row([
            user.username or "Аноним",
            user.id,
            url,
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ])
        
        stats = await generate_stats(user.id)
        await message.answer(f"✅ Ссылка успешно сохранена!\n{stats}", parse_mode='HTML')
        
    except gspread.exceptions.APIError as e:
        logger.error(f"Ошибка Google Sheets API: {e}")
        await message.answer("❌ Ошибка доступа к таблице. Попробуйте позже.")
    except Exception as e:
        logger.error(f"Ошибка обработки ссылки: {e}", exc_info=True)
        await message.answer(
            "❌ Произошла ошибка при сохранении ссылки\n"
            "Попробуйте другую ссылку или повторите позже"
        )

@dp.message_handler(commands=['test'])
async def test_command(message: types.Message):
    """Обработчик команды test"""
    try:
        sheet = sheets_manager.get_sheet()
        records = sheet.get_all_records()
        user = message.from_user
        user_data = [row for row in records if str(row.get('User ID', '')) == str(user.id)]
        
        await message.answer(
            f"🛠 <b>Тест системы</b>\n"
            f"• Подключение к Google Sheets: <b>успешно</b>\n"
            f"• Таблица: <code>{sheet.title}</code>\n"
            f"• Всего записей: <code>{len(records)}</code>\n"
            f"• Ваших записей: <code>{len(user_data)}</code>\n"
            f"• User ID: <code>{user.id}</code>\n"
            f"• Username: <code>{user.username or 'не указан'}</code>\n\n"
            f"Статус: <b>работает нормально</b> ✅",
            parse_mode='HTML'
        )
    except Exception as e:
        logger.error(f"Ошибка в команде /test: {e}")
        await message.answer(
            f"❌ <b>Тест не пройден</b>\n"
            f"Ошибка: <code>{str(e)}</code>\n\n"
            f"Проверьте:\n"
            f"1. Настройки переменных окружения\n"
            f"2. Доступ к Google Sheets\n"
            f"3. Права доступа сервисного аккаунта",
            parse_mode='HTML'
        )

@dp.message_handler(content_types=types.ContentTypes.TEXT)
async def handle_text(message: types.Message):
    """Обработчик текстовых сообщений"""
    if message.text.startswith('/'):
        await message.answer("❌ Неизвестная команда. Используйте /help для списка команд")
    else:
        await message.answer("🚫 Я принимаю только YouTube-ссылки")
        await message.delete()

async def run_bot_safely():
    """Безопасный запуск бота с обработкой ошибок"""
    retry_count = 0
    max_retries = 3
    retry_delay = 5
    
    while retry_count < max_retries:
        try:
            await bot.delete_webhook(drop_pending_updates=True)
            await asyncio.sleep(1)
            
            logger.info(f"Запуск бота (попытка {retry_count + 1}/{max_retries})")
            await dp.start_polling()
            return
            
        except exceptions.TerminatedByOtherGetUpdates:
            retry_count += 1
            logger.warning(f"Конфликт getUpdates. Попытка {retry_count}/{max_retries}")
            if retry_count < max_retries:
                await asyncio.sleep(retry_delay)
                
        except Exception as e:
            logger.critical(f"Критическая ошибка: {e}")
            raise
            
    logger.error("Не удалось запустить бота из-за конфликта getUpdates")
    raise RuntimeError("Не удалось запустить бота после нескольких попыток")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
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

app = FastAPI(lifespan=lifespan, title="YouTube Links Collector Bot")

@app.get("/")
async def health_check():
    """Проверка работоспособности сервиса"""
    return JSONResponse(content={
        "status": "ok",
        "bot": "running",
        "timestamp": datetime.now().isoformat()
    })

@app.get("/config")
async def show_config():
    """Показывает текущую конфигурацию (без секретных данных)"""
    return JSONResponse(content={
        "google_sheet_url": os.getenv('GOOGLE_SHEET_URL'),
        "bot_username": (await bot.me).username,
        "environment": {k: v for k, v in os.environ.items() if not k.startswith('GS_')}
    })

@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Глобальный обработчик исключений"""
    logger.error(f"Необработанное исключение: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal Server Error"}
    )

if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    uvicorn.run(app, host="0.0.0.0", port=port, log_config=None)
