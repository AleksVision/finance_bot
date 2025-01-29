import logging
import asyncio
import signal
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import SimpleMemoryStorage 
from aiogram.filters import Command

from bot.config import BOT_TOKEN
from bot.database import FinanceDatabase, DatabaseError
from bot.handlers import router, register_handlers

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Инициализация компонентов
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
db = FinanceDatabase()

async def on_startup():
    """Действия при запуске бота"""
    try:
        # Инициализация базы данных
        await db.init_db()
        logger.info("Database initialized successfully")
        
        # Регистрация обработчиков
        register_handlers(router)
        dp.include_router(router)
        
        logger.info("Handlers registered successfully")
        
        logger.info("Bot started successfully!")
    except DatabaseError as e:
        logger.error(f"Failed to initialize database: {e}")
        raise
    except Exception as e:
        logger.error(f"Error during startup: {e}")
        raise

async def on_shutdown():
    """Действия при выключении бота"""
    try:
        # Закрываем сессию бота
        await bot.session.close()
        logger.info("Bot session closed")
        
        # Очищаем хранилище состояний
        await storage.close()
        logger.info("Storage closed")
        
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")
    finally:
        logger.info("Bot stopped!")

async def main():
    try:
        # Регистрация хендлеров для запуска и остановки
        dp.startup.register(on_startup)
        dp.shutdown.register(on_shutdown)
        
        # Настройка graceful shutdown
        loop = asyncio.get_event_loop()
        signals = (signal.SIGTERM, signal.SIGINT)
        for sig in signals:
            loop.add_signal_handler(
                sig,
                lambda s=sig: asyncio.create_task(shutdown(sig, loop))
            )
        
        # Запуск поллинга
        logger.info("Starting bot polling...")
        await dp.start_polling(bot)
        
    except Exception as e:
        logger.error(f"Critical error: {e}")
        raise
    finally:
        await on_shutdown()

async def shutdown(signal, loop):
    """Graceful shutdown"""
    logger.info(f'Received exit signal {signal.name}...')
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    
    [task.cancel() for task in tasks]
    logger.info(f'Cancelling {len(tasks)} outstanding tasks')
    
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped by user")
