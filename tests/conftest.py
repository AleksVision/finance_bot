import pytest
import pytest_asyncio
import logging
import os
from pathlib import Path
from typing import Generator, AsyncGenerator
from bot.database import FinanceDatabase

# Устанавливаем переменную окружения для тестов
os.environ['TESTING'] = 'true'

# Настройка pytest-asyncio
def pytest_configure(config):
    """Настройка pytest"""
    # Регистрируем кастомные маркеры
    config.addinivalue_line(
        "markers",
        "integration: mark test as integration test"
    )
    config.addinivalue_line(
        "markers",
        "slow: mark test as slow running"
    )

# Настройка логирования для тестов
@pytest.fixture(autouse=True)
def setup_logging():
    """Настраивает логирование для тестов"""
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

# Фикстура для тестовой директории
@pytest.fixture
def test_dir(tmp_path) -> Generator[Path, None, None]:
    """Создает временную директорию для тестов"""
    yield tmp_path

# Фикстура для очистки после тестов
@pytest_asyncio.fixture(autouse=True)
async def cleanup():
    """Очищает ресурсы после каждого теста"""
    yield

# Инициализация базы данных перед запуском тестов
@pytest_asyncio.fixture(scope="function")
async def init_test_database():
    """Инициализация базы данных перед запуском тестов"""
    db = FinanceDatabase('test_finance.db')
    await db.init_db()
    yield db
    await db.close()  # Добавляем закрытие соединения

# Фикстура для работы с базой данных в тестах
@pytest_asyncio.fixture
async def test_db(init_test_database):
    """Фикстура для работы с базой данных в тестах"""
    return init_test_database

# Создание тестового пользователя
@pytest_asyncio.fixture
async def test_user(test_db):
    """Создание тестового пользователя"""
    await test_db.create_user(1, "TestUser")
    return 1
