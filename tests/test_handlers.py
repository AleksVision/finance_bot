import pytest
import asyncio
from decimal import Decimal
from aiogram import Dispatcher
from aiogram.fsm.storage.base import BaseStorage, StorageKey
from aiogram.fsm.context import FSMContext
from aiogram.types import Message, CallbackQuery, User
from unittest.mock import AsyncMock, MagicMock
from typing import Dict, Any

from bot.handlers import FinanceHandler, TransactionType, Categories, FinanceForm
from bot.database import FinanceDatabase

class MockStorage(BaseStorage):
    """Простая реализация MockStorage для тестирования"""
    def __init__(self):
        self.storage = {}

    async def set_state(self, key: StorageKey, state=None):
        self.storage.setdefault(key, {})['state'] = state

    async def get_state(self, key: StorageKey):
        return self.storage.get(key, {}).get('state')

    async def set_data(self, key: StorageKey, data: Dict[str, Any]):
        self.storage.setdefault(key, {})['data'] = data

    async def get_data(self, key: StorageKey):
        return self.storage.get(key, {}).get('data', {})

    async def update_data(self, key: StorageKey, data: Dict[str, Any]):
        current_data = await self.get_data(key)
        current_data.update(data)
        await self.set_data(key, current_data)
        return current_data

    async def close(self):
        self.storage.clear()

@pytest.fixture
async def finance_handler():
    """Фикстура для создания обработчика финансов"""
    handler = FinanceHandler()
    handler.db = AsyncMock(spec=FinanceDatabase)
    handler.keyboard_factory = MagicMock()  # Добавляем мок для keyboard_factory
    return handler

@pytest.fixture
async def message_mock():
    """Фикстура для создания мок-объекта сообщения"""
    message_mock = AsyncMock()
    message_mock.from_user = MagicMock()
    message_mock.from_user.id = 123456
    message_mock.answer = AsyncMock()
    return message_mock

@pytest.fixture
async def state_mock():
    """Фикстура для создания мок-объекта состояния"""
    state_mock = AsyncMock()
    state_mock.update_data = AsyncMock()
    state_mock.set_state = AsyncMock()
    state_mock.get_data = AsyncMock(return_value={})
    return state_mock

@pytest.mark.asyncio
async def test_start_transaction_income(finance_handler, message_mock, state_mock, mocker):
    """Тестирование начала транзакции дохода"""
    # Подготовка
    transaction_type = TransactionType.INCOME
    
    # Мокаем базу данных
    mocker.patch.object(finance_handler.db, 'create_user_if_not_exists', return_value=None)
    
    # Вызов метода
    await finance_handler.start_transaction(message_mock, state_mock, transaction_type)
    
    # Проверки
    # 1. Проверяем, что состояние обновлено
    state_mock.update_data.assert_called_once_with(transaction_type=transaction_type)
    state_mock.set_state.assert_called_once_with(FinanceForm.waiting_for_category)
    
    # 2. Проверяем отправку сообщения
    message_mock.answer.assert_called_once()
    
    # 3. Проверяем текст сообщения
    call_args = message_mock.answer.call_args
    assert "📥 Выберите статью дохода" in call_args[0][0]
    
    # 4. Проверяем клавиатуру
    keyboard = call_args[1]['reply_markup']
    assert keyboard is not None, "Клавиатура не должна быть пустой"

@pytest.mark.asyncio
async def test_start_transaction_expense(finance_handler, message_mock, state_mock, mocker):
    """Тестирование начала транзакции расхода"""
    # Подготовка
    transaction_type = TransactionType.EXPENSE
    
    # Мокаем базу данных
    mocker.patch.object(finance_handler.db, 'create_user_if_not_exists', return_value=None)
    
    # Вызов метода
    await finance_handler.start_transaction(message_mock, state_mock, transaction_type)
    
    # Проверки
    # 1. Проверяем, что состояние обновлено
    state_mock.update_data.assert_called_once_with(transaction_type=transaction_type)
    state_mock.set_state.assert_called_once_with(FinanceForm.waiting_for_category)
    
    # 2. Проверяем отправку сообщения
    message_mock.answer.assert_called_once()
    
    # 3. Проверяем текст сообщения
    call_args = message_mock.answer.call_args
    assert "📤 Выберите статью расхода" in call_args[0][0]
    
    # 4. Проверяем клавиатуру
    keyboard = call_args[1]['reply_markup']
    assert keyboard is not None, "Клавиатура не должна быть пустой"

@pytest.mark.asyncio
async def test_process_category_callback_income(finance_handler, state_mock):
    """Тест выбора категории дохода"""
    # Устанавливаем предварительные данные состояния
    await state_mock.update_data(transaction_type=TransactionType.INCOME)

    callback_mock = AsyncMock(spec=CallbackQuery)
    callback_mock.data = "category_salary"
    callback_mock.message = AsyncMock(spec=Message)
    callback_mock.message.answer = AsyncMock()
    callback_mock.from_user = User(id=456, first_name="Test", is_bot=False)
    callback_mock.answer = AsyncMock()

    # Мокаем get_data для возврата правильных данных
    state_mock.get_data.return_value = {'transaction_type': TransactionType.INCOME}

    await finance_handler.process_category_callback(callback_mock, state_mock)

    # Проверяем, что состояние обновлено
    state_mock.update_data.assert_called_with(category="salary")
    callback_mock.message.answer.assert_called_once_with("Выберите сумму транзакции:")
    callback_mock.answer.assert_called_once()

@pytest.mark.asyncio
async def test_process_amount_income(finance_handler, state_mock):
    """Тест добавления дохода"""
    # Устанавливаем предварительные данные состояния
    await state_mock.update_data({
        'transaction_type': TransactionType.INCOME,
        'category': 'salary'
    })

    message_mock = AsyncMock(spec=Message)
    message_mock.text = "1000.50"
    message_mock.from_user = User(id=456, first_name="Test", is_bot=False)
    message_mock.answer = AsyncMock()

    # Мокаем get_data для возврата правильных данных
    state_mock.get_data.return_value = {
        'transaction_type': TransactionType.INCOME,
        'category': 'salary'
    }

    # Мокаем метод добавления транзакции
    finance_handler.db.add_transaction = AsyncMock()

    await finance_handler.process_amount(message_mock, state_mock)

    # Проверяем, что транзакция добавлена
    finance_handler.db.add_transaction.assert_called_once_with(
        user_id=456,
        amount=Decimal('1000.50'),
        type_='income',
        category='salary'
    )

@pytest.mark.asyncio
async def test_invalid_amount_input(finance_handler, state_mock):
    """Тест обработки некорректного ввода суммы"""
    # Устанавливаем предварительные данные состояния
    await state_mock.update_data({
        'transaction_type': TransactionType.INCOME,
        'category': 'salary'
    })

    message_mock = AsyncMock(spec=Message)
    message_mock.text = "invalid_amount"
    message_mock.from_user = User(id=456, first_name="Test", is_bot=False)
    message_mock.answer = AsyncMock()

    await finance_handler.process_amount(message_mock, state_mock)

    # Проверяем, что отправлено сообщение об ошибке
    message_mock.answer.assert_called_once()
    assert "❌ Некорректная сумма" in message_mock.answer.call_args[0][0]

@pytest.mark.asyncio
async def test_show_statistics(finance_handler):
    """Тест получения статистики"""
    # Создаем мок-объект сообщения
    message_mock = AsyncMock(spec=Message)
    message_mock.from_user = User(id=456, first_name="Test", is_bot=False)
    message_mock.answer = AsyncMock()

    # Мокаем методы базы данных
    finance_handler.db.create_user_if_not_exists = AsyncMock()

    # Создаем мок-объект статистики
    class MockStats:
        total_income = 100000
        total_expense = 50000
        balance = 50000

    finance_handler.db.get_statistics = AsyncMock(return_value=MockStats())

    # Мокаем клавиатуру
    finance_handler.keyboard_factory.get_main_keyboard = MagicMock(return_value=None)

    await finance_handler.show_statistics(message_mock)

    # Проверяем вызовы методов
    finance_handler.db.create_user_if_not_exists.assert_called_once_with(456)
    finance_handler.db.get_statistics.assert_called_once()
    message_mock.answer.assert_called_once()
    assert "📊 Статистика за последние 30 дней" in message_mock.answer.call_args[0][0]
