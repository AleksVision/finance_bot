import pytest
import pytest_asyncio
import os
import asyncio
import aiosqlite
from decimal import Decimal
from datetime import datetime, timedelta
from bot.database import (
    FinanceDatabase, DatabaseError, Transaction,
    Statistics, CategoryStatistics
)

# Фикстура для тестовой базы данных
@pytest_asyncio.fixture(scope="function")
async def test_db():
    """Создает временную тестовую базу данных"""
    test_db_name = "test_finance.db"
    
    # Удаляем тестовую БД если она существует
    if os.path.exists(test_db_name):
        os.remove(test_db_name)
    
    db = FinanceDatabase(test_db_name)
    await db.init_db()
    
    yield db
    
    # Очистка после тестов
    if os.path.exists(test_db_name):
        os.remove(test_db_name)

# Фикстура для тестового пользователя
@pytest_asyncio.fixture(scope="function")
async def test_user(test_db):
    """Создает тестового пользователя"""
    async with aiosqlite.connect(test_db.database_name) as db:
        cursor = await db.execute(
            "INSERT INTO users (telegram_id, username) VALUES (?, ?)",
            (1, "test_user")
        )
        user_id = cursor.lastrowid
        await db.commit()
        return 1  # Возвращаем telegram_id

# Остальные тесты остаются прежними, но добавим исправления

@pytest.mark.asyncio
async def test_add_transaction(test_db, test_user):
    """Тест добавления транзакции"""
    # Добавляем транзакцию
    amount = Decimal("100.50")
    transaction_type = "income"
    category = "Зарплата"
    description = "Тестовая транзакция"

    transaction_id = await test_db.add_transaction(
        test_user,
        amount,
        transaction_type,
        category,
        description
    )

    assert transaction_id is not None

    # Проверяем, что транзакция добавлена
    async with aiosqlite.connect(test_db.database_name) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """
            SELECT t.*, c.name as category_name
            FROM transactions t
            JOIN categories c ON t.category_id = c.id
            WHERE t.id = ?
            """,
            (transaction_id,)
        )
        transaction = await cursor.fetchone()

        assert transaction is not None
        assert Decimal(str(transaction['amount'])) == amount
        assert transaction['type'] == transaction_type
        assert transaction['category_name'] == category
        assert transaction['description'] == description

@pytest.mark.asyncio
async def test_transaction_date_filtering(test_db, test_user):
    """Тест фильтрации транзакций по дате"""
    # Фиксируем дату для теста
    test_date = datetime(2024, 1, 15)
    past_date = test_date - timedelta(days=30)
    future_date = test_date + timedelta(days=45)  # Увеличим диапазон будущей транзакции

    transactions = [
        (past_date, Decimal("50.00"), "expense", "Старая"),
        (test_date, Decimal("100.00"), "income", "Текущая"),
        (future_date, Decimal("75.00"), "income", "Будущая")
    ]

    for date, amount, type_, description in transactions:
        await test_db.add_transaction(
            test_user,
            amount,
            type_,
            "Тестовая",
            description,
            date
        )

    # Фильтрация транзакций за последние 15 дней и 45 дней вперед
    start_date = test_date - timedelta(days=15)
    end_date = test_date + timedelta(days=45)

    filtered_transactions = await test_db.get_transactions(
        test_user,
        start_date,
        end_date
    )

    # Проверяем количество и содержание отфильтрованных транзакций
    assert len(filtered_transactions) == 2
    assert {t.description for t in filtered_transactions} == {"Текущая", "Будущая"}
    
    # Проверяем, что транзакции попадают в нужный диапазон
    for t in filtered_transactions:
        if t.description == "Текущая":
            assert start_date.date() <= t.date.date() <= end_date.date(), \
                f"Текущая транзакция {t.description} не попадает в диапазон {start_date.date()} - {end_date.date()}"
        elif t.description == "Будущая":
            assert t.date.date() > test_date.date() and t.date.date() <= end_date.date(), \
                f"Будущая транзакция {t.description} не попадает в диапазон {start_date.date()} - {end_date.date()}"

@pytest.mark.asyncio
async def test_concurrent_transactions(test_db, test_user):
    """Тест обработки конкурентных транзакций"""
    # Симуляция одновременного добавления транзакций
    transactions = [
        (Decimal("50.00"), "income", "Подработка"),
        (Decimal("75.00"), "expense", "Развлечения"),
        (Decimal("100.00"), "income", "Бонус")
    ]

    async def add_transaction(amount, type_, category):
        return await test_db.add_transaction(
            test_user,
            amount,
            type_,
            category,
            f"Конкурентная транзакция {category}"
        )

    # Выполняем транзакции параллельно
    transaction_ids = await asyncio.gather(
        *[add_transaction(*t) for t in transactions]
    )

    # Проверяем, что все транзакции добавлены
    assert len(transaction_ids) == len(transactions)
    assert len(set(transaction_ids)) == len(transactions)

@pytest.mark.asyncio
async def test_add_transaction_with_optional_date(test_db):
    """Тест добавления транзакции с опциональной датой"""
    user_id = 1
    await test_db.create_user(user_id, "Test User")
    
    # Добавляем транзакцию без указания даты
    transaction_id1 = await test_db.add_transaction(
        user_id=user_id,
        amount=Decimal("100.50"),
        type_="income",
        category="salary",
        description="Monthly salary"
    )
    
    # Добавляем транзакцию с конкретной датой
    specific_date = datetime(2023, 6, 15, 12, 0, 0)
    transaction_id2 = await test_db.add_transaction(
        user_id=user_id,
        amount=Decimal("50.25"),
        type_="expense",
        category="food",
        description="Lunch",
        date=specific_date
    )
    
    # Проверяем, что транзакции добавлены
    async with aiosqlite.connect(test_db.database_name) as db:
        async with db.execute("SELECT date FROM transactions WHERE id = ?", (transaction_id1,)) as cursor:
            transaction1_date = await cursor.fetchone()
        
        async with db.execute("SELECT date FROM transactions WHERE id = ?", (transaction_id2,)) as cursor:
            transaction2_date = await cursor.fetchone()
    
    # Проверяем, что первая транзакция имеет текущую дату
    assert transaction1_date[0] is not None
    
    # Проверяем, что вторая транзакция имеет указанную дату
    assert transaction2_date[0] == specific_date.strftime("%Y-%m-%d %H:%M:%S")

# Добавляем дополнительные параметры в сигнатуру метода add_transaction в database.py
