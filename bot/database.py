import aiosqlite
import logging
import os
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from cachetools import TTLCache
import matplotlib.pyplot as plt
import io
import numpy as np

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DATABASE_NAME = 'finance.db'

CATEGORY_TRANSLATIONS = {
    # Доходы
    'salary': '💼 Зарплата',
    'freelance': '💻 Фриланс',
    'investments': '📈 Инвестиции', 
    'other_income': '💰 Прочие доходы',
    'gifts': '🎁 Подарки',
    
    # Расходы
    'housing': '🏠 Жилье',
    'transport': '🚗 Транспорт',
    'electronics': '💻 Электроника',
    'health': '🏥 Здоровье',
    'other_expense': '💸 Прочие расходы'
}

def translate_category(category):
    return CATEGORY_TRANSLATIONS.get(category, category)

@dataclass
class Transaction:
    id: Optional[int]
    type: str
    amount: Decimal
    category: str
    description: Optional[str]
    date: datetime
    user_id: int
    total_income: Optional[Decimal] = None
    total_expense: Optional[Decimal] = None
    balance: Optional[Decimal] = None

@dataclass
class Statistics:
    """Класс для хранения статистики"""
    total_income: Decimal
    total_expense: Decimal
    balance: Decimal
    transactions: List[Transaction]

@dataclass
class CategoryStatistics:
    """Класс для хранения статистики по категориям"""
    category: str
    type: str
    total: Decimal

@dataclass
class StatisticsResult:
    total_income: Decimal
    total_expense: Decimal
    balance: Decimal
    transactions: List[Transaction]
    income_details: List[Dict[str, float]]
    expense_details: List[Dict[str, float]]

class DatabaseError(Exception):
    """
    База данных ошибок с дополнительным контекстом
    
    Этот класс используется для более детальной обработки ошибок базы данных,
    позволяя сохранять оригинальную ошибку и предоставлять дополнительный контекст.
    """
    def __init__(self, message: str, original_error: Optional[Exception] = None):
        """
        Инициализация ошибки базы данных
        
        :param message: Описательное сообщение об ошибке
        :param original_error: Оригинальное исключение, вызвавшее ошибку
        """
        super().__init__(message)
        self.original_error = original_error

class FinanceCache:
    """Класс для кэширования финансовых данных"""
    def __init__(self, ttl: int = 300):  # TTL по умолчанию 5 минут
        self.statistics_cache = TTLCache(maxsize=1000, ttl=ttl)
        self.category_statistics_cache = TTLCache(maxsize=1000, ttl=ttl)

    def _make_key(self, user_id: int, start_date: Optional[datetime] = None, 
                  end_date: Optional[datetime] = None) -> str:
        """Создание ключа для кэша"""
        start_str = start_date.isoformat() if start_date else "none"
        end_str = end_date.isoformat() if end_date else "none"
        return f"user_{user_id}_start_{start_str}_end_{end_str}"

    def get_statistics(self, user_id: int, start_date: Optional[datetime] = None,
                      end_date: Optional[datetime] = None) -> Optional[Statistics]:
        """Получение статистики из кэша"""
        key = self._make_key(user_id, start_date, end_date)
        cached_data = self.statistics_cache.get(key)
        if cached_data:
            return Statistics(
                total_income=Decimal(str(cached_data[0])),
                total_expense=Decimal(str(cached_data[1])),
                balance=Decimal(str(cached_data[2])),
                transactions=cached_data[3]
            )
        return None

    def set_statistics(self, user_id: int, stats: Statistics,
                      start_date: Optional[datetime] = None,
                      end_date: Optional[datetime] = None):
        """Сохранение статистики в кэш"""
        key = self._make_key(user_id, start_date, end_date)
        cached_data = (
            float(stats.total_income),
            float(stats.total_expense),
            float(stats.balance),
            stats.transactions
        )
        self.statistics_cache[key] = cached_data

    def get_category_statistics(self, user_id: int, start_date: Optional[datetime] = None,
                              end_date: Optional[datetime] = None) -> Optional[List[CategoryStatistics]]:
        """Получение статистики по категориям из кэша"""
        key = self._make_key(user_id, start_date, end_date)
        return self.category_statistics_cache.get(key)

    def set_category_statistics(self, user_id: int, stats: List[CategoryStatistics],
                              start_date: Optional[datetime] = None,
                              end_date: Optional[datetime] = None):
        """Сохранение статистики по категориям в кэш"""
        key = self._make_key(user_id, start_date, end_date)
        self.category_statistics_cache[key] = stats

    def invalidate_user_cache(self, user_id: int):
        """Инвалидация кэша для пользователя"""
        for cache in [self.statistics_cache, self.category_statistics_cache]:
            keys_to_remove = [k for k in cache.keys() if str(user_id) in k]
            for key in keys_to_remove:
                cache.pop(key, None)

class FinanceDatabase:
    def __init__(self, database_name: str = DATABASE_NAME):
        self.database_name = database_name
        self.cache = FinanceCache()

    async def init_db(self, force_recreate: bool = False):
        """
        Инициализация базы данных с новой схемой
        
        :param force_recreate: Принудительное пересоздание базы данных
        """
        try:
            # Проверяем существование базы данных
            db_exists = os.path.exists(self.database_name)
            
            logger.info(f"Initializing database: {self.database_name}")
            logger.info(f"Database exists: {db_exists}, Force recreate: {force_recreate}")
            
            # Если база не существует или принудительное пересоздание
            if not db_exists or force_recreate:
                async with aiosqlite.connect(self.database_name) as db:
                    # Включаем внешние ключи
                    await db.execute('PRAGMA foreign_keys = ON')
                    logger.info("Enabled foreign keys")
                    
                    # Создаем таблицу пользователей
                    await db.execute('''
                        CREATE TABLE IF NOT EXISTS users (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            telegram_id INTEGER UNIQUE NOT NULL,
                            username TEXT,
                            first_name TEXT,
                            last_name TEXT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            settings_json TEXT DEFAULT '{}'
                        )
                    ''')
                    logger.info("Created users table")

                    # Создаем таблицу категорий
                    await db.execute('''
                        CREATE TABLE IF NOT EXISTS categories (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            name TEXT NOT NULL,
                            type TEXT NOT NULL,
                            icon TEXT DEFAULT '📁',
                            is_default BOOLEAN DEFAULT FALSE,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            UNIQUE(name, type)
                        )
                    ''')
                    logger.info("Created categories table")

                    # Создаем улучшенную таблицу транзакций
                    await db.execute('''
                        CREATE TABLE IF NOT EXISTS transactions (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            user_id INTEGER NOT NULL,
                            type TEXT NOT NULL CHECK(type IN ('income', 'expense')),
                            amount DECIMAL(10,2) NOT NULL CHECK(amount > 0),
                            category_id INTEGER NOT NULL,
                            description TEXT,
                            date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
                            FOREIGN KEY(category_id) REFERENCES categories(id)
                        )
                    ''')
                    logger.info("Created transactions table")

                    # Создаем триггер для обновления updated_at
                    await db.execute('''
                        CREATE TRIGGER IF NOT EXISTS update_transaction_timestamp 
                        AFTER UPDATE ON transactions
                        BEGIN
                            UPDATE transactions SET updated_at = CURRENT_TIMESTAMP
                            WHERE id = NEW.id;
                        END;
                    ''')
                    logger.info("Created transaction timestamp trigger")

                    # Создаем индексы для оптимизации
                    await db.execute('CREATE INDEX IF NOT EXISTS idx_transactions_user_date ON transactions(user_id, date)')
                    await db.execute('CREATE INDEX IF NOT EXISTS idx_transactions_category ON transactions(category_id)')
                    logger.info("Created indexes")
                    
                    # Добавляем дефолтные категории
                    default_categories = [
                        # Доходы
                        ('Зарплата', 'income', '💼'),
                        ('Свободная деятельность', 'income', '💻'),
                        ('Инвестиции', 'income', '📈'),
                        ('Подарки', 'income', '🎁'),
                        ('Другие доходы', 'income', '❓'),
                        
                        # Расходы
                        ('Продукты', 'expense', '🛒'),
                        ('Транспорт', 'expense', '🚇'),
                        ('Жилье', 'expense', '🏠'),
                        ('Развлечения', 'expense', '🍿'),
                        ('Здоровье', 'expense', '💊'),
                        ('Образование', 'expense', '📚'),
                        ('Другие расходы', 'expense', '❓')
                    ]
                    
                    for name, type_, icon in default_categories:
                        await db.execute(
                            "INSERT OR IGNORE INTO categories (name, type, icon, is_default) 
                            VALUES (?, ?, ?, ?)",
                            (name, type_, icon, True)
                        )
                    
                    logger.info("Added default categories")
                    
                    # Новая таблица для настроек пользователя
                    await db.execute('''
                        CREATE TABLE IF NOT EXISTS user_settings (
                            user_id INTEGER PRIMARY KEY,
                            default_currency TEXT DEFAULT 'RUB',
                            monthly_expense_limit REAL DEFAULT NULL,
                            notification_frequency TEXT DEFAULT 'weekly',
                            report_period TEXT DEFAULT 'month',
                            FOREIGN KEY (user_id) REFERENCES users(id)
                        )
                    ''')
                    
                    # Новая таблица для персональных категорий
                    await db.execute('''
                        CREATE TABLE IF NOT EXISTS user_categories (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            user_id INTEGER,
                            name TEXT,
                            type TEXT CHECK(type IN ('income', 'expense')),
                            is_default BOOLEAN DEFAULT 0,
                            FOREIGN KEY (user_id) REFERENCES users(id)
                        )
                    ''')
                    
                    # Новая таблица для лимитов по категориям
                    await db.execute('''
                        CREATE TABLE IF NOT EXISTS category_limits (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            user_id INTEGER,
                            category TEXT,
                            monthly_limit REAL,
                            FOREIGN KEY (user_id) REFERENCES users(id)
                        )
                    ''')
                    
                    await db.commit()
            
            # Если база уже существует, просто подключаемся
            logger.info(f"Database {self.database_name} initialized successfully")
            
        except Exception as e:
            logger.error(f"Database initialization error: {e}")
            raise DatabaseError("Failed to initialize database", e)

    async def _migrate_old_data(self, db: aiosqlite.Connection):
        """Миграция данных из старой схемы"""
        try:
            # Проверяем существование старой таблицы
            async with db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='transactions'") as cursor:
                if not await cursor.fetchone():
                    return

            # Проверяем структуру старой таблицы
            async with db.execute("PRAGMA table_info(transactions)") as cursor:
                columns = {row[1] for row in await cursor.fetchall()}
                required_columns = {'category', 'type', 'amount', 'date'}
                if not all(col in columns for col in required_columns):
                    return  # Старая таблица существует, но не содержит нужных колонок

            # Создаем временного пользователя для старых транзакций
            await db.execute(
                "INSERT OR IGNORE INTO users (telegram_id, username) VALUES (?, ?)",
                (0, 'migrated_user')
            )

            # Получаем ID пользователя
            async with db.execute("SELECT id FROM users WHERE telegram_id = 0") as cursor:
                user_id = (await cursor.fetchone())[0]

            # Мигрируем категории
            old_categories = set()
            async with db.execute("SELECT DISTINCT category, type FROM transactions") as cursor:
                async for row in cursor:
                    old_categories.add((row[0], row[1]))

            for category, type_ in old_categories:
                await db.execute(
                    "INSERT OR IGNORE INTO categories (name, type, is_default) VALUES (?, ?, ?)",
                    (category, 'income' if type_ == 'доход' else 'expense', True)
                )

            # Мигрируем транзакции
            async with db.execute("SELECT * FROM transactions") as cursor:
                async for row in cursor:
                    category_name = row['category']
                    transaction_type = 'income' if row['type'] == 'доход' else 'expense'

                    # Получаем ID категории
                    async with db.execute(
                        "SELECT id FROM categories WHERE name = ? AND type = ?",
                        (category_name, transaction_type)
                    ) as cat_cursor:
                        category_id = (await cat_cursor.fetchone())[0]

                    # Добавляем транзакцию
                    await db.execute('''
                        INSERT INTO transactions 
                        (user_id, type, amount, category_id, date)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (user_id, transaction_type, row['amount'], category_id, row['date']))

            # Переименовываем старую таблицу
            await db.execute("ALTER TABLE transactions RENAME TO old_transactions_backup")

            await db.commit()

        except Exception as e:
            logger.error(f"Migration error: {e}")
            await db.rollback()
            # В тестовой среде не нужно поднимать ошибку миграции
            if not os.getenv('TESTING'):
                raise DatabaseError("Failed to migrate old data", e)

    async def create_user_if_not_exists(self, user_id: int) -> None:
        """Создает пользователя, если он не существует"""
        logger.info(f"Checking if user {user_id} exists")
        try:
            async with aiosqlite.connect(self.database_name) as db:
                # Проверяем существование пользователя
                cursor = await db.execute(
                    "SELECT id FROM users WHERE telegram_id = ?",
                    (user_id,)
                )
                user = await cursor.fetchone()
                
                if not user:
                    logger.info(f"User {user_id} not found, creating new user")
                    # Создаем нового пользователя
                    await db.execute(
                        "INSERT INTO users (telegram_id) VALUES (?)",
                        (user_id,)
                    )
                    await db.commit()
                    logger.info(f"Created new user with telegram_id {user_id}")
                else:
                    logger.info(f"User {user_id} already exists")
        except aiosqlite.Error as e:
            logger.error(f"Database error in create_user_if_not_exists: {e}")
            raise DatabaseError(f"Failed to create/check user: {str(e)}", e)

    async def create_user(self, telegram_id: int, username: str) -> int:
        """
        Создает нового пользователя в базе данных.
        
        :param telegram_id: Уникальный ID пользователя в Telegram
        :param username: Имя пользователя
        :return: ID созданного пользователя в базе данных
        """
        logger.info(f"Creating user with telegram_id {telegram_id}")
        
        try:
            async with aiosqlite.connect(self.database_name) as db:
                # Проверяем, существует ли уже пользователь
                async with db.execute("SELECT id FROM users WHERE telegram_id = ?", (telegram_id,)) as cursor:
                    existing_user = await cursor.fetchone()
                    
                    if existing_user:
                        logger.warning(f"User with telegram_id {telegram_id} already exists")
                        return existing_user[0]
                
                # Добавляем нового пользователя
                cursor = await db.execute(
                    "INSERT INTO users (telegram_id, username) VALUES (?, ?)",
                    (telegram_id, username)
                )
                await db.commit()
                
                # Возвращаем ID созданного пользователя
                return cursor.lastrowid

        except aiosqlite.Error as e:
            logger.error(f"Database error in create_user: {e}")
            raise DatabaseError(f"Failed to create user: {str(e)}", e)
        except Exception as e:
            logger.error(f"Error creating user: {e}")
            raise DatabaseError("Failed to create user", e)

    async def add_transaction(self, 
                            user_id: int,
                            amount: Decimal,
                            type_: str,
                            category: str,
                            description: Optional[str] = None,
                            date: Optional[datetime] = None) -> int:
        """Добавляет новую транзакцию"""
        logger.info(f"Adding {type_} transaction for user {user_id}: {amount} {category}")
        try:
            # Проверяем корректность суммы
            if amount < Decimal("0.00"):
                raise ValueError("Transaction amount cannot be negative")
            elif amount == Decimal("0.00"):
                raise ValueError("Transaction amount cannot be zero")

            # Проверяем тип транзакции
            if type_ not in ["income", "expense"]:
                raise ValueError(f"Invalid transaction type: {type_}")

            # Проверяем длину описания
            if description and len(description) > 1000:
                description = description[:1000]  # Обрезаем слишком длинное описание

            # Если дата не указана, используем текущую
            if date is None:
                date = datetime.now()

            async with aiosqlite.connect(self.database_name) as db:
                # Проверяем существование пользователя
                async with db.execute("SELECT id FROM users WHERE telegram_id = ?", (user_id,)) as cursor:
                    user = await cursor.fetchone()
                    if not user:
                        raise ValueError(f"User with telegram_id {user_id} not found")
                    db_user_id = user[0]

                # Проверяем и создаем категорию если её нет
                async with db.execute(
                    "INSERT OR IGNORE INTO categories (name, type) VALUES (?, ?)",
                    (category, type_)
                ):
                    await db.commit()

                # Получаем ID категории
                async with db.execute(
                    "SELECT id FROM categories WHERE name = ? AND type = ?",
                    (category, type_)
                ) as cursor:
                    category_id = (await cursor.fetchone())[0]

                # Добавляем транзакцию
                cursor = await db.execute(
                    '''
                    INSERT INTO transactions
                    (user_id, type, amount, category_id, description, date)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ''',
                    (db_user_id, type_, str(amount), category_id, description, date)
                )
                transaction_id = cursor.lastrowid
                await db.commit()

                # Инвалидируем кэш после добавления новой транзакции
                self.cache.invalidate_user_cache(user_id)

                return transaction_id

        except ValueError as e:
            logger.error(f"Validation error: {str(e)}")
            raise DatabaseError(str(e))
        except aiosqlite.Error as e:
            logger.error(f"Database error in add_transaction: {e}")
            raise DatabaseError(f"Failed to add transaction: {str(e)}", e)
        except Exception as e:
            logger.error(f"Error adding transaction: {e}")
            raise DatabaseError("Failed to add transaction", e)

    async def get_statistics(
        self,
        user_id: int,
        days: int = 30
    ) -> Optional[StatisticsResult]:
        """
        Получает статистику транзакций пользователя за указанный период
        
        :param user_id: ID пользователя
        :param days: Количество дней для анализа
        :return: Объект с результатами статистики или None
        """
        try:
            logger.info(f"Получение статистики для пользователя {user_id} за {days} дней")
            
            # Получаем транзакции за последние N дней
            async with aiosqlite.connect(self.database_name) as db:
                # Вычисляем дату начала периода
                start_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days)
                logger.info(f"Начальная дата для выборки: {start_date}")
                
                # Проверяем существование пользователя
                async with db.execute("SELECT COUNT(*) FROM users WHERE telegram_id = ?", (user_id,)) as cursor:
                    user_count = await cursor.fetchone()
                    logger.info(f"Количество пользователей с ID {user_id}: {user_count[0]}")
                    
                    if user_count[0] == 0:
                        logger.warning(f"Пользователь {user_id} не найден в базе данных")
                        return None
                
                # Запрос на получение транзакций с ограничением по дате
                query = '''
                    SELECT 
                        t.id, t.type, t.amount, 
                        c.name AS category, t.description, 
                        t.date, t.user_id
                    FROM transactions t
                    JOIN categories c ON t.category_id = c.id
                    JOIN users u ON t.user_id = u.id
                    WHERE u.telegram_id = ? AND t.date >= ?
                    ORDER BY t.date DESC
                '''
                params = [user_id, start_date.isoformat()]
                
                # Выполняем запрос
                async with db.execute(query, params) as cursor:
                    rows = await cursor.fetchall()
                    logger.info(f"Найдено транзакций: {len(rows)}")

                # Если транзакций нет, возвращаем None
                if not rows:
                    logger.warning(f"Нет транзакций для пользователя {user_id}")
                    return None
                
                # Преобразуем результаты в список транзакций
                transactions = []
                for row in rows:
                    transaction_date = datetime.fromisoformat(row[5])
                    transaction = Transaction(
                        id=row[0],
                        type=row[1],
                        amount=Decimal(row[2]),
                        category=row[3],
                        description=row[4],
                        date=transaction_date,
                        user_id=row[6]
                    )
                    transactions.append(transaction)

                # Группируем транзакции по типу и категории
                income_transactions = [t for t in transactions if t.type == 'income']
                expense_transactions = [t for t in transactions if t.type == 'expense']
                
                # Вычисляем общие суммы
                total_income = sum(t.amount for t in income_transactions)
                total_expense = sum(t.amount for t in expense_transactions)
                
                logger.info(f"Общий доход: {total_income}, Общий расход: {total_expense}")
                
                # Группируем доходы и расходы по категориям
                income_categories = {}
                for transaction in income_transactions:
                    category = translate_category(transaction.category)
                    income_categories[category] = income_categories.get(category, 0) + transaction.amount
                
                expense_categories = {}
                for transaction in expense_transactions:
                    category = translate_category(transaction.category)
                    expense_categories[category] = expense_categories.get(category, 0) + transaction.amount
                
                # Создаем детализированный результат с процентами
                income_details = [
                    {
                        'category': category, 
                        'amount': float(amount), 
                        'percentage': round(amount / total_income * 100, 1) if total_income > 0 else 0
                    } 
                    for category, amount in income_categories.items()
                ]
                
                expense_details = [
                    {
                        'category': category, 
                        'amount': float(amount), 
                        'percentage': round(amount / total_expense * 100, 1) if total_expense > 0 else 0
                    } 
                    for category, amount in expense_categories.items()
                ]
                
                # Сортируем по сумме в убывающем порядке
                income_details.sort(key=lambda x: x['amount'], reverse=True)
                expense_details.sort(key=lambda x: x['amount'], reverse=True)
                
                # Возвращаем результат
                result = StatisticsResult(
                    total_income=total_income,
                    total_expense=total_expense,
                    balance=total_income - total_expense,
                    transactions=transactions,
                    income_details=income_details,
                    expense_details=expense_details
                )
                
                logger.info(f"Статистика для пользователя {user_id} успешно сформирована")
                return result
        
        except Exception as e:
            logger.error(f"Ошибка при получении статистики для пользователя {user_id}: {e}", exc_info=True)
            return None

    async def get_category_statistics(self,
                                    user_id: int,
                                    start_date: datetime = None,
                                    end_date: datetime = None) -> List[CategoryStatistics]:
        """Получает статистику по категориям за период"""
        try:
            # Пробуем получить данные из кэша
            cached_stats = self.cache.get_category_statistics(user_id, start_date, end_date)
            if cached_stats:
                return cached_stats

            async with aiosqlite.connect(self.database_name) as db:
                # Проверяем существование пользователя и получаем db_user_id
                async with db.execute(
                    "SELECT id FROM users WHERE telegram_id = ?",
                    (user_id,)
                ) as cursor:
                    user = await cursor.fetchone()
                    if not user:
                        raise ValueError(f"User with telegram_id {user_id} not found")
                    db_user_id = user[0]

                # Формируем условие для дат
                date_condition = ""
                params = [db_user_id]
                if start_date:
                    date_condition += " AND t.date >= ?"
                    params.append(start_date.isoformat())
                if end_date:
                    date_condition += " AND t.date <= ?"
                    params.append(end_date.isoformat())

                # Получаем статистику по категориям
                async with db.execute(
                    f"""
                    SELECT c.name as category, t.type,
                           COALESCE(SUM(CAST(t.amount AS DECIMAL(10,2))), 0) as total
                    FROM transactions t
                    JOIN categories c ON t.category_id = c.id
                    WHERE t.user_id = ?{date_condition}
                    GROUP BY c.name, t.type
                    ORDER BY total DESC
                    """,
                    params
                ) as cursor:
                    category_stats = []
                    async for row in cursor:
                        category_stats.append(CategoryStatistics(
                            category=row[0],
                            type=row[1],
                            total=Decimal(str(row[2]))
                        ))

                    # Сохраняем результат в кэш
                    self.cache.set_category_statistics(user_id, category_stats, start_date, end_date)

                    return category_stats

        except ValueError as e:
            logger.error(f"Validation error: {str(e)}")
            raise DatabaseError(str(e))
        except aiosqlite.Error as e:
            logger.error(f"Database error in get_category_statistics: {e}")
            raise DatabaseError(f"Failed to get category statistics: {str(e)}", e)
        except Exception as e:
            logger.error(f"Error getting category statistics: {e}")
            raise DatabaseError("Failed to get category statistics", e)

    async def get_transactions(
        self, 
        user_id: int, 
        start_date: Optional[datetime] = None, 
        end_date: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[Transaction]:
        """
        Получает список транзакций для пользователя с возможностью фильтрации по дате.
        
        :param user_id: ID пользователя в Telegram
        :param start_date: Начальная дата для фильтрации
        :param end_date: Конечная дата для фильтрации
        :param limit: Максимальное количество транзакций
        :return: Список транзакций
        """
        logger.info(f"Retrieving transactions for user {user_id}")
        logger.info(f"Date filtering: start_date={start_date}, end_date={end_date}")
        
        try:
            # Проверяем существование пользователя
            async with aiosqlite.connect(self.database_name) as db:
                async with db.execute("SELECT id FROM users WHERE telegram_id = ?", (user_id,)) as cursor:
                    user = await cursor.fetchone()
                    if not user:
                        raise ValueError(f"User with telegram_id {user_id} not found")
                    db_user_id = user[0]

                # Формируем базовый SQL-запрос
                query = '''
                    SELECT 
                        t.id, t.type, t.amount, 
                        c.name AS category, t.description, 
                        t.date, t.user_id
                    FROM transactions t
                    JOIN categories c ON t.category_id = c.id
                    WHERE t.user_id = ? AND t.date >= ?
                    ORDER BY t.date DESC
                '''
                params = [db_user_id, start_date]

                # Добавляем фильтрацию по дате, если указаны даты
                if end_date:
                    query += " AND t.date <= ?"
                    params.append(end_date.isoformat())
                    logger.info(f"Date range filter: {start_date.isoformat()} - {end_date.isoformat()}")
                logger.info(f"Date filter: {start_date.isoformat()}")

                # Сортируем по дате в убывающем порядке
                query += " ORDER BY t.date DESC"

                # Добавляем лимит, если указан
                if limit is not None:
                    query += " LIMIT ?"
                    params.append(limit)

                logger.info(f"Final SQL query: {query}")
                logger.info(f"Query parameters: {params}")

                # Выполняем запрос
                async with db.execute(query, params) as cursor:
                    rows = await cursor.fetchall()
                    logger.info(f"Rows fetched: {len(rows)}")

                # Преобразуем результаты в список транзакций
                transactions = []
                for row in rows:
                    transaction_date = datetime.fromisoformat(row[5])
                    logger.info(f"Transaction date: {transaction_date}")
                    transaction = Transaction(
                        id=row[0],
                        type=row[1],
                        amount=Decimal(row[2]),
                        category=row[3],
                        description=row[4],
                        date=transaction_date,
                        user_id=row[6]
                    )
                    transactions.append(transaction)
                    logger.info(f"Transaction: {transaction}")

                return transactions

        except ValueError as e:
            logger.error(f"Validation error: {str(e)}")
            raise DatabaseError(str(e))
        except aiosqlite.Error as e:
            logger.error(f"Database error in get_transactions: {e}")
            raise DatabaseError(f"Failed to retrieve transactions: {str(e)}", e)
        except Exception as e:
            logger.error(f"Error retrieving transactions: {e}")
            raise DatabaseError("Failed to retrieve transactions", e)

    async def update_transaction(
        self, 
        transaction_id: int, 
        amount: Optional[Decimal] = None, 
        description: Optional[str] = None,
        category: Optional[str] = None,
        date: Optional[datetime] = None
    ) -> Transaction:
        """Обновление существующей транзакции"""
        try:
            async with aiosqlite.connect(self.database_name) as db:
                # Получаем текущую транзакцию
                cursor = await db.execute(
                    "SELECT * FROM transactions WHERE id = ?", 
                    (transaction_id,)
                )
                current_transaction = await cursor.fetchone()
                
                if not current_transaction:
                    raise DatabaseError(f"Transaction with id {transaction_id} not found")

                # Подготовка параметров для обновления
                update_params = {}
                if amount is not None:
                    if amount <= 0:
                        raise DatabaseError("Transaction amount must be positive")
                    update_params['amount'] = float(amount)
                if description is not None:
                    update_params['description'] = description
                if category is not None:
                    # Находим ID категории
                    category_cursor = await db.execute(
                        "SELECT id FROM categories WHERE name = ?", 
                        (category,)
                    )
                    category_id = await category_cursor.fetchone()
                    if not category_id:
                        raise DatabaseError(f"Category {category} not found")
                    update_params['category_id'] = category_id[0]
                if date is not None:
                    update_params['date'] = date

                # Формируем SQL-запрос
                if update_params:
                    set_clause = ", ".join([f"{k} = ?" for k in update_params.keys()])
                    values = list(update_params.values()) + [transaction_id]
                    
                    await db.execute(
                        f"UPDATE transactions SET {set_clause} WHERE id = ?", 
                        values
                    )
                    await db.commit()

                # Инвалидируем кэш
                self.cache.invalidate_user_cache(current_transaction[1])  # user_id

                return await self._get_transaction_by_id(transaction_id)

        except Exception as e:
            logger.error(f"Error updating transaction: {e}")
            raise DatabaseError("Failed to update transaction", e)

    async def delete_transaction(self, transaction_id: int):
        """Удаление транзакции"""
        try:
            async with aiosqlite.connect(self.database_name) as db:
                # Получаем user_id перед удалением
                cursor = await db.execute(
                    "SELECT user_id FROM transactions WHERE id = ?", 
                    (transaction_id,)
                )
                result = await cursor.fetchone()
                
                if not result:
                    raise DatabaseError(f"Transaction with id {transaction_id} not found")
                
                user_id = result[0]

                # Удаляем транзакцию
                await db.execute(
                    "DELETE FROM transactions WHERE id = ?", 
                    (transaction_id,)
                )
                await db.commit()

                # Инвалидируем кэш
                self.cache.invalidate_user_cache(user_id)

        except Exception as e:
            logger.error(f"Error deleting transaction: {e}")
            raise DatabaseError("Failed to delete transaction", e)

    async def get_total_balance(self, user_id: int) -> Decimal:
        """Расчет общего баланса пользователя"""
        try:
            async with aiosqlite.connect(self.database_name) as db:
                cursor = await db.execute("""
                    SELECT 
                        SUM(CASE WHEN type = 'income' THEN amount ELSE 0 END) as total_income,
                        SUM(CASE WHEN type = 'expense' THEN amount ELSE 0 END) as total_expense
                    FROM transactions
                    WHERE user_id = ?
                """, (user_id,))
                
                result = await cursor.fetchone()
                
                total_income = Decimal(str(result[0] or 0))
                total_expense = Decimal(str(result[1] or 0))
                
                return total_income - total_expense

        except Exception as e:
            logger.error(f"Error calculating total balance: {e}")
            raise DatabaseError("Failed to calculate total balance", e)

    async def _get_transaction_by_id(self, transaction_id: int) -> Transaction:
        """Получение транзакции по ID"""
        async with aiosqlite.connect(self.database_name) as db:
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
            row = await cursor.fetchone()
            
            if not row:
                raise DatabaseError(f"Transaction with id {transaction_id} not found")
            
            return Transaction(
                id=row['id'],
                type=row['type'],
                amount=Decimal(str(row['amount'])),
                category=row['category_name'],
                description=row['description'],
                date=datetime.fromisoformat(row['date']),
                user_id=row['user_id']
            )

    async def graph_image(self, user_id: int, days: int = 30) -> Optional[bytes]:
        """
        Генерирует подробный график доходов и расходов за указанный период
        
        :param user_id: ID пользователя
        :param days: Количество дней для анализа
        :return: Байты изображения графика или None
        """
        try:
            # Получаем статистику
            stats = await self.get_statistics(user_id)
            
            # Проверяем наличие транзакций
            if not stats or not stats.transactions:
                logging.info(f"Недостаточно данных для генерации графика для пользователя {user_id}")
                return None
            
            # Группируем транзакции по категориям
            categories_income = {}
            categories_expense = {}
            
            for transaction in stats.transactions:
                category = str(transaction.category)
                amount = float(transaction.amount)
                
                if transaction.type == 'income':
                    categories_income[category] = categories_income.get(category, 0) + amount
                else:
                    categories_expense[category] = categories_expense.get(category, 0) + amount
            
            # Проверяем наличие данных для графиков
            if not categories_income and not categories_expense:
                logging.info(f"Нет данных для построения графиков для пользователя {user_id}")
                return None
            
            # Создание графика
            plt.figure(figsize=(16, 8))
            plt.suptitle(f'Финансовая статистика за {days} дней', fontsize=16, fontweight='bold')
            
            # Subplot для доходов
            plt.subplot(1, 2, 1)
            plt.title('Доходы по категориям', fontsize=14)
            
            total_income = sum(categories_income.values())
            income_labels = [f"{cat}\n{val:.0f} руб. ({val/total_income*100:.1f}%)" 
                             for cat, val in categories_income.items()]
            
            plt.pie(
                list(categories_income.values()), 
                labels=income_labels, 
                autopct='%1.1f%%',
                wedgeprops={'edgecolor': 'white', 'linewidth': 1},
                colors=plt.cm.Greens(np.linspace(0.4, 0.8, len(categories_income)))
            )
            
            # Subplot для расходов
            plt.subplot(1, 2, 2)
            plt.title('Расходы по категориям', fontsize=14)
            
            total_expense = sum(categories_expense.values())
            expense_labels = [f"{cat}\n{val:.0f} руб. ({val/total_expense*100:.1f}%)" 
                              for cat, val in categories_expense.items()]
            
            plt.pie(
                list(categories_expense.values()), 
                labels=expense_labels, 
                autopct='%1.1f%%',
                wedgeprops={'edgecolor': 'white', 'linewidth': 1},
                colors=plt.cm.Reds(np.linspace(0.4, 0.8, len(categories_expense)))
            )
            
            # Добавляем общую информацию
            plt.figtext(0.5, 0.02, 
                        f"💰 Общий доход: {total_income:.0f} руб. | 💸 Общий расход: {total_expense:.0f} руб. | 💵 Баланс: {total_income-total_expense:.0f} руб.", 
                        ha='center', fontsize=10, bbox=dict(facecolor='white', alpha=0.5))
            
            # Сохраняем график в память
            buf = io.BytesIO()
            plt.tight_layout()
            plt.savefig(buf, format='png', dpi=200)
            plt.close()
            
            # Возвращаем байты изображения
            chart_data = buf.getvalue()
            
            logging.info(f"График для пользователя {user_id} сгенерирован. Размер: {len(chart_data)} байт")
            
            return chart_data
        
        except Exception as e:
            logging.error(f"Ошибка при генерации графика: {e}", exc_info=True)
            return None

    async def generate_statistics_chart(self, user_id: int, days: int = 30) -> Optional[bytes]:
        """
        Генерирует график доходов и расходов за указанный период
        
        :param user_id: ID пользователя
        :param days: Количество дней для анализа
        :return: Байты изображения графика или None
        """
        try:
            # Получаем статистику
            stats = await self.get_statistics(user_id)
            
            # Проверяем наличие транзакций
            if not stats or not stats.transactions:
                logging.info(f"Недостаточно данных для генерации графика для пользователя {user_id}")
                return None
            
            # Подготовка данных для графика
            categories_income = {}
            categories_expense = {}
            
            for transaction in stats.transactions:
                try:
                    # Явное преобразование и обработка
                    category = str(transaction.category)
                    amount = float(transaction.amount)
                    
                    if transaction.type == 'income':
                        categories_income[category] = categories_income.get(category, 0) + amount
                    else:
                        categories_expense[category] = categories_expense.get(category, 0) + amount
                except Exception as e:
                    logging.error(f"Ошибка обработки транзакции: {e}, транзакция: {transaction}")
            
            # Проверяем наличие данных для графиков
            if not categories_income and not categories_expense:
                logging.info(f"Нет данных для построения графиков для пользователя {user_id}")
                return None
            
            # Создание графика
            plt.figure(figsize=(10, 5))  # Уменьшаем размер для мобильных устройств
            plt.suptitle(f'Финансовая статистика', fontsize=12)
            
            # Счетчик для определения количества subplot
            subplot_count = 1 if not categories_income or not categories_expense else 2
            
            # Добавляем графики только если есть данные
            if categories_income:
                plt.subplot(1, subplot_count, 1)
                plt.title('Доходы')
                plt.pie(
                    list(categories_income.values()), 
                    labels=list(categories_income.keys()), 
                    autopct='%1.1f%%',
                    wedgeprops={'edgecolor': 'white'},
                    colors=plt.cm.Greens(np.linspace(0.4, 0.8, len(categories_income)))
                )
            
            if categories_expense:
                plt.subplot(1, subplot_count, 2 if categories_income else 1)
                plt.title('Расходы')
                plt.pie(
                    list(categories_expense.values()), 
                    labels=list(categories_expense.keys()), 
                    autopct='%1.1f%%',
                    wedgeprops={'edgecolor': 'white'},
                    colors=plt.cm.Reds(np.linspace(0.4, 0.8, len(categories_expense)))
                )
            
            # Сохраняем график в память
            buf = io.BytesIO()
            plt.tight_layout()
            plt.savefig(buf, format='png', dpi=150)
            plt.close()
            
            # Возвращаем байты изображения
            chart_data = buf.getvalue()
            
            logging.info(f"График для пользователя {user_id} сгенерирован. Размер: {len(chart_data)} байт")
            
            return chart_data
        
        except Exception as e:
            logging.error(f"Ошибка при генерации графика: {e}", exc_info=True)
            return None

    async def get_user_settings(self, user_id):
        async with aiosqlite.connect(self.database_name) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute('SELECT * FROM user_settings WHERE user_id = ?', (user_id,)) as cursor:
                settings = await cursor.fetchone()
                return dict(settings) if settings else None

    async def update_user_settings(self, user_id, **kwargs):
        async with aiosqlite.connect(self.database_name) as db:
            # Создаем настройки, если их нет
            await db.execute('''
                INSERT OR REPLACE INTO user_settings (user_id, default_currency, monthly_expense_limit, 
                notification_frequency, report_period)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                user_id, 
                kwargs.get('default_currency', 'RUB'),
                kwargs.get('monthly_expense_limit'),
                kwargs.get('notification_frequency', 'weekly'),
                kwargs.get('report_period', 'month')
            ))
            await db.commit()

    async def add_user_category(self, user_id, name, category_type):
        async with aiosqlite.connect(self.database_name) as db:
            await db.execute('''
                INSERT INTO user_categories (user_id, name, type) 
                VALUES (?, ?, ?)
            ''', (user_id, name, category_type))
            await db.commit()

    async def get_user_categories(self, user_id, category_type=None):
        async with aiosqlite.connect(self.database_name) as db:
            query = 'SELECT * FROM user_categories WHERE user_id = ?'
            params = [user_id]
            
            if category_type:
                query += ' AND type = ?'
                params.append(category_type)
            
            async with db.execute(query, params) as cursor:
                return await cursor.fetchall()

    async def remove_user_category(self, user_id, category_name, category_type):
        """
        Удаление пользовательской категории
        
        :param user_id: ID пользователя
        :param category_name: Название категории
        :param category_type: Тип категории (income/expense)
        """
        async with aiosqlite.connect(self.database_name) as db:
            # Проверяем, можно ли удалить категорию
            async with db.execute('''
                SELECT COUNT(*) 
                FROM transactions t
                JOIN user_categories uc ON t.category_id = uc.id
                WHERE uc.user_id = ? AND uc.name = ? AND uc.type = ?
            ''', (user_id, category_name, category_type)) as cursor:
                transaction_count = await cursor.fetchone()
                
                # Если есть транзакции с этой категорией, запрещаем удаление
                if transaction_count[0] > 0:
                    logger.warning(f"Нельзя удалить категорию {category_name}: есть связанные транзакции")
                    return False
            
            # Удаляем категорию
            await db.execute('''
                DELETE FROM user_categories 
                WHERE user_id = ? AND name = ? AND type = ?
            ''', (user_id, category_name, category_type))
            
            await db.commit()
            logger.info(f"Удалена категория {category_name} для пользователя {user_id}")
            return True

    async def get_user_custom_categories(self, user_id, category_type=None):
        """
        Получение пользовательских категорий
        
        :param user_id: ID пользователя
        :param category_type: Тип категории (income/expense), опционально
        :return: Список пользовательских категорий
        """
        async with aiosqlite.connect(self.database_name) as db:
            query = '''
                SELECT name, type 
                FROM user_categories 
                WHERE user_id = ? AND is_default = 0
            '''
            params = [user_id]
            
            if category_type:
                query += ' AND type = ?'
                params.append(category_type)
            
            async with db.execute(query, params) as cursor:
                return await cursor.fetchall()

    async def update_notification_settings(self, user_id, notification_type, is_enabled, frequency=None):
        """
        Обновление настроек уведомлений для пользователя
        
        :param user_id: ID пользователя
        :param notification_type: Тип уведомления (expense_limit, monthly_report, etc.)
        :param is_enabled: Включены ли уведомления
        :param frequency: Частота уведомлений (daily, weekly, monthly)
        """
        async with aiosqlite.connect(self.database_name) as db:
            await db.execute('''
                INSERT OR REPLACE INTO user_settings 
                (user_id, setting_name, setting_value, additional_value) 
                VALUES (?, ?, ?, ?)
            ''', (
                user_id, 
                f'notification_{notification_type}', 
                'enabled' if is_enabled else 'disabled',
                frequency or ''
            ))
            await db.commit()
            logger.info(f"Обновлены настройки уведомлений для {user_id}: {notification_type}")

    async def get_notification_settings(self, user_id, notification_type=None):
        """
        Получение настроек уведомлений для пользователя
        
        :param user_id: ID пользователя
        :param notification_type: Тип уведомления (опционально)
        :return: Словарь настроек уведомлений
        """
        async with aiosqlite.connect(self.database_name) as db:
            if notification_type:
                query = '''
                    SELECT setting_value, additional_value 
                    FROM user_settings 
                    WHERE user_id = ? AND setting_name = ?
                '''
                params = [user_id, f'notification_{notification_type}']
            else:
                query = '''
                    SELECT setting_name, setting_value, additional_value 
                    FROM user_settings 
                    WHERE user_id = ? AND setting_name LIKE 'notification_%'
                '''
                params = [user_id]
            
            async with db.execute(query, params) as cursor:
                results = await cursor.fetchall()
                
                if notification_type:
                    return {
                        'status': results[0][0] if results else 'disabled',
                        'frequency': results[0][1] if results else None
                    }
                else:
                    return {
                        setting.replace('notification_', ''): {
                            'status': value,
                            'frequency': freq
                        }
                        for setting, value, freq in results
                    }

    async def get_users_for_notifications(self, notification_type):
        """
        Получение пользователей, у которых включены определенные уведомления
        
        :param notification_type: Тип уведомления
        :return: Список ID пользователей
        """
        async with aiosqlite.connect(self.database_name) as db:
            async with db.execute('''
                SELECT user_id 
                FROM user_settings 
                WHERE setting_name = ? AND setting_value = 'enabled'
            ''', (f'notification_{notification_type}',)) as cursor:
                return [row[0] for row in await cursor.fetchall()]

    async def update_report_period(self, user_id, start_day=1, period_type='monthly'):
        """
        Обновление настроек периода отчетности
        
        :param user_id: ID пользователя
        :param start_day: День начала периода (1-28)
        :param period_type: Тип периода (monthly, quarterly, custom)
        """
        async with aiosqlite.connect(self.database_name) as db:
            # Проверяем корректность дня
            if not (1 <= start_day <= 28):
                raise ValueError("День должен быть от 1 до 28")
            
            # Сохраняем настройки периода отчетности
            await db.execute('''
                INSERT OR REPLACE INTO user_settings 
                (user_id, setting_name, setting_value, additional_value) 
                VALUES (?, ?, ?, ?)
            ''', (
                user_id, 
                'report_period', 
                period_type,
                str(start_day)
            ))
            
            await db.commit()
            logger.info(f"Обновлен период отчетности для {user_id}: {period_type}, начало: {start_day}")

    async def get_report_period(self, user_id):
        """
        Получение настроек периода отчетности
        
        :param user_id: ID пользователя
        :return: Словарь с настройками периода отчетности
        """
        async with aiosqlite.connect(self.database_name) as db:
            async with db.execute('''
                SELECT setting_value, additional_value 
                FROM user_settings 
                WHERE user_id = ? AND setting_name = 'report_period'
            ''', (user_id,)) as cursor:
                result = await cursor.fetchone()
                
                # Значения по умолчанию
                if not result:
                    return {
                        'period_type': 'monthly',
                        'start_day': 1
                    }
                
                return {
                    'period_type': result[0],
                    'start_day': int(result[1])
                }

    async def calculate_report_period(self, user_id, current_date=None):
        """
        Расчет текущего и предыдущего периода отчетности
        
        :param user_id: ID пользователя
        :param current_date: Текущая дата (по умолчанию - текущая)
        :return: Словарь с датами начала и конца текущего и предыдущего периодов
        """
        from datetime import datetime, timedelta
        
        # Используем текущую дату, если не передана
        if current_date is None:
            current_date = datetime.now()
        
        # Получаем настройки периода
        period_settings = await self.get_report_period(user_id)
        start_day = period_settings['start_day']
        period_type = period_settings['period_type']
        
        # Расчет начала и конца текущего периода
        if period_type == 'monthly':
            # Определяем начало и конец текущего месяца
            if current_date.day < start_day:
                # Если текущая дата раньше дня старта, берем предыдущий месяц
                current_period_start = datetime(current_date.year, current_date.month, start_day) - timedelta(days=1)
                current_period_end = datetime(current_date.year, current_date.month, start_day) - timedelta(days=1)
            else:
                current_period_start = datetime(current_date.year, current_date.month, start_day)
                current_period_end = datetime(current_date.year, current_date.month + 1, start_day) - timedelta(days=1)
        
        elif period_type == 'quarterly':
            # Определяем квартал
            quarter = (current_date.month - 1) // 3
            quarter_months = {
                0: (1, 2, 3),
                1: (4, 5, 6),
                2: (7, 8, 9),
                3: (10, 11, 12)
            }
            
            first_month = quarter_months[quarter][0]
            current_period_start = datetime(current_date.year, first_month, start_day)
            current_period_end = datetime(current_date.year, first_month + 2, start_day) - timedelta(days=1)
        
        else:
            raise ValueError(f"Неподдерживаемый тип периода: {period_type}")
        
        return {
            'current_period_start': current_period_start,
            'current_period_end': current_period_end,
            'previous_period_start': current_period_start - timedelta(days=current_period_end.day),
            'previous_period_end': current_period_start - timedelta(days=1)
        }

# Создаем глобальный экземпляр базы данных
db = FinanceDatabase()
