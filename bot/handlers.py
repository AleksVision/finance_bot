from aiogram import Router, types, F
from aiogram.filters import Command
from aiogram.types import CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder

from decimal import Decimal, InvalidOperation
from datetime import datetime, timedelta
import logging

from bot.database import FinanceDatabase, DatabaseError

# Настройка логирования
logger = logging.getLogger(__name__)

# Создаем роутер для обработчиков
router = Router()

class TransactionType:
    INCOME = 'income'
    EXPENSE = 'expense'

class Categories:
    INCOME = {
        'salary': 'Зарплата',
        'freelance': 'Фриланс',
        'investments': 'Инвестиции',
        'gifts': 'Подарки',
        'other_income': 'Другое'
    }
    
    EXPENSE = {
        'food': 'Продукты',
        'transport': 'Транспорт',
        'housing': 'Жилье',
        'entertainment': 'Развлечения',
        'health': 'Здоровье',
        'clothes': 'Одежда',
        'electronics': 'Техника',
        'other_expense': 'Другое'
    }

class FinanceForm(StatesGroup):
    waiting_for_transaction_type = State()
    waiting_for_category = State()
    waiting_for_amount = State()

class KeyboardFactory:
    @staticmethod
    def get_main_keyboard():
        builder = ReplyKeyboardBuilder()
        builder.button(text="💰 Доходы")
        builder.button(text="💸 Расходы")
        builder.button(text="📊 Статистика")
        builder.button(text="⚙️ Настройки")
        builder.adjust(2, 2)
        return builder.as_markup(resize_keyboard=True, one_time_keyboard=True)

    @staticmethod
    def get_transaction_type_keyboard():
        builder = InlineKeyboardBuilder()
        builder.button(text="💰 Доход", callback_data="transaction_income")
        builder.button(text="💸 Расход", callback_data="transaction_expense")
        builder.button(text="🏠 Главное меню", callback_data="main_menu")
        builder.adjust(2, 1)
        return builder.as_markup()

    @staticmethod
    def get_category_inline_keyboard(transaction_type: str):
        logger.info(f"Создание клавиатуры для типа транзакции: {transaction_type}")
        
        builder = InlineKeyboardBuilder()
        
        categories = (
            Categories.INCOME 
            if transaction_type == TransactionType.INCOME 
            else Categories.EXPENSE
        )
        logger.info(f"Категории: {categories}")
        
        emoji_map = {
            'salary': '💼', 'freelance': '💻', 'investments': '📈', 'gifts': '🎁', 'other_income': '❓',
            'food': '🍽️', 'transport': '🚇', 'housing': '🏠', 'entertainment': '🎉', 
            'health': '💊', 'clothes': '👚', 'electronics': '💻', 'other_expense': '❓'
        }
        
        for key, value in categories.items():
            emoji = emoji_map.get(key, '💰')
            button_text = f"{emoji} {value}"
            callback_data = f"category_{key}"
            
            logger.info(f"Добавление кнопки: {button_text}, callback_data: {callback_data}")
            builder.button(
                text=button_text, 
                callback_data=callback_data
            )
        
        builder.button(text="❌ Отмена", callback_data="cancel")
        builder.button(text="🏠 Главное меню", callback_data="main_menu")
        builder.adjust(2, 1)
        
        keyboard = builder.as_markup()
        logger.info(f"Клавиатура создана: {keyboard}")
        return keyboard

    @staticmethod
    def get_confirmation_keyboard():
        builder = InlineKeyboardBuilder()
        builder.button(text="✅ Подтвердить", callback_data="confirm")
        builder.button(text="❌ Отменить", callback_data="cancel")
        builder.adjust(2)
        return builder.as_markup()

class FinanceHandler:
    def __init__(self):
        self.db = FinanceDatabase()
        self.keyboard_factory = KeyboardFactory()

    async def start_transaction_income(self, message: types.Message, state: FSMContext):
        """Начало процесса добавления транзакции"""
        try:
            logger.info(f"Начало транзакции: тип = {TransactionType.INCOME}")
            logger.info(f"Тип сообщения: {type(message)}")
            logger.info(f"Текст сообщения: {message.text}")
            
            # Создаем пользователя, если не существует
            await self.db.create_user_if_not_exists(message.from_user.id)
            
            # Сохраняем тип транзакции в состоянии
            await state.update_data(transaction_type=TransactionType.INCOME)
            
            # Переводим в состояние выбора категории
            await state.set_state(FinanceForm.waiting_for_category)
            
            # Получаем клавиатуру с категориями
            keyboard = self.keyboard_factory.get_category_inline_keyboard(TransactionType.INCOME)
            logger.info(f"Клавиатура: {keyboard}")
            
            # Отправляем сообщение с выбором категории
            text = "📥 Выберите статью дохода"
            
            logger.info(f"Отправка сообщения: {text}")
            logger.info(f"Клавиатура: {keyboard}")
            
            # Проверяем, является ли message объектом Message
            if hasattr(message, 'answer'):
                await message.answer(text, reply_markup=keyboard)
            elif hasattr(message, 'message') and hasattr(message.message, 'answer'):
                await message.message.answer(text, reply_markup=keyboard)
            else:
                logger.error(f"Не удалось отправить сообщение. Неподдерживаемый тип: {type(message)}")
        except Exception as e:
            logger.error(f"Ошибка при старте транзакции: {e}", exc_info=True)
            if hasattr(message, 'answer'):
                await message.answer("❌ Произошла ошибка. Попробуйте снова.")
            await state.clear()

    async def start_transaction_expense(self, message: types.Message, state: FSMContext):
        """Начало процесса добавления транзакции"""
        try:
            logger.info(f"Начало транзакции: тип = {TransactionType.EXPENSE}")
            logger.info(f"Тип сообщения: {type(message)}")
            logger.info(f"Текст сообщения: {message.text}")
            
            # Создаем пользователя, если не существует
            await self.db.create_user_if_not_exists(message.from_user.id)
            
            # Сохраняем тип транзакции в состоянии
            await state.update_data(transaction_type=TransactionType.EXPENSE)
            
            # Переводим в состояние выбора категории
            await state.set_state(FinanceForm.waiting_for_category)
            
            # Получаем клавиатуру с категориями
            keyboard = self.keyboard_factory.get_category_inline_keyboard(TransactionType.EXPENSE)
            logger.info(f"Клавиатура: {keyboard}")
            
            # Отправляем сообщение с выбором категории
            text = "📤 Выберите статью расхода"
            
            logger.info(f"Отправка сообщения: {text}")
            logger.info(f"Клавиатура: {keyboard}")
            
            # Проверяем, является ли message объектом Message
            if hasattr(message, 'answer'):
                await message.answer(text, reply_markup=keyboard)
            elif hasattr(message, 'message') and hasattr(message.message, 'answer'):
                await message.message.answer(text, reply_markup=keyboard)
            else:
                logger.error(f"Не удалось отправить сообщение. Неподдерживаемый тип: {type(message)}")
        except Exception as e:
            logger.error(f"Ошибка при старте транзакции: {e}", exc_info=True)
            if hasattr(message, 'answer'):
                await message.answer("❌ Произошла ошибка. Попробуйте снова.")
            await state.clear()

    async def process_category_callback(self, callback: CallbackQuery, state: FSMContext):
        try:
            # Проверяем, что это callback выбора категории
            if not callback.data.startswith('category_'):
                return

            # Извлекаем категорию
            category = callback.data.split('_')[1]
            
            # Получаем текущий тип транзакции из состояния
            data = await state.get_data()
            transaction_type = data.get('transaction_type')

            # Проверяем корректность категории
            categories = (
                Categories.INCOME 
                if transaction_type == TransactionType.INCOME 
                else Categories.EXPENSE
            )
            
            if category not in categories:
                await callback.message.answer("❌ Некорректная категория. Попробуйте снова.")
                return

            # Сохраняем категорию и переводим в состояние ввода суммы
            await state.update_data(category=category)
            await state.set_state(FinanceForm.waiting_for_amount)

            # Просим ввести сумму
            text = (
                f"💰 Введите сумму дохода ({categories[category]})" 
                if transaction_type == TransactionType.INCOME 
                else f"💸 Введите сумму расхода ({categories[category]})"
            )
            await callback.message.answer(
                text, 
                reply_markup=self.keyboard_factory.get_confirmation_keyboard()
            )
            await callback.answer()
        except Exception as e:
            logger.error(f"Ошибка при обработке категории: {e}")
            await callback.message.answer("❌ Произошла ошибка. Попробуйте снова.")
            await state.clear()

    async def cancel_transaction(self, callback: CallbackQuery, state: FSMContext):
        """Отмена текущей транзакции"""
        try:
            await callback.message.answer(
                "❌ Транзакция отменена", 
                reply_markup=self.keyboard_factory.get_main_keyboard()
            )
            await state.clear()
            await callback.answer()
        except Exception as e:
            logger.error(f"Ошибка при отмене транзакции: {e}")

    async def main_menu(self, callback: CallbackQuery, state: FSMContext):
        """Возврат в главное меню"""
        try:
            await callback.message.answer(
                "🏠 Вы вернулись в главное меню", 
                reply_markup=self.keyboard_factory.get_main_keyboard()
            )
            await state.clear()
            await callback.answer()
        except Exception as e:
            logger.error(f"Ошибка при возврате в главное меню: {e}")

    async def process_amount(self, message: types.Message, state: FSMContext):
        try:
            # Проверяем корректность суммы
            try:
                amount = Decimal(message.text.replace(',', '.'))
                if amount <= 0:
                    raise ValueError("Сумма должна быть положительной")
            except (ValueError, InvalidOperation):
                await message.answer(
                    "❌ Некорректная сумма. Введите число больше нуля.",
                    reply_markup=self.keyboard_factory.get_confirmation_keyboard()
                )
                return

            # Получаем данные о транзакции
            data = await state.get_data()
            transaction_type = data.get('transaction_type')
            category = data.get('category')

            # Добавляем транзакцию в базу данных
            transaction = await self.db.add_transaction(
                user_id=message.from_user.id,
                amount=amount,
                type_=transaction_type,
                category=category
            )

            # Формируем сообщение о результате
            category_name = (
                Categories.INCOME.get(category, category) 
                if transaction_type == TransactionType.INCOME 
                else Categories.EXPENSE.get(category, category)
            )
            
            result_message = (
                f"✅ Доход добавлен:\n"
                f"Сумма: {amount} руб.\n"
                f"Категория: {category_name}"
            ) if transaction_type == TransactionType.INCOME else (
                f"✅ Расход добавлен:\n"
                f"Сумма: {amount} руб.\n"
                f"Категория: {category_name}"
            )

            # Отправляем подтверждение и возвращаем в главное меню
            await message.answer(
                result_message, 
                reply_markup=self.keyboard_factory.get_main_keyboard()
            )

            # Логируем транзакцию
            logger.info(
                f"Транзакция добавлена: "
                f"user_id={message.from_user.id}, "
                f"amount={amount}, "
                f"type={transaction_type}, "
                f"category={category}"
            )

            # Очищаем состояние
            await state.clear()

        except Exception as e:
            logger.error(f"Ошибка при обработке суммы: {e}", exc_info=True)
            await message.answer(
                "❌ Произошла ошибка при добавлении транзакции. Попробуйте снова.", 
                reply_markup=self.keyboard_factory.get_main_keyboard()
            )
            await state.clear()

    async def show_statistics(self, message: types.Message):
        try:
            # Создаем пользователя, если не существует
            await self.db.create_user_if_not_exists(message.from_user.id)
            
            # Получаем статистику
            stats = await self.db.get_statistics(
                user_id=message.from_user.id,
                start_date=datetime.now() - timedelta(days=30)
            )
            
            # Формируем детальную статистику по категориям
            income_categories = {}
            expense_categories = {}
            
            for transaction in stats.transactions:
                if transaction.type == TransactionType.INCOME:
                    income_categories[transaction.category] = income_categories.get(transaction.category, 0) + transaction.amount
                else:
                    expense_categories[transaction.category] = expense_categories.get(transaction.category, 0) + transaction.amount
            
            # Сортируем категории по сумме
            sorted_income_categories = sorted(income_categories.items(), key=lambda x: x[1], reverse=True)
            sorted_expense_categories = sorted(expense_categories.items(), key=lambda x: x[1], reverse=True)
            
            # Формируем текст сообщения
            message_text = "📊 Статистика за последние 30 дней:\n\n"
            message_text += f"💰 Общий доход: {stats.total_income} руб.\n"
            message_text += f"💸 Общий расход: {stats.total_expense} руб.\n"
            message_text += f"💵 Баланс: {stats.total_income - stats.total_expense} руб.\n\n"
            
            # Доходы по категориям
            message_text += "📈 Доходы по категориям:\n"
            for category, amount in sorted_income_categories:
                category_name = Categories.INCOME.get(category, category)
                message_text += f"- {category_name}: {amount} руб. ({amount/stats.total_income*100:.1f}%)\n"
            
            # Расходы по категориям
            message_text += "\n📉 Расходы по категориям:\n"
            for category, amount in sorted_expense_categories:
                category_name = Categories.EXPENSE.get(category, category)
                message_text += f"- {category_name}: {amount} руб. ({amount/stats.total_expense*100:.1f}%)\n"
            
            # Отправляем статистику
            await message.answer(
                message_text, 
                reply_markup=self.keyboard_factory.get_main_keyboard()
            )
            
            # Логируем запрос статистики
            logger.info(
                f"Статистика запрошена: "
                f"user_id={message.from_user.id}, "
                f"total_income={stats.total_income}, "
                f"total_expense={stats.total_expense}"
            )
        
        except Exception as e:
            logger.error(f"Ошибка при получении статистики: {e}", exc_info=True)
            await message.answer(
                "❌ Не удалось получить статистику. Попробуйте позже.", 
                reply_markup=self.keyboard_factory.get_main_keyboard()
            )

    async def start_command(self, message: types.Message):
        """Обработчик команды /start"""
        try:
            # Создаем пользователя, если не существует
            await self.db.create_user_if_not_exists(message.from_user.id)
            
            # Отправляем приветственное сообщение
            await message.answer(
                f"👋 Привет, {message.from_user.first_name}! \n\n"
                "Я твой личный финансовый помощник 💰\n\n"
                "Что умею:\n"
                "• 💰 Добавлять доходы\n"
                "• 💸 Учитывать расходы\n"
                "• 📊 Показывать статистику\n\n"
                "Выбери действие в меню или используй команды:",
                reply_markup=self.keyboard_factory.get_main_keyboard()
            )
        except Exception as e:
            logger.error(f"Ошибка при обработке команды /start: {e}")
            await message.answer("❌ Произошла ошибка. Попробуйте позже.")

def register_handlers(router: Router):
    handler = FinanceHandler()

    # Обработчики для кнопок и команд добавления транзакций
    router.message.register(
        handler.start_transaction_income, 
        F.text == "💰 Доходы"
    )
    router.message.register(
        handler.start_transaction_expense, 
        F.text == "💸 Расходы"
    )
    router.message.register(
        handler.start_transaction_income, 
        Command("add_income")
    )
    router.message.register(
        handler.start_transaction_expense, 
        Command("add_expense")
    )

    # Обработчики для выбора категории
    router.callback_query.register(
        handler.process_category_callback,
        F.data.startswith("category_")
    )

    # Обработчики отмены и возврата в главное меню
    router.callback_query.register(
        handler.cancel_transaction,
        F.data == "cancel"
    )
    router.callback_query.register(
        handler.main_menu,
        F.data == "main_menu"
    )

    # Обработчики ввода суммы
    router.message.register(
        handler.process_amount, 
        FinanceForm.waiting_for_amount
    )

    # Обработчики статистики
    router.message.register(
        handler.show_statistics, 
        F.text == "📊 Статистика"
    )

    # Обработчик команды /start
    router.message.register(
        handler.start_command, 
        Command("start")
    )

# Создаем глобальный экземпляр обработчика
finance_handler = FinanceHandler()
