from aiogram import Router, types, F
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InputFile
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder

from decimal import Decimal, InvalidOperation
from datetime import datetime, timedelta
import logging
import io

from bot.database import FinanceDatabase, DatabaseError

from typing import Dict, Any
from aiogram import types
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, User, Message, InputFile

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
        'freelance': 'Свободная деятельность',
        'investments': 'Инвестиции',
        'gifts': 'Подарки',
        'other_income': 'Другие доходы'
    }
    
    EXPENSE = {
        'food': 'Продукты питания',
        'transport': 'Транспортные расходы',
        'housing': 'Жилищные расходы',
        'entertainment': 'Развлечения',
        'health': 'Здоровье',
        'clothes': 'Одежда и обувь',
        'electronics': 'Электроника',
        'other_expense': 'Другие расходы'
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
        builder.button.text("❌ Отменить", callback_data="cancel")
        builder.adjust(2)
        return builder.as_markup()

    @staticmethod
    def get_statistics_keyboard():
        builder = InlineKeyboardBuilder()
        builder.button(text="📊 Показать статистику", callback_data="show_statistics")
        builder.button(text="📈 Показать график", callback_data="show_chart")
        builder.adjust(2)
        return builder.as_markup()

class FinanceHandler:
    def __init__(self):
        self.db = FinanceDatabase()
        self.keyboard_factory = KeyboardFactory()

    def get_transaction_type_text(self, data: Dict[str, Any]) -> str:
        """
        Получает текстовое описание типа транзакции
        """
        transaction_type = data.get('transaction_type')
        if transaction_type == TransactionType.INCOME:
            return "дохода"
        elif transaction_type == TransactionType.EXPENSE:
            return "расхода"
        return ""

    async def start_transaction(self, message: types.Message, state: FSMContext, transaction_type: str):
        """
        Начало процесса добавления транзакции
        """
        try:
            # Создаем пользователя, если не существует
            await self.db.create_user_if_not_exists(message.from_user.id)

            # Обновляем состояние типом транзакции
            await state.update_data(transaction_type=transaction_type)
            await state.set_state(FinanceForm.waiting_for_category)

            # Формируем текст и клавиатуру в зависимости от типа транзакции
            text = (
                "📥 Выберите статью дохода" 
                if transaction_type == TransactionType.INCOME 
                else "📤 Выберите статью расхода"
            )

            # Выбираем клавиатуру категорий
            keyboard = (
                self.keyboard_factory.get_category_inline_keyboard(TransactionType.INCOME) 
                if transaction_type == TransactionType.INCOME 
                else self.keyboard_factory.get_category_inline_keyboard(TransactionType.EXPENSE)
            )

            # Отправляем сообщение
            await message.answer(text, reply_markup=keyboard)

        except Exception as e:
            logger.error(f"Ошибка при начале транзакции: {e}")
            await message.answer("❌ Произошла ошибка. Попробуйте позже.")

    async def start_transaction_income(self, message: types.Message, state: FSMContext):
        """Начало процесса добавления транзакции"""
        await self.start_transaction(message, state, TransactionType.INCOME)

    async def start_transaction_expense(self, message: types.Message, state: FSMContext):
        """Начало процесса добавления транзакции"""
        await self.start_transaction(message, state, TransactionType.EXPENSE)

    async def process_category_callback(self, callback: CallbackQuery, state: FSMContext):
        # Получаем текущие данные состояния
        data = await state.get_data()
        transaction_type = data.get('transaction_type')

        # Извлекаем категорию из callback.data
        category = callback.data.split('_', 1)[1]

        # Обновляем состояние с новой категорией
        await state.update_data(category=category)

        # Отправляем подтверждение и переходим к следующему шагу
        await callback.message.answer("Выберите сумму транзакции:")
        await state.set_state(FinanceForm.waiting_for_amount)
        await callback.answer()

    async def process_amount(self, message: types.Message, state: FSMContext):
        try:
            # Получаем текущие данные состояния
            data = await state.get_data()
            transaction_type = data.get('transaction_type')
            category = data.get('category')

            # Проверяем корректность суммы
            try:
                amount = Decimal(message.text.replace(',', '.'))
                if amount <= 0:
                    raise InvalidOperation
            except (InvalidOperation, ValueError):
                await message.answer("❌ Некорректная сумма. Пожалуйста, введите число.")
                return

            # Добавляем транзакцию
            await self.db.add_transaction(
                user_id=message.from_user.id,
                amount=amount,
                type_=transaction_type.value if hasattr(transaction_type, 'value') else transaction_type,
                category=category
            )

            # Очищаем состояние и показываем успешное сообщение
            await message.answer(f"✅ Транзакция {transaction_type} на сумму {amount} добавлена.")
            await state.set_state(None)  # Сбрасываем состояние, но не очищаем данные полностью

        except Exception as e:
            logger.error(f"Ошибка при обработке суммы: {e}")
            await message.answer("❌ Произошла ошибка при добавлении транзакции.")
            await state.set_state(None)

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

    async def show_statistics(self, message: types.Message):
        """
        Отображение статистики за последний период
        """
        try:
            # Создаем пользователя, если его нет
            await self.db.create_user_if_not_exists(message.from_user.id)
            
            # Получаем статистику
            stats = await self.db.get_statistics(message.from_user.id)
            
            if not stats:
                await message.answer("У вас пока нет транзакций.")
                return
            
            # Формируем текст статистики
            message_text = self.format_statistics_message(stats)
            
            # Отправляем сообщение со статистикой
            await message.answer(
                message_text, 
                reply_markup=self.keyboard_factory.get_statistics_keyboard()
            )
        
        except Exception as e:
            logger.error(f"Ошибка при получении статистики: {e}")
            await message.answer("Не удалось получить статистику. Попробуйте позже.")

    def format_statistics_message(self, stats):
        """
        Форматирует статистику в читаемое сообщение с процентами
        """
        message = f"Статистика за последние 30 дней:\n\n"
        message += f"💰 Общий доход: {stats.total_income:.0f} руб.\n\n"
        message += f"💸 Общий расход: {stats.total_expense:.0f} руб.\n\n"
        message += f"💵 Баланс: {stats.balance:.0f} руб.\n\n"
        
        message += "📈 Доходы по категориям:\n\n"
        for item in stats.income_details:
            message += f"- {item['category']}: {item['amount']:.0f} руб. ({item['percentage']}%)\n\n"
        
        message += "\n📉 Расходы по категориям:\n\n"
        for item in stats.expense_details:
            message += f"- {item['category']}: {item['amount']:.0f} руб. ({item['percentage']}%)\n\n"
        
        return message

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

    async def process_show_chart(self, callback: CallbackQuery):
        """
        Обработчик для отображения графика статистики
        """
        try:
            user_id = callback.from_user.id
            logger.info(f"Запрос графика статистики для пользователя {user_id}")
            
            # Получаем статистику
            stats = await self.db.get_statistics(user_id)
            
            # Если статистики нет, отправляем сообщение
            if not stats or not stats.transactions:
                await callback.message.answer(
                    "📊 У вас пока нет транзакций для построения статистики.\n\n"
                    "Добавьте первую транзакцию, чтобы начать отслеживание финансов!",
                    reply_markup=self.keyboard_factory.get_statistics_keyboard()
                )
                await callback.answer()
                return
            
            # Генерируем график с помощью graph_image
            chart_bytes = await self.db.graph_image(user_id)
            
            if chart_bytes:
                logger.info(f"Отправка графика для пользователя {user_id}. Размер: {len(chart_bytes)} байт")
                
                # Отправляем график как изображение
                await callback.message.answer_photo(
                    photo=chart_bytes, 
                    caption='📊 Статистика доходов и расходов',
                    reply_markup=self.keyboard_factory.get_statistics_keyboard()
                )
            else:
                # Если не удалось сгенерировать график, отправляем текстовую статистику
                message_text = self.format_statistics_message(stats)
                await callback.message.answer(
                    message_text,
                    reply_markup=self.keyboard_factory.get_statistics_keyboard()
                )
            
            await callback.answer()
        
        except Exception as e:
            logger.error(f"Критическая ошибка при отображении графика для пользователя {user_id}: {e}", exc_info=True)
            await callback.message.answer(
                'Произошла ошибка при генерации графика.',
                reply_markup=self.keyboard_factory.get_statistics_keyboard()
            )
            await callback.answer()

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

    # Обработчик для графика статистики
    router.callback_query.register(
        handler.process_show_chart,
        F.data == "show_chart"
    )

# Создаем глобальный экземпляр обработчика
finance_handler = FinanceHandler()
