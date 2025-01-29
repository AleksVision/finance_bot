from aiogram import Router, types, F
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InputFile
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

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

class SettingsForm(StatesGroup):
    """Состояния для работы с настройками"""
    settings_menu = State()
    choose_currency = State()
    set_expense_limit = State()
    set_notification_frequency = State()
    set_report_period = State()
    manage_categories = State()
    add_category = State()
    notification_settings = State()
    report_period_settings = State()

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
<<<<<<< HEAD
        """
        Создание клавиатуры для статистики с новой кнопкой графика
        """
        builder = InlineKeyboardBuilder()
        builder.button(text="📊 За 30 дней", callback_data="stats_30_days")
        builder.button(text="📈 График", callback_data="show_chart")
        builder.button(text="🔙 Назад", callback_data="main_menu")
=======
        builder = InlineKeyboardBuilder()
        builder.button(text="📊 Показать статистику", callback_data="show_statistics")
        builder.button(text="📈 Показать график", callback_data="show_chart")
        builder.adjust(2)
        return builder.as_markup()

    @staticmethod
    def get_settings_keyboard():
        """Клавиатура для настроек"""
        builder = InlineKeyboardBuilder()
        builder.button(text="💱 Валюта", callback_data="settings_currency")
        builder.button(text="💸 Лимит расходов", callback_data="settings_expense_limit")
        builder.button(text="🔔 Уведомления", callback_data="settings_notifications")
        builder.button(text="📊 Период отчета", callback_data="settings_report_period")
        builder.button(text="📋 Категории", callback_data="settings_categories")
        builder.button(text="📈 Просмотр отчетов", callback_data="show_report_periods")
        builder.button(text="🏠 Главное меню", callback_data="main_menu")
        builder.adjust(2, 2)
        return builder.as_markup()

    @staticmethod
    def get_currency_keyboard():
        """Клавиатура выбора валюты"""
        builder = InlineKeyboardBuilder()
        builder.button(text="🇷🇺 Рубль (RUB)", callback_data="currency_RUB")
        builder.button(text="🇺🇸 Доллар (USD)", callback_data="currency_USD")
        builder.button(text="🇪🇺 Евро (EUR)", callback_data="currency_EUR")
        builder.button(text="🔙 Назад", callback_data="settings_menu")
        builder.adjust(2, 1)
        return builder.as_markup()

    @staticmethod
    def get_categories_keyboard(category_type):
        """Клавиатура для управления категориями"""
        builder = InlineKeyboardBuilder()
        
        # Добавляем кнопки для существующих категорий
        default_categories = (
            Categories.INCOME.keys() if category_type == 'income' 
            else Categories.EXPENSE.keys()
        )
        
        for category in default_categories:
            builder.button(
                text=f"❌ {Categories.INCOME.get(category, Categories.EXPENSE.get(category, category))}", 
                callback_data=f"remove_category_{category_type}_{category}"
            )
        
        # Кнопка добавления новой категории
        builder.button(text="➕ Добавить категорию", callback_data=f"add_category_{category_type}")
        builder.button(text="🔙 Назад", callback_data="settings_menu")
        
        builder.adjust(2)
        return builder.as_markup()

    @staticmethod
    def get_notifications_keyboard():
        """Клавиатура для настроек уведомлений"""
        builder = InlineKeyboardBuilder()
        
        notifications = [
            ('expense_limit', 'Лимит расходов'),
            ('monthly_report', 'Месячный отчет'),
            ('weekly_summary', 'Недельная сводка')
        ]
        
        for notification_type, label in notifications:
            builder.button(
                text=f"🔔 {label}", 
                callback_data=f"notification_settings_{notification_type}"
            )
        
        builder.button(text="🔙 Назад", callback_data="settings_menu")
        builder.adjust(1)
        return builder.as_markup()

    @staticmethod
    def get_notification_type_keyboard(notification_type):
        """Клавиатура для настроек конкретного типа уведомлений"""
        builder = InlineKeyboardBuilder()
        
        frequencies = {
            'expense_limit': ['daily', 'weekly', 'monthly'],
            'monthly_report': ['monthly'],
            'weekly_summary': ['weekly']
        }
        
        # Кнопка включения/выключения
        builder.button(
            text="✅ Включить", 
            callback_data=f"notification_toggle_{notification_type}"
        )
        
        # Кнопки частоты (если применимо)
        for freq in frequencies.get(notification_type, []):
            builder.button(
                text=f"🕒 {freq.capitalize()}", 
                callback_data=f"notification_frequency_{notification_type}_{freq}"
            )
        
        builder.button(text="🔙 Назад", callback_data="notification_settings")
        builder.adjust(1)
        return builder.as_markup()

    @staticmethod
    def get_report_period_keyboard():
        """Клавиатура для выбора периода отчетности"""
        builder = InlineKeyboardBuilder()
        
        periods = [
            ('monthly', 'Ежемесячный'),
            ('quarterly', 'Ежеквартальный')
        ]
        
        for period_type, label in periods:
            builder.button(
                text=f"📅 {label}", 
                callback_data=f"report_period_{period_type}"
            )
        
        builder.button(text="🔙 Назад", callback_data="settings_menu")
        builder.adjust(1)
        return builder.as_markup()

    @staticmethod
    def get_report_period_start_keyboard(period_type):
        """Клавиатура для выбора дня начала периода"""
        builder = InlineKeyboardBuilder()
        
        # Создаем кнопки для дней начала периода
        for day in range(1, 29):
            builder.button(
                text=f"📆 {day} число", 
                callback_data=f"report_period_start_{period_type}_{day}"
            )
        
        builder.button(text="🔙 Назад", callback_data="settings_report_period")
        builder.adjust(7)
        return builder.as_markup()

    @staticmethod
    def get_report_periods_keyboard(periods):
        """Клавиатура для выбора периода отчета"""
        builder = InlineKeyboardBuilder()
        
        for i, period in enumerate(periods, 1):
            start = period['start'].strftime('%d.%m.%Y')
            end = period['end'].strftime('%d.%m.%Y')
            builder.button(
                text=f"📊 {start} - {end}", 
                callback_data=f"generate_report_{i-1}"
            )
        
        builder.button(text="🔙 Назад", callback_data="settings_menu")
        builder.adjust(1)
        return builder.as_markup()

    @staticmethod
    def get_report_actions_keyboard(period_index):
        """Клавиатура действий для отчета"""
        builder = InlineKeyboardBuilder()
        
        builder.button(text="📊 Диаграмма", callback_data=f"report_chart_{period_index}")
        builder.button(text="💾 Сохранить PDF", callback_data=f"report_pdf_{period_index}")
        builder.button(text="🔙 Назад", callback_data="settings_report_periods")
        
>>>>>>> f0703d875c19494d9b6433598ebe239a613bb162
        builder.adjust(2, 1)
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
<<<<<<< HEAD
            )
            
            # Логируем запрос статистики
            logger.info(
                f"Статистика запрошена: "
                f"user_id={message.from_user.id}, "
                f"total_income={stats.total_income}, "
                f"total_expense={stats.total_expense}"
=======
>>>>>>> f0703d875c19494d9b6433598ebe239a613bb162
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

    async def show_settings(self, message: types.Message):
        """Показать меню настроек"""
        try:
            await message.answer(
                "⚙️ Настройки бота\n\n"
                "Выберите, что хотите настроить:",
                reply_markup=self.keyboard_factory.get_settings_keyboard()
            )
        except Exception as e:
            logger.error(f"Ошибка при открытии настроек: {e}")
            await message.answer("❌ Не удалось открыть настройки. Попробуйте позже.")

    async def process_currency_settings(self, callback: CallbackQuery, state: FSMContext):
        """Обработка выбора валюты"""
        try:
            currency = callback.data.split('_')[1]
            user_id = callback.from_user.id
            
            await self.db.update_user_settings(user_id, default_currency=currency)
            
            await callback.message.edit_text(
                f"💱 Валюта по умолчанию установлена: {currency}\n\n"
                "Выберите следующее действие:",
                reply_markup=self.keyboard_factory.get_settings_keyboard()
            )
            await callback.answer(f"Валюта изменена на {currency}")
        except Exception as e:
            logger.error(f"Ошибка при смене валюты: {e}")
            await callback.answer("❌ Не удалось изменить валюту")

    async def process_expense_limit(self, callback: CallbackQuery, state: FSMContext):
        """Начало установки лимита расходов"""
        try:
            await state.set_state(SettingsForm.set_expense_limit)
            await callback.message.edit_text(
                "💸 Введите месячный лимит расходов (число):\n\n"
                "Например: 50000"
            )
            await callback.answer()
        except Exception as e:
            logger.error(f"Ошибка при настройке лимита: {e}")
            await callback.answer("❌ Не удалось начать настройку лимита")

    async def save_expense_limit(self, message: types.Message, state: FSMContext):
        """Сохранение лимита расходов"""
        try:
            limit = float(message.text)
            user_id = message.from_user.id
            
            await self.db.update_user_settings(user_id, monthly_expense_limit=limit)
            
            await message.answer(
                f"✅ Месячный лимит расходов установлен: {limit} руб.\n\n"
                "Выберите следующее действие:",
                reply_markup=self.keyboard_factory.get_settings_keyboard()
            )
            await state.clear()
        except ValueError:
            await message.answer("❌ Введите корректное число")
        except Exception as e:
            logger.error(f"Ошибка при сохранении лимита: {e}")
            await message.answer("❌ Не удалось сохранить лимит")

    async def manage_categories(self, callback: CallbackQuery, state: FSMContext):
        """Управление категориями"""
        try:
            category_type = callback.data.split('_')[2]
            
            await state.update_data(category_type=category_type)
            
            await callback.message.edit_text(
                f"📋 Управление категориями ({category_type})\n\n"
                "Выберите категорию для удаления или добавьте новую:",
                reply_markup=self.keyboard_factory.get_categories_keyboard(category_type)
            )
            await callback.answer()
        except Exception as e:
            logger.error(f"Ошибка при управлении категориями: {e}")
            await callback.answer("❌ Не удалось открыть управление категориями")

    async def start_add_category(self, callback: CallbackQuery, state: FSMContext):
        """Начало добавления новой категории"""
        try:
            category_type = callback.data.split('_')[2]
            
            await state.set_state(SettingsForm.add_category)
            await state.update_data(category_type=category_type)
            
            await callback.message.edit_text(
                f"➕ Добавление новой категории ({category_type})\n\n"
                "Введите название категории:"
            )
            await callback.answer()
        except Exception as e:
            logger.error(f"Ошибка при начале добавления категории: {e}")
            await callback.answer("❌ Не удалось начать добавление категории")

    async def save_new_category(self, message: types.Message, state: FSMContext):
        """Сохранение новой категории"""
        try:
            # Получаем данные о типе категории из состояния
            state_data = await state.get_data()
            category_type = state_data.get('category_type')
            
            # Проверяем корректность названия
            category_name = message.text.lower().replace(' ', '_')
            
            # Сохраняем категорию
            user_id = message.from_user.id
            await self.db.add_user_category(user_id, category_name, category_type)
            
            await message.answer(
                f"✅ Категория '{category_name}' добавлена\n\n"
                "Выберите следующее действие:",
                reply_markup=self.keyboard_factory.get_categories_keyboard(category_type)
            )
            await state.clear()
        except Exception as e:
            logger.error(f"Ошибка при сохранении категории: {e}")
            await message.answer("❌ Не удалось сохранить категорию")

    async def remove_category(self, callback: CallbackQuery):
        """Удаление категории"""
        try:
            # Парсим callback_data
            _, category_type, category = callback.data.split('_')
            user_id = callback.from_user.id
            
            # Пытаемся удалить категорию
            result = await self.db.remove_user_category(user_id, category, category_type)
            
            if result:
                await callback.message.edit_text(
                    f"✅ Категория '{category}' удалена\n\n"
                    "Выберите следующее действие:",
                    reply_markup=self.keyboard_factory.get_categories_keyboard(category_type)
                )
                await callback.answer(f"Категория {category} удалена")
            else:
                await callback.message.edit_text(
                    f"❌ Не удалось удалить категорию '{category}'\n\n"
                    "Возможно, у вас есть транзакции с этой категорией.\n"
                    "Сначала удалите или измените связанные транзакции.",
                    reply_markup=self.keyboard_factory.get_categories_keyboard(category_type)
                )
                await callback.answer("Не удалось удалить категорию")
        except Exception as e:
            logger.error(f"Ошибка при удалении категории: {e}")
            await callback.answer("❌ Не удалось удалить категорию")

    async def show_notifications_menu(self, callback: CallbackQuery):
        """Показать меню настроек уведомлений"""
        try:
            await callback.message.edit_text(
                "🔔 Настройки уведомлений\n\n"
                "Выберите тип уведомлений:",
                reply_markup=self.keyboard_factory.get_notifications_keyboard()
            )
            await callback.answer()
        except Exception as e:
            logger.error(f"Ошибка при показе меню уведомлений: {e}")
            await callback.answer("❌ Не удалось открыть настройки уведомлений")

    async def show_notification_type_settings(self, callback: CallbackQuery, state: FSMContext):
        """Показать настройки конкретного типа уведомлений"""
        try:
            notification_type = callback.data.split('_')[-1]
            
            # Получаем текущие настройки
            user_id = callback.from_user.id
            self.current_notification_settings = await self.db.get_notification_settings(
                user_id, 
                notification_type
            )
            
            await state.update_data(notification_type=notification_type)
            
            await callback.message.edit_text(
                f"🔔 Настройки уведомлений: {notification_type}\n\n"
                f"Текущий статус: {'Включены' if self.current_notification_settings.get('status') == 'enabled' else 'Выключены'}\n"
                f"Частота: {self.current_notification_settings.get('frequency', 'Не установлена')}",
                reply_markup=self.keyboard_factory.get_notification_type_keyboard(notification_type)
            )
            await callback.answer()
        except Exception as e:
            logger.error(f"Ошибка при показе настроек уведомлений: {e}")
            await callback.answer("❌ Не удалось открыть настройки уведомлений")

    async def toggle_notification(self, callback: CallbackQuery, state: FSMContext):
        """Включение/выключение уведомлений"""
        try:
            state_data = await state.get_data()
            notification_type = state_data.get('notification_type')
            user_id = callback.from_user.id
            
            # Переключаем статус
            current_status = self.current_notification_settings.get('status', 'disabled')
            new_status = 'disabled' if current_status == 'enabled' else 'enabled'
            
            # Обновляем в базе
            await self.db.update_notification_settings(
                user_id, 
                notification_type, 
                new_status == 'enabled'
            )
            
            # Обновляем текущие настройки
            self.current_notification_settings['status'] = new_status
            
            await callback.message.edit_text(
                f"🔔 Настройки уведомлений: {notification_type}\n\n"
                f"Статус: {'Включены' if new_status == 'enabled' else 'Выключены'}",
                reply_markup=self.keyboard_factory.get_notification_type_keyboard(notification_type)
            )
            await callback.answer(f"Уведомления {'включены' if new_status == 'enabled' else 'выключены'}")
        except Exception as e:
            logger.error(f"Ошибка при переключении уведомлений: {e}")
            await callback.answer("❌ Не удалось изменить настройки уведомлений")

    async def set_notification_frequency(self, callback: CallbackQuery, state: FSMContext):
        """Установка частоты уведомлений"""
        try:
            _, _, notification_type, frequency = callback.data.split('_')
            user_id = callback.from_user.id
            
            # Обновляем в базе
            await self.db.update_notification_settings(
                user_id, 
                notification_type, 
                True,  # включаем уведомления
                frequency
            )
            
            # Обновляем текущие настройки
            self.current_notification_settings = {
                'status': 'enabled',
                'frequency': frequency
            }
            
            await callback.message.edit_text(
                f"🔔 Настройки уведомлений: {notification_type}\n\n"
                f"Статус: Включены\n"
                f"Частота: {frequency}",
                reply_markup=self.keyboard_factory.get_notification_type_keyboard(notification_type)
            )
            await callback.answer(f"Частота уведомлений установлена: {frequency}")
        except Exception as e:
            logger.error(f"Ошибка при установке частоты уведомлений: {e}")
            await callback.answer("❌ Не удалось установить частоту уведомлений")

    async def show_report_period_menu(self, callback: CallbackQuery):
        """Показать меню выбора периода отчетности"""
        try:
            # Получаем текущие настройки периода
            user_id = callback.from_user.id
            current_settings = await self.db.get_report_period(user_id)
            
            await callback.message.edit_text(
                "📅 Настройка периода отчетности\n\n"
                f"Текущий период: {current_settings['period_type'].capitalize()}\n"
                f"Начало периода: {current_settings['start_day']} число",
                reply_markup=self.keyboard_factory.get_report_period_keyboard()
            )
            await callback.answer()
        except Exception as e:
            logger.error(f"Ошибка при показе меню периода отчетности: {e}")
            await callback.answer("❌ Не удалось открыть настройки периода")

    async def select_report_period_type(self, callback: CallbackQuery, state: FSMContext):
        """Выбор типа периода отчетности"""
        try:
            period_type = callback.data.split('_')[-1]
            
            await state.update_data(report_period_type=period_type)
            
            await callback.message.edit_text(
                f"📅 Период: {period_type.capitalize()}\n\n"
                "Выберите день начала периода:",
                reply_markup=self.keyboard_factory.get_report_period_start_keyboard(period_type)
            )
            await callback.answer()
        except Exception as e:
            logger.error(f"Ошибка при выборе типа периода: {e}")
            await callback.answer("❌ Не удалось выбрать период")

    async def save_report_period(self, callback: CallbackQuery, state: FSMContext):
        """Сохранение настроек периода отчетности"""
        try:
            state_data = await state.get_data()
            period_type = state_data.get('report_period_type')
            start_day = int(callback.data.split('_')[-1])
            user_id = callback.from_user.id
            
            # Сохраняем настройки
            await self.db.update_report_period(user_id, start_day, period_type)
            
            await callback.message.edit_text(
                "✅ Период отчетности обновлен\n\n"
                f"Тип: {period_type.capitalize()}\n"
                f"Начало периода: {start_day} число",
                reply_markup=self.keyboard_factory.get_settings_keyboard()
            )
            await callback.answer("Период отчетности сохранен")
            await state.clear()
        except Exception as e:
            logger.error(f"Ошибка при сохранении периода отчетности: {e}")
            await callback.answer("❌ Не удалось сохранить период")

    async def show_report_periods(self, callback: CallbackQuery):
        """Показать доступные периоды для отчетов"""
        try:
            user_id = callback.from_user.id
            
            # Получаем доступные периоды
            periods = await self.db.get_financial_report_periods(user_id)
            
            if not periods:
                await callback.message.edit_text(
                    "❌ У вас пока нет транзакций для создания отчета",
                    reply_markup=self.keyboard_factory.settings_menu()
                )
                return
            
            await callback.message.edit_text(
                "📊 Выберите период для отчета:",
                reply_markup=self.keyboard_factory.get_report_periods_keyboard(periods)
            )
            await callback.answer()
        except Exception as e:
            logger.error(f"Ошибка при показе периодов отчета: {e}")
            await callback.answer("❌ Не удалось получить периоды отчета")

    async def generate_financial_report(self, callback: CallbackQuery):
        """Генерация финансового отчета"""
        try:
            # Получаем индекс периода из callback_data
            period_index = int(callback.data.split('_')[-1])
            user_id = callback.from_user.id
            
            # Получаем доступные периоды
            periods = await self.db.get_financial_report_periods(user_id)
            selected_period = periods[period_index]
            
            # Генерируем отчет
            report = await self.db.generate_financial_report(
                user_id, 
                selected_period['start'].strftime('%Y-%m-%d'), 
                selected_period['end'].strftime('%Y-%m-%d')
            )
            
            # Форматируем отчет
            report_text = self.format_financial_report(report)
            
            await callback.message.edit_text(
                report_text,
                reply_markup=self.keyboard_factory.get_report_actions_keyboard(period_index)
            )
            await callback.answer()
        except Exception as e:
            logger.error(f"Ошибка при генерации финансового отчета: {e}")
            await callback.answer("❌ Не удалось сгенерировать отчет")

    def format_financial_report(self, report):
        """Форматирование финансового отчета"""
        # Заголовок
        report_text = f"📊 Финансовый отчет\n"
        report_text += f"Период: {report['period_start'].strftime('%d.%m.%Y')} - {report['period_end'].strftime('%d.%m.%Y')}\n"
        report_text += f"Валюта: {report['currency']}\n\n"
        
        # Общая статистика
        report_text += "💰 Общая статистика:\n"
        report_text += f"Доход: {report['total_income']:.2f}\n"
        report_text += f"Расход: {report['total_expense']:.2f}\n"
        report_text += f"Баланс: {report['balance']:.2f}\n\n"
        
        # Лимит расходов
        report_text += "🚨 Лимит расходов:\n"
        report_text += f"Установленный лимит: {report['expense_limit']:.2f}\n"
        status_map = {
            'exceeded': "❌ Превышен",
            'warning': "⚠️ Приближается к лимиту",
            'normal': "✅ В норме"
        }
        report_text += f"Статус: {status_map[report['expense_limit_status']]}\n\n"
        
        # Доходы по категориям
        report_text += "📈 Доходы по категориям:\n"
        for category in report['income_categories']:
            report_text += f"• {category['name']}: {category['total_amount']:.2f} ({category['transaction_count']} транзакций)\n"
        
        # Расходы по категориям
        report_text += "\n📉 Расходы по категориям:\n"
        for category in report['expense_categories']:
            report_text += f"• {category['name']}: {category['total_amount']:.2f} (ср. {category['avg_amount']:.2f}, {category['transaction_count']} транзакций)\n"
        
        return report_text

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

    # Обработчики настроек
    router.message.register(
        handler.show_settings, 
        F.text == "⚙️ Настройки"
    )
    router.callback_query.register(
        handler.process_currency_settings, 
        F.data.startswith("currency_")
    )
    router.callback_query.register(
        handler.process_expense_limit, 
        F.data == "settings_expense_limit"
    )
    router.message.register(
        handler.save_expense_limit, 
        SettingsForm.set_expense_limit
    )
    router.callback_query.register(
        handler.show_notifications_menu, 
        F.data == "settings_notifications"
    )
    router.callback_query.register(
        handler.show_notification_type_settings, 
        F.data.startswith("notification_settings_")
    )
    router.callback_query.register(
        handler.toggle_notification, 
        F.data.startswith("notification_toggle_")
    )
    router.callback_query.register(
        handler.set_notification_frequency, 
        F.data.startswith("notification_frequency_")
    )
    router.callback_query.register(
        handler.show_report_period_menu, 
        F.data == "settings_report_period"
    )
    router.callback_query.register(
        handler.select_report_period_type, 
        F.data.startswith("report_period_")
    )
    router.callback_query.register(
        handler.save_report_period, 
        F.data.startswith("report_period_start_")
    )

    # Обработчики категорий
    router.callback_query.register(
        handler.manage_categories, 
        F.data.startswith("settings_categories_")
    )
    router.callback_query.register(
        handler.start_add_category, 
        F.data.startswith("add_category_")
    )
    router.message.register(
        handler.save_new_category, 
        SettingsForm.add_category
    )
    router.callback_query.register(
        handler.remove_category, 
        F.data.startswith("remove_category_")
    )

    # Обработчики отчетов
    router.callback_query.register(
        handler.show_report_periods, 
        F.data == "show_report_periods"
    )
    router.callback_query.register(
        handler.generate_financial_report, 
        F.data.startswith("generate_report_")
    )

# Создаем глобальный экземпляр обработчика
finance_handler = FinanceHandler()
