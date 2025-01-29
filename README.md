# 💰 Vision Budget Bot

## 🌟 Описание проекта
Vision Budget Bot - это персональный финансовый помощник в Telegram, который поможет вам легко и удобно вести учет доходов и расходов.

## ✨ Функциональность
- 📥 Добавление доходов по категориям
- 📤 Добавление расходов по категориям
- 📊 Подробная статистика за последние 30 дней
- 🔍 Анализ финансовых потоков
- 💾 Кэширование и оптимизация работы с данными

## 🚀 Технологии
- Python 3.9+
- Aiogram (Telegram Bot Framework)
- SQLite
- Poetry (управление зависимостями)
- Pytest (тестирование)

## 🔧 Установка и настройка

### Prerequisites
- Python 3.9+
- Poetry

### Шаги установки
1. Клонируйте репозиторий
```bash
git clone https://github.com/AleksVision/finance_bot.git
cd finance_bot
```

2. Установите зависимости
```bash
poetry install
```

3. Создайте `.env` файл с токеном Telegram бота
```
BOT_TOKEN=ваш_токен_telegram_бота
```

4. Запустите бота
```bash
poetry run python -m bot.main
```

## 🤖 Использование
1. Найдите бота в Telegram: `@VisionBudgetBot`
2. Нажмите `/start`
3. Используйте кнопки для добавления доходов и расходов
4. Смотрите статистику в разделе "📊 Статистика"

## 🧪 Тестирование
```bash
poetry run pytest
```

## 🤝 Contributing
1. Форкните проект
2. Создайте свою ветку (`git checkout -b feature/AmazingFeature`)
3. Закоммитьте изменения (`git commit -m 'Add some AmazingFeature'`)
4. Запушьте в ветку (`git push origin feature/AmazingFeature`)
5. Откройте Pull Request

## 📝 Лицензия
Распространяется под MIT License.

## 👥 Контакты
Александр - [GitHub Profile](https://github.com/AleksVision)

## 🙏 Поддержка
Если вам понравился проект, поставьте ⭐ звезду на GitHub!
