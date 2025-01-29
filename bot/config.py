from dotenv import load_dotenv
import os
from pathlib import Path

# Загружаем переменные окружения из .env файла
load_dotenv()

# BOT_TOKEN из .env
BOT_TOKEN = os.getenv('BOT_TOKEN')

# Указываем относительный путь к базе данных
BASE_DIR = Path(__file__).resolve().parent
DATABASE_PATH = BASE_DIR / 'finance_bot.db'

# Проверка наличия токена
if BOT_TOKEN is None:
    raise ValueError("Не найден BOT_TOKEN в переменных окружения")

# Для отладки (если необходимо)
if __name__ == "__main__":
    print(f"Текущая директория: {BASE_DIR}")
    print(f"Загруженный токен: {BOT_TOKEN[:5]}...")  # Печатаем только первые 5 символов токена для безопасности
    print(f"Путь к базе данных: {DATABASE_PATH}")

