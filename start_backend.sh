#!/bin/bash

# Скрипт для запуска backend приложения
echo "🚀 Запуск Backend приложения..."

# Проверяем наличие виртуального окружения
if [ -d "/Users/olgasnissarenko/venv" ]; then
    echo "📦 Активация виртуального окружения..."
    source /Users/olgasnissarenko/venv/bin/activate
else
    echo "❌ Виртуальное окружение не найдено в /Users/olgasnissarenko/venv"
    echo "💡 Создайте виртуальное окружение: python3 -m venv /Users/olgasnissarenko/venv"
    exit 1
fi

# Проверяем наличие Streamlit
if ! command -v streamlit &> /dev/null; then
    echo "📦 Установка Streamlit..."
    pip install streamlit==1.37.1 SQLAlchemy==2.0.32 psycopg2-binary==2.9.9
fi

# Переходим в корневую директорию проекта
cd /Users/olgasnissarenko/hackaton_2

echo "🌐 Запуск Streamlit приложения..."
echo "📍 URL: http://localhost:8501"
echo "⏹️  Для остановки нажмите Ctrl+C"
echo "----------------------------------------"

# Запускаем Streamlit
streamlit run backend/app/presentation/streamlit_app.py --server.port 8501 --server.address localhost
