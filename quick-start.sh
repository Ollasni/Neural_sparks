#!/bin/bash

# Универсальный скрипт быстрого запуска BI-GPT Agent

set -e

echo "🚀 BI-GPT Agent - Quick Start"
echo "============================="

# Проверяем наличие Python
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3 не установлен. Пожалуйста, установите Python 3.8+ и попробуйте снова."
    exit 1
fi

# Проверяем наличие pip
if ! command -v pip3 &> /dev/null; then
    echo "❌ pip3 не установлен. Пожалуйста, установите pip и попробуйте снова."
    exit 1
fi

# Создаем виртуальное окружение если его нет
if [ ! -d "venv" ]; then
    echo "📦 Создаем виртуальное окружение..."
    python3 -m venv venv
fi

# Активируем виртуальное окружение
echo "🔧 Активируем виртуальное окружение..."
source venv/bin/activate

# Устанавливаем зависимости
echo "📥 Устанавливаем зависимости..."
pip install -r requirements.txt

# Создаем .env файл если его нет
if [ ! -f .env ]; then
    echo "📝 Создаем файл .env..."
    cat > .env << EOF
# OpenAI API Key (замените на ваш ключ)
OPENAI_API_KEY=your_openai_api_key_here

# Database settings (для локальной разработки)
DATABASE_URL=sqlite:///bi_demo.db
EOF
    echo "⚠️  Пожалуйста, отредактируйте файл .env и добавьте ваш OpenAI API ключ"
fi

echo "🌐 Запускаем приложение..."
echo "📱 Приложение будет доступно по адресу: http://localhost:8501"
echo "⏹️  Для остановки нажмите Ctrl+C"

# Запускаем приложение
python launch_app.py --model integrated --port 8501
