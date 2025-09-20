#!/bin/bash

# Скрипт для быстрого запуска BI-GPT Agent с Docker

set -e

echo "🚀 BI-GPT Agent - Docker Quick Start"
echo "=================================="

# Проверяем наличие Docker
if ! command -v docker &> /dev/null; then
    echo "❌ Docker не установлен. Пожалуйста, установите Docker и попробуйте снова."
    exit 1
fi

# Проверяем наличие docker-compose
if ! command -v docker-compose &> /dev/null; then
    echo "❌ docker-compose не установлен. Пожалуйста, установите docker-compose и попробуйте снова."
    exit 1
fi

# Создаем .env файл если его нет
if [ ! -f .env ]; then
    echo "📝 Создаем файл .env..."
    cat > .env << EOF
# OpenAI API Key (замените на ваш ключ)
OPENAI_API_KEY=your_openai_api_key_here

# Database settings
POSTGRES_DB=bi_gpt_db
POSTGRES_USER=bi_gpt_user
POSTGRES_PASSWORD=bi_gpt_password

# Streamlit settings
STREAMLIT_SERVER_PORT=8501
STREAMLIT_SERVER_ADDRESS=0.0.0.0
EOF
    echo "⚠️  Пожалуйста, отредактируйте файл .env и добавьте ваш OpenAI API ключ"
fi

# Функция для остановки контейнеров
cleanup() {
    echo "🛑 Останавливаем контейнеры..."
    docker-compose down
    exit 0
}

# Обработчик сигналов для корректного завершения
trap cleanup SIGINT SIGTERM

echo "🔧 Собираем Docker образы..."
docker-compose build

echo "🚀 Запускаем сервисы..."
docker-compose up -d postgres redis

echo "⏳ Ждем готовности базы данных..."
sleep 10

echo "🌐 Запускаем основное приложение..."
docker-compose up app

# Если мы дошли сюда, значит приложение было остановлено
cleanup
