#!/bin/bash

# Скрипт для разработки с Docker (с монтированием кода)

set -e

echo "🔧 BI-GPT Agent - Docker Development Mode"
echo "========================================"

# Проверяем наличие Docker
if ! command -v docker &> /dev/null; then
    echo "❌ Docker не установлен. Пожалуйста, установите Docker и попробуйте снова."
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

echo "🚀 Запускаем в режиме разработки..."
echo "📁 Код будет монтироваться в контейнер для live reload"

# Запускаем только базу данных и Redis
docker-compose up -d postgres redis

echo "⏳ Ждем готовности базы данных..."
sleep 10

echo "🔧 Запускаем приложение в режиме разработки..."
echo "🌐 Приложение будет доступно по адресу: http://localhost:8501"
echo "⏹️  Для остановки нажмите Ctrl+C"

# Запускаем приложение с монтированием кода
docker run --rm -it \
    --name bi_gpt_dev \
    --network hackaton_2_bi_gpt_network \
    -p 8501:8501 \
    -v "$(pwd)":/app \
    -e DATABASE_URL=postgresql://bi_gpt_user:bi_gpt_password@postgres:5432/bi_gpt_db \
    -e POSTGRES_HOST=postgres \
    -e POSTGRES_PORT=5432 \
    -e POSTGRES_DB=bi_gpt_db \
    -e POSTGRES_USER=bi_gpt_user \
    -e POSTGRES_PASSWORD=bi_gpt_password \
    -e OPENAI_API_KEY="${OPENAI_API_KEY:-your_openai_api_key_here}" \
    -e PYTHONPATH=/app \
    -e PYTHONUNBUFFERED=1 \
    bi_gpt_agent_app \
    python launch_app.py --model integrated --port 8501
