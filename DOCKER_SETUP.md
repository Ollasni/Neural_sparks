# 🐳 Docker Setup для BI-GPT Agent

Этот документ содержит инструкции по запуску BI-GPT Agent с использованием Docker.

## 📋 Требования

- Docker (версия 20.10+)
- Docker Compose (версия 2.0+)
- Минимум 4GB RAM
- Минимум 2GB свободного места на диске

## 🚀 Быстрый запуск

### 1. Клонирование и настройка

```bash
# Клонируйте репозиторий (если еще не сделано)
git clone <your-repo-url>
cd hackaton_2

# Сделайте скрипты исполняемыми
chmod +x *.sh
```

### 2. Настройка переменных окружения

Создайте файл `.env` в корне проекта:

```bash
# OpenAI API Key (обязательно!)
OPENAI_API_KEY=your_actual_openai_api_key_here

# Database settings
POSTGRES_DB=bi_gpt_db
POSTGRES_USER=bi_gpt_user
POSTGRES_PASSWORD=bi_gpt_password

# Streamlit settings
STREAMLIT_SERVER_PORT=8501
STREAMLIT_SERVER_ADDRESS=0.0.0.0
```

### 3. Запуск приложения

#### Вариант A: Полный Docker (рекомендуется)

```bash
# Запуск всех сервисов
./docker-start.sh
```

#### Вариант B: Только база данных + локальное приложение

```bash
# Запуск только PostgreSQL и Redis
docker-compose up -d postgres redis

# Запуск приложения локально
./quick-start.sh
```

#### Вариант C: Режим разработки

```bash
# Запуск с монтированием кода для live reload
./docker-dev.sh
```

## 🛠️ Управление сервисами

### Остановка

```bash
# Остановка всех контейнеров
./docker-stop.sh

# Или вручную
docker-compose down
```

### Перезапуск

```bash
# Перезапуск с пересборкой
docker-compose up --build

# Перезапуск только приложения
docker-compose restart app
```

### Просмотр логов

```bash
# Логи всех сервисов
docker-compose logs

# Логи конкретного сервиса
docker-compose logs app
docker-compose logs postgres
```

## 🌐 Доступ к приложению

После запуска приложение будет доступно по адресам:

- **Streamlit UI**: http://localhost:8501
- **PostgreSQL**: localhost:5432
- **Redis**: localhost:6379

## 📁 Структура Docker

```
hackaton_2/
├── Dockerfile              # Основной Docker образ
├── docker-compose.yml      # Конфигурация сервисов
├── .dockerignore          # Исключения для Docker
├── docker-start.sh        # Скрипт запуска
├── docker-stop.sh         # Скрипт остановки
├── docker-dev.sh          # Скрипт для разработки
└── quick-start.sh         # Локальный запуск
```

## 🔧 Конфигурация сервисов

### PostgreSQL
- **Порт**: 5432
- **База данных**: bi_gpt_db
- **Пользователь**: bi_gpt_user
- **Пароль**: bi_gpt_password

### Redis
- **Порт**: 6379
- **Использование**: Кэширование (опционально)

### Приложение
- **Порт Streamlit**: 8501
- **Порт FastAPI**: 8000 (если используется)

## 🐛 Решение проблем

### Проблема: Контейнер не запускается

```bash
# Проверьте логи
docker-compose logs app

# Пересоберите образ
docker-compose build --no-cache
```

### Проблема: База данных недоступна

```bash
# Проверьте статус PostgreSQL
docker-compose ps postgres

# Перезапустите базу данных
docker-compose restart postgres
```

### Проблема: Недостаточно памяти

```bash
# Очистите неиспользуемые образы
docker system prune -a

# Ограничьте использование ресурсов в docker-compose.yml
```

## 📊 Мониторинг

### Статус контейнеров

```bash
# Список запущенных контейнеров
docker-compose ps

# Использование ресурсов
docker stats
```

### Подключение к базе данных

```bash
# Подключение к PostgreSQL
docker-compose exec postgres psql -U bi_gpt_user -d bi_gpt_db
```

## 🔄 Обновление

```bash
# Остановка сервисов
./docker-stop.sh

# Обновление кода
git pull

# Перезапуск с обновлениями
./docker-start.sh
```

## 📝 Дополнительные команды

### Очистка данных

```bash
# Удаление всех данных (ОСТОРОЖНО!)
docker-compose down -v
docker system prune -a
```

### Резервное копирование

```bash
# Создание бэкапа базы данных
docker-compose exec postgres pg_dump -U bi_gpt_user bi_gpt_db > backup.sql

# Восстановление из бэкапа
docker-compose exec -T postgres psql -U bi_gpt_user bi_gpt_db < backup.sql
```

## 🆘 Поддержка

Если у вас возникли проблемы:

1. Проверьте логи: `docker-compose logs`
2. Убедитесь, что все порты свободны
3. Проверьте наличие OpenAI API ключа в `.env`
4. Убедитесь, что Docker имеет достаточно ресурсов

---

**Примечание**: Для продакшена рекомендуется использовать внешнюю базу данных и настроить SSL сертификаты.
