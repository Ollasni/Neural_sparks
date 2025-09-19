# Настройка PostgreSQL для BI-GPT Agent

Система теперь полностью переключена на PostgreSQL для обеспечения совместимости с fine-tuned моделью.

## 🚀 Быстрый запуск

### Вариант 1: Docker (Рекомендуется)

```bash
# Запуск PostgreSQL в Docker
docker run --name bi-postgres \
  -e POSTGRES_PASSWORD=password \
  -e POSTGRES_DB=bi_demo \
  -p 5432:5432 \
  -d postgres:15

# Проверка работы
docker ps | grep bi-postgres
```

### Вариант 2: Локальная установка

#### macOS (Homebrew)
```bash
brew install postgresql
brew services start postgresql

# Создание базы данных
createdb bi_demo
```

#### Ubuntu/Debian
```bash
sudo apt update
sudo apt install postgresql postgresql-contrib

# Создание пользователя и базы данных
sudo -u postgres createuser --interactive
sudo -u postgres createdb bi_demo
```

## ⚙️ Конфигурация

### .env файл (создайте если нет)

```bash
# База данных PostgreSQL
DATABASE_URL=postgresql://postgres:password@localhost:5432/bi_demo

# Fine-tuned модель
USE_FINETUNED_MODEL=true
MODEL_PROVIDER=local

# Заглушки для API (не используются с fine-tuned)
LOCAL_API_KEY=dummy_key
LOCAL_BASE_URL=http://localhost:8000/v1
```

### Для production

```bash
# Пример production конфигурации
DATABASE_URL=postgresql://username:password@your-db-host:5432/bi_demo_prod
APP_ENVIRONMENT=production
LOG_LEVEL=INFO
```

## 🔧 Проверка подключения

```bash
# Тест подключения к базе данных
psql postgresql://postgres:password@localhost:5432/bi_demo -c "SELECT version();"
```

## 🎯 Преимущества PostgreSQL

1. **Совместимость с fine-tuned моделью**: BIRD-SQL датасет использует PostgreSQL синтаксис
2. **Продвинутые функции**: INTERVAL, advanced DATE функции
3. **Лучшая производительность**: для сложных аналитических запросов
4. **Стандартность**: PostgreSQL стандарт в enterprise

## 🔄 Fallback на SQLite

Если PostgreSQL недоступен, система автоматически переключится на SQLite:

```
⚠️  Ошибка подключения к PostgreSQL: could not connect...
🔄 Fallback: используем SQLite
✅ SQLite fallback база данных инициализирована
```

## 🚨 Устранение проблем

### Ошибка подключения
```bash
# Проверьте что PostgreSQL запущен
sudo service postgresql status

# Проверьте порт
netstat -an | grep 5432
```

### Ошибка аутентификации
```bash
# Сбросить пароль пользователя
sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'password';"
```

### Docker проблемы
```bash
# Остановить и пересоздать контейнер
docker stop bi-postgres
docker rm bi-postgres

# Запустить заново
docker run --name bi-postgres -e POSTGRES_PASSWORD=password -e POSTGRES_DB=bi_demo -p 5432:5432 -d postgres:15
```

## 📊 Тестирование

```bash
# Запуск с fine-tuned моделью
python launch_finetuned.py

# Должно появиться:
# ✅ PostgreSQL демо база данных инициализирована
# ✅ Используется fine-tuned модель Phi-3 + LoRA
```

Теперь система будет генерировать корректный PostgreSQL синтаксис! 🎉
