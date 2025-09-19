# Backend - Анализ схемы базы данных

Этот модуль предоставляет backend для анализа схемы PostgreSQL базы данных с использованием Clean Architecture.

## Структура проекта

```
backend/
├── app/
│   ├── domain/           # Доменные модели
│   │   ├── __init__.py
│   │   └── models.py     # Column, Table, SchemaOverview
│   ├── application/      # Use cases (бизнес-логика)
│   │   ├── __init__.py
│   │   └── use_cases.py  # GetSchemaOverviewUC
│   ├── infrastructure/   # Инфраструктурный слой
│   │   ├── __init__.py
│   │   ├── db/
│   │   │   ├── __init__.py
│   │   │   └── postgres.py  # Подключение к PostgreSQL
│   │   └── repositories/
│   │       ├── __init__.py
│   │       └── schema_repository.py  # Репозиторий схемы
│   ├── presentation/     # UI слой
│   │   ├── __init__.py
│   │   └── streamlit_app.py  # Streamlit приложение
│   └── config/           # Конфигурация
│       ├── __init__.py
│       └── settings.py   # Настройки UI
├── requirements.txt      # Зависимости Python
├── test_integration.py   # Тесты интеграции
├── run_app.py           # Скрипт запуска приложения
└── README.md            # Этот файл
```

## Архитектура

Проект следует принципам Clean Architecture:

- **Domain** - доменные модели (Column, Table, SchemaOverview)
- **Application** - use cases (бизнес-логика)
- **Infrastructure** - репозитории и подключение к БД
- **Presentation** - Streamlit UI

## Установка и запуск

### 1. Установка зависимостей

```bash
cd backend
pip install -r requirements.txt
```

### 2. Запуск приложения

```bash
python run_app.py
```

Или напрямую через Streamlit:

```bash
streamlit run app/presentation/streamlit_app.py
```

### 3. Тестирование

```bash
python test_integration.py
```

## Использование

1. Откройте браузер по адресу http://localhost:8501
2. В боковой панели введите параметры подключения к PostgreSQL:
   - Host (по умолчанию: localhost)
   - Port (по умолчанию: 5432)
   - Username (по умолчанию: postgres)
   - Password
   - Database Name (по умолчанию: postgres)
   - SSL Mode (опционально)
3. Нажмите "Подключиться"
4. Просматривайте схему базы данных в удобном интерфейсе

## Функциональность

- ✅ Подключение к PostgreSQL базе данных
- ✅ Получение списка таблиц и их колонок
- ✅ Отображение типов данных и ограничений
- ✅ Современный UI с использованием Streamlit
- ✅ Обработка ошибок подключения
- ✅ Адаптивный дизайн

## Технические детали

### Модели данных

- **Column**: колонка таблицы с типом, nullable, default значением
- **Table**: таблица с схемой, именем и списком колонок
- **SchemaOverview**: обзор всей схемы БД

### Репозиторий

`SchemaRepository` использует SQL-запросы к `information_schema` для получения метаданных PostgreSQL.

### Use Case

`GetSchemaOverviewUC` координирует получение данных через репозиторий и формирует доменную модель.

## Расширение функциональности

Приложение легко расширяется:

- Добавление фильтров по схемам
- Поиск по колонкам
- Экспорт в CSV/JSON
- Natural Language SQL запросы
- Визуализация связей между таблицами

## Требования

- Python 3.9+
- PostgreSQL (для подключения)
- Зависимости из requirements.txt

## Лицензия

Проект создан в рамках хакатона.
