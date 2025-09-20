# Руководство по динамическому извлечению схемы БД

## Обзор

Добавлена функциональность динамического извлечения схемы базы данных в реальном времени, которая позволяет:

- ✅ Автоматически получать актуальные названия колонок из БД
- ✅ Работать с удаленными и локальными базами данных
- ✅ Поддерживать PostgreSQL, MySQL и другие БД через SQLAlchemy
- ✅ Кэшировать схему для повышения производительности
- ✅ Автоматически обновлять промпты модели с актуальной схемой

## Новые компоненты

### 1. `DynamicSchemaExtractor` (dynamic_schema_extractor.py)

Основной класс для извлечения схемы из БД:

```python
from dynamic_schema_extractor import create_dynamic_extractor

# Создание экстрактора
extractor = create_dynamic_extractor(
    connection_string="postgresql://user:password@localhost:5432/mydb",
    cache_ttl=300  # Время жизни кэша в секундах
)

# Получение схемы
schema = extractor.get_schema()
print(schema.to_prompt_format())
```

### 2. Обновленные генераторы SQL

Теперь `SQLGenerator` и `FineTunedSQLGenerator` поддерживают динамическую схему:

```python
from bi_gpt_agent import SQLGenerator

# С динамической схемой (по умолчанию)
generator = SQLGenerator(
    api_key="your_api_key",
    connection_string="postgresql://localhost:5432/mydb",
    use_dynamic_schema=True  # Включено по умолчанию
)

# Без динамической схемы (legacy режим)
generator = SQLGenerator(
    api_key="your_api_key",
    use_dynamic_schema=False
)
```

## Функции

### Поддерживаемые СУБД

- ✅ **PostgreSQL** (рекомендуется) - через SQLAlchemy или psycopg2
- ✅ **MySQL** - через SQLAlchemy
- ✅ **SQLite** - через SQLAlchemy
- ✅ **Oracle** - через SQLAlchemy

### Извлекаемая информация

- **Таблицы**: имена, схемы, комментарии
- **Колонки**: имена, типы данных, nullable, первичные ключи, комментарии
- **Внешние ключи**: связи между таблицами
- **Метаданные**: количество строк, время извлечения

### Кэширование

- ⚡ **Автоматическое кэширование** схемы для повышения производительности
- 🕐 **Настраиваемое время жизни кэша** (по умолчанию 5 минут)
- 🔄 **Принудительное обновление** через параметр `force_refresh=True`

## Использование

### 1. Базовое использование

```python
# Автоматическое создание экстрактора с настройками по умолчанию
from dynamic_schema_extractor import create_dynamic_extractor

extractor = create_dynamic_extractor()
schema = extractor.get_schema()

print(f"Найдено {len(schema.tables)} таблиц")
print(f"Схема для промпта:\n{schema.to_prompt_format()}")
```

### 2. С пользовательской строкой подключения

```python
# Удаленная БД
connection_string = "postgresql://user:pass@remote-host:5432/production_db"
extractor = create_dynamic_extractor(connection_string)
schema = extractor.get_schema()
```

### 3. Сохранение схемы в файл

```python
# Сохранение в совместимом с schema.json формате
extractor.save_schema_to_file("current_schema.json")
```

### 4. Использование с BI-GPT Agent

```python
from bi_gpt_agent import BIGPTAgent

# Агент автоматически будет использовать динамическую схему
agent = BIGPTAgent(
    api_key="your_api_key",
    use_finetuned=False
)

# Запрос с актуальной схемой
result = agent.query("покажи всех пользователей")
```

### 5. Использование с fine-tuned моделью

```python
from finetuned_sql_generator import FineTunedSQLGenerator

generator = FineTunedSQLGenerator(
    connection_string="postgresql://localhost:5432/mydb",
    use_dynamic_schema=True
)

sql, time_taken = generator.generate_sql("количество заказов")
```

## Конфигурация

### Переменные окружения

```bash
# Основная строка подключения к БД
DATABASE_URL="postgresql://user:password@localhost:5432/mydb"

# Для моделей через API
OPENAI_API_KEY="your_openai_key"
LOCAL_BASE_URL="https://your-api-endpoint/v1"
LOCAL_API_KEY="your_local_key"
```

### Настройки в config.py

```python
class Settings(BaseSettings):
    database_url: str = Field("postgresql://localhost:5432/bi_demo", env="DATABASE_URL")
    # ... другие настройки
```

## Тестирование

Для проверки функциональности запустите:

```bash
python test_dynamic_schema.py
```

Тест проверит:
- ✅ Подключение к БД и извлечение схемы
- ✅ Создание промптов с динамической схемой
- ✅ Работу кэширования
- ✅ Совместимость с генераторами SQL

## Принципы работы

### 1. Извлечение схемы

```mermaid
graph TB
    A[DynamicSchemaExtractor] --> B{SQLAlchemy доступен?}
    B -->|Да| C[Использовать SQLAlchemy]
    B -->|Нет| D{PostgreSQL + psycopg2?}
    D -->|Да| E[Использовать psycopg2]
    D -->|Нет| F[Ошибка: нет подходящего коннектора]
    
    C --> G[Извлечь метаданные]
    E --> G
    G --> H[Создать DatabaseSchema]
    H --> I[Кэшировать результат]
```

### 2. Генерация промптов

```mermaid
graph TB
    A[SQLGenerator.generate_sql] --> B[Получить схему из кэша/БД]
    B --> C[Преобразовать в формат промпта]
    C --> D[Создать динамический промпт]
    D --> E[Отправить в модель]
    E --> F[Вернуть SQL]
```

### 3. Кэширование

- **Время жизни**: настраиваемое (по умолчанию 5 минут)
- **Стратегия**: извлечение только при истечении TTL или принудительном обновлении
- **Память**: схема хранится в памяти экземпляра класса

## Fallback стратегия

При ошибках динамического извлечения система автоматически переключается на статическую схему:

1. **Попытка динамического извлечения** схемы из БД
2. **При ошибке**: предупреждение в логах + использование статической схемы
3. **Статическая схема**: предопределенная схема таблиц `customers`, `products`, `orders`, `sales`, `inventory`

## Производительность

- ⚡ **Первый запрос**: ~200-500ms (извлечение схемы из БД)
- ⚡ **Последующие запросы**: ~1-5ms (использование кэша)
- 📊 **Память**: ~1-10MB на схему (зависит от размера БД)

## Ограничения

- **Размер БД**: очень большие БД (1000+ таблиц) могут замедлить первый запрос
- **Права доступа**: требуются права чтения метаданных БД
- **Сетевая задержка**: для удаленных БД время извлечения может увеличиться

## Устранение неполадок

### Ошибка подключения к БД

```
Failed to extract schema: connection failed
```

**Решение**: Проверьте строку подключения и права доступа

### Отсутствие SQLAlchemy

```
Cannot import dynamic schema extractor: No module named 'sqlalchemy'
```

**Решение**: Установите зависимости:
```bash
pip install sqlalchemy psycopg2-binary
```

### Fallback на статическую схему

```
Warning: Failed to get dynamic schema, falling back to static
```

**Это нормально**: система продолжит работать со статической схемой

## Примеры использования

### Пример 1: Миграция с существующей БД

```python
# Извлекаем схему из продакшн БД
prod_extractor = create_dynamic_extractor(
    "postgresql://readonly_user@prod-db:5432/app_db"
)
schema = prod_extractor.get_schema()

# Используем схему для генерации SQL
agent = BIGPTAgent(connection_string="postgresql://readonly_user@prod-db:5432/app_db")
result = agent.query("покажи последние заказы")
```

### Пример 2: Разработка с локальной БД

```python
# Локальная БД для разработки
dev_extractor = create_dynamic_extractor(
    "postgresql://dev_user@localhost:5432/dev_db",
    cache_ttl=30  # Быстрое обновление для разработки
)

# Автоматическое получение актуальной схемы при изменениях в БД
while True:
    schema = dev_extractor.get_schema(force_refresh=True)
    print(f"Текущая схема: {len(schema.tables)} таблиц")
    time.sleep(60)  # Проверка каждую минуту
```

## Заключение

Динамическое извлечение схемы значительно упрощает работу с реальными базами данных, устраняя необходимость ручного обновления схемы в коде и обеспечивая актуальность информации для моделей ИИ.
