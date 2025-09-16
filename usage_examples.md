# Примеры использования BI-GPT Agent

## Запуск с аргументами командной строки

### Базовый запуск (с настройками по умолчанию)
```bash
python3 bi_gpt_agent.py
```

### Запуск с кастомными параметрами
```bash
python3 bi_gpt_agent.py --api_key your_api_key --base_url https://your-model-url.com/v1
```

### Выполнение одиночного запроса
```bash
python3 bi_gpt_agent.py --query "покажи всех клиентов"
```

### Запрос с кастомными параметрами
```bash
python3 bi_gpt_agent.py --api_key your_key --base_url https://your-url.com/v1 --query "средний чек клиентов"
```

## Запуск системы запуска

### Базовый запуск
```bash
python3 start_system.py
```

### Запуск с кастомными параметрами
```bash
python3 start_system.py --api_key your_key --base_url https://your-url.com/v1
```

### Запуск без демо (сразу к меню)
```bash
python3 start_system.py --skip_demo
```

### Полная кастомизация
```bash
python3 start_system.py --api_key your_key --base_url https://your-url.com/v1 --skip_demo
```

## Примеры запросов

### Простые запросы
```bash
python3 bi_gpt_agent.py --query "покажи всех клиентов"
python3 bi_gpt_agent.py --query "количество заказов"
python3 bi_gpt_agent.py --query "средний чек клиентов"
```

### Сложные запросы
```bash
python3 bi_gpt_agent.py --query "прибыль за последние 2 дня"
python3 bi_gpt_agent.py --query "топ 3 клиента по выручке"
python3 bi_gpt_agent.py --query "остатки товаров на складе"
```

## Справка по аргументам

### bi_gpt_agent.py
```bash
python3 bi_gpt_agent.py --help
```

Доступные аргументы:
- `--api_key` - API ключ для модели
- `--base_url` - Базовый URL API модели
- `--query` - Одиночный запрос для выполнения

### start_system.py
```bash
python3 start_system.py --help
```

Доступные аргументы:
- `--api_key` - API ключ для модели
- `--base_url` - Базовый URL API модели  
- `--skip_demo` - Пропустить начальное демо

## Переменные окружения

Альтернативно можно использовать переменные окружения:
```bash
export BI_GPT_API_KEY="your_api_key"
export BI_GPT_BASE_URL="https://your-url.com/v1"
python3 bi_gpt_agent.py
```

## Интеграция в скрипты

### Batch обработка запросов
```bash
#!/bin/bash
queries=(
    "покажи всех клиентов"
    "количество заказов"
    "средний чек клиентов"
)

for query in "${queries[@]}"; do
    echo "Processing: $query"
    python3 bi_gpt_agent.py --query "$query"
    echo "---"
done
```

### CI/CD интеграция
```yaml
# GitHub Actions example
- name: Test BI-GPT Agent
  run: |
    python3 bi_gpt_agent.py --api_key ${{ secrets.API_KEY }} --query "покажи всех клиентов"
```
