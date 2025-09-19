# 🚀 Быстрый старт BI-GPT Agent с PostgreSQL

## ✅ PostgreSQL настроен и работает!

Ваша система полностью готова к работе с PostgreSQL:

```bash
✅ PostgreSQL 15.14 установлен и запущен
✅ База данных bi_demo создана  
✅ Python драйвер psycopg2-binary установлен
✅ Подключение протестировано и работает
```

## 🔧 Осталось установить ML зависимости

Для работы fine-tuned модели установите:

```bash
# Основные ML библиотеки
pip3 install torch transformers peft

# Или все сразу из requirements
pip3 install -r requirements.txt
```

## 🎯 Запуск системы

### Вариант 1: Fine-tuned модель (рекомендуется)
```bash
export PATH="/opt/homebrew/opt/postgresql@15/bin:$PATH"
python3 launch_finetuned.py
```

### Вариант 2: Универсальный лаунчер
```bash
export PATH="/opt/homebrew/opt/postgresql@15/bin:$PATH"  
python3 launch.py
```

## 📊 Что будет работать

1. **Fine-tuned модель Phi-3 + LoRA** - генерирует SQL напрямую
2. **PostgreSQL база данных** - правильный синтаксис без ошибок  
3. **Веб-интерфейс Streamlit** - на http://localhost:8501
4. **Автоматическое создание таблиц** - customers, products, orders, sales, inventory

## 💡 Добавить в ~/.zshrc для постоянного PATH

```bash
echo 'export PATH="/opt/homebrew/opt/postgresql@15/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

## 🎉 Результат

После установки ML библиотек вы получите:

```
✅ PostgreSQL демо база данных инициализирована
✅ Используется fine-tuned модель Phi-3 + LoRA  
🔍 Сгенерированный SQL: SELECT * FROM customers LIMIT 1000
```

Система будет генерировать корректный PostgreSQL синтаксис! 🚀
