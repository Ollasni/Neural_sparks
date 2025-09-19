# 🔒 Безопасная настройка BI-GPT Agent

## 🚨 ВАЖНО: Защита API ключей и секретов

Все пароли и API ключи теперь вынесены из кода в переменные окружения для максимальной безопасности.

## 🔧 Быстрая настройка

### Вариант 1: Автоматическая настройка (Рекомендуется)

```bash
# Запустите интерактивный скрипт настройки
python3 setup_env.py

# Следуйте инструкциям для настройки API ключей
```

### Вариант 2: Ручная настройка

```bash
# 1. Скопируйте пример конфигурации
cp env.example .env

# 2. Отредактируйте .env файл
nano .env  # или любой редактор

# 3. Заполните необходимые поля:
# LOCAL_API_KEY=ваш_реальный_api_ключ
# LOCAL_BASE_URL=https://ваш_url_модели/v1
```

### Вариант 3: Переменные окружения

```bash
# Установите переменные в системе
export LOCAL_API_KEY="ваш_api_ключ"
export LOCAL_BASE_URL="https://ваш_url_модели/v1"
export MODEL_PROVIDER="local"

# Или добавьте в ~/.bashrc / ~/.zshrc для постоянного эффекта
echo 'export LOCAL_API_KEY="ваш_api_ключ"' >> ~/.bashrc
```

## 🔑 Настройка API ключей

### Для локальной модели (Llama-4-Scout):
```bash
LOCAL_API_KEY=ваш_реальный_ключ_здесь
LOCAL_BASE_URL=https://ваш-сервер.runpod.net/v1
LOCAL_MODEL_NAME=llama4scout
MODEL_PROVIDER=local
```

### Для OpenAI:
```bash
OPENAI_API_KEY=sk-ваш_openai_ключ_здесь
OPENAI_MODEL=gpt-4
MODEL_PROVIDER=openai
```

## 🛡️ Проверка безопасности

### ✅ Что сделано для защиты:

1. **Все секреты вынесены из кода** - нет хардкода паролей
2. **Файл .env в .gitignore** - не попадет в Git
3. **Graceful fallback** - система работает с любыми источниками настроек
4. **Маскирование в логах** - секреты скрыты в выводе
5. **Type safety** - валидация всех параметров

### 🔍 Проверьте конфигурацию:

```bash
# Показать текущие настройки (без секретов)
python3 setup_env.py show

# Проверить работу с новой конфигурацией
python3 bi_gpt_agent.py --query "покажи всех клиентов"
```

## 📁 Структура файлов конфигурации

```
bi-gpt-agent/
├── .env                    # ❌ НЕ в Git - ваши реальные секреты
├── env.example             # ✅ В Git - пример конфигурации
├── .gitignore              # ✅ Защищает .env от попадания в Git
├── config.py               # ✅ Система управления конфигурацией
└── setup_env.py            # ✅ Помощник настройки
```

## 🚀 Запуск после настройки

### Веб-интерфейс:
```bash
python3 start_system.py
# Откроется http://localhost:8501
```

### Командная строка:
```bash
python3 bi_gpt_agent.py --query "покажи всех клиентов"
```

### Streamlit:
```bash
streamlit run streamlit_app.py
```

## ⚠️ Правила безопасности

### ✅ МОЖНО:
- Использовать .env файлы для локальной разработки
- Устанавливать переменные окружения на сервере
- Делиться env.example файлом
- Использовать разные ключи для dev/staging/prod

### ❌ НЕЛЬЗЯ:
- Коммитить .env файлы в Git
- Отправлять API ключи в Slack/email
- Использовать production ключи для разработки
- Логировать секретные значения

## 🔧 Troubleshooting

### Проблема: "API key is required"
```bash
# Проверьте установлены ли переменные
echo $LOCAL_API_KEY
echo $LOCAL_BASE_URL

# Если пусто, проверьте .env файл
cat .env | grep LOCAL_API_KEY
```

### Проблема: "Configuration validation failed"
```bash
# Запустите диагностику
python3 setup_env.py show

# Перенастройте если нужно
python3 setup_env.py
```

### Проблема: Streamlit не видит переменные
```bash
# Убедитесь что .env файл в той же директории
ls -la .env

# Или экспортируйте переменные явно
export LOCAL_API_KEY="ваш_ключ"
streamlit run streamlit_app.py
```

## 🌍 Deployment в Production

### Docker:
```dockerfile
# Передавайте секреты через Docker secrets или env файлы
ENV LOCAL_API_KEY=${LOCAL_API_KEY}
ENV LOCAL_BASE_URL=${LOCAL_BASE_URL}
```

### Kubernetes:
```yaml
# Используйте Kubernetes secrets
apiVersion: v1
kind: Secret
metadata:
  name: bi-gpt-secrets
type: Opaque
stringData:
  LOCAL_API_KEY: "ваш_ключ"
  LOCAL_BASE_URL: "https://ваш_url"
```

### Cloud Providers:
- **AWS**: AWS Systems Manager Parameter Store
- **Google Cloud**: Secret Manager
- **Azure**: Key Vault
- **Heroku**: Config Vars

## 📞 Поддержка

Если у вас проблемы с настройкой:

1. Запустите диагностику: `python3 setup_env.py show`
2. Проверьте .env файл существует и содержит ключи
3. Убедитесь что .env в той же директории что и скрипты
4. Попробуйте экспортировать переменные вручную

---

🔒 **Помните: Безопасность ваших API ключей - это безопасность ваших данных!**
