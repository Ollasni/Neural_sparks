#!/usr/bin/env python3
"""
Скрипт запуска BI-GPT Agent с Llama 4 через API
Все настройки берутся из .env файла
"""

import os
import sys
import subprocess
from pathlib import Path

# Загружаем переменные окружения из .env файла
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("⚠️ Модуль python-dotenv не установлен. Установите: pip install python-dotenv")

def check_env_file():
    """Проверка .env файла и обязательных настроек"""
    print("🔧 Проверка настроек из .env файла")
    print("=" * 50)
    
    # Проверяем наличие .env файла
    env_file = Path(".env")
    if not env_file.exists():
        print("❌ Файл .env не найден!")
        print("\nСоздайте .env файл с настройками:")
        print("LOCAL_API_KEY=your_api_key")
        print("LOCAL_BASE_URL=https://vsjz8fv63q4oju-8000.proxy.runpod.net/v1")
        print("MODEL_PROVIDER=local")
        return False
    
    print("✅ Файл .env найден")
    
    # Проверяем обязательные переменные
    api_key = os.getenv("LOCAL_API_KEY")
    base_url = os.getenv("LOCAL_BASE_URL")
    
    if not api_key:
        print("❌ LOCAL_API_KEY не найден в .env файле!")
        return False
    
    if not base_url:
        print("❌ LOCAL_BASE_URL не найден в .env файле!")
        return False
    
    print(f"✅ API Key: {api_key[:10]}...")
    print(f"✅ Base URL: {base_url}")
    return True

def check_api_connection():
    """Проверка подключения к API"""
    print("\n🔍 Проверка подключения к Llama 4 API...")
    
    base_url = os.getenv("LOCAL_BASE_URL")
    if not base_url:
        print("❌ BASE_URL не настроен")
        return False
    
    try:
        import requests
        # Убираем /v1 для проверки docs
        docs_url = base_url.replace("/v1", "/docs")
        response = requests.get(docs_url, timeout=10)
        if response.status_code == 200:
            print("✅ API сервер доступен")
            print("✅ Документация API найдена")
            return True
    except Exception as e:
        print(f"❌ Ошибка подключения к API: {e}")
        return False
    
    print("❌ API недоступен")
    return False

def launch_with_api():
    """Запуск системы с Llama 4 API"""
    print("\n🚀 Запуск BI-GPT Agent с Llama 4 API")
    print("=" * 50)
    
    # Проверка настроек
    if not check_env_file():
        return False
    
    # Проверка API
    if not check_api_connection():
        print("\n⚠️  API недоступен, но продолжаем...")
        print("Система попытается подключиться при первом запросе")
    
    # Запуск системы
    print("\n🌐 Запуск веб-интерфейса...")
    print("Откроется: http://localhost:8501")
    print("Используется: Llama 4 API из .env файла")
    
    try:
        subprocess.run([
            sys.executable, "-m", "streamlit", "run", 
            "streamlit_app.py",
            "--server.port=8501",
            "--server.address=0.0.0.0"
        ])
    except KeyboardInterrupt:
        print("\n👋 Веб-интерфейс остановлен")
    except Exception as e:
        print(f"❌ Ошибка запуска: {e}")

def main():
    """Главная функция"""
    print("🦙 BI-GPT Agent - Llama 4 API Launcher")
    print("Настройки загружаются из .env файла")
    print("")
    
    if len(sys.argv) > 1 and sys.argv[1] == "--help":
        print("Использование:")
        print("  python launch_api.py               # Запуск веб-интерфейса")
        print("  python launch_api.py --test        # Только тестирование")
        print("  python launch_api.py --help        # Эта справка")
        print("\nНастройте .env файл:")
        print("  LOCAL_API_KEY=your_api_key")
        print("  LOCAL_BASE_URL=https://vsjz8fv63q4oju-8000.proxy.runpod.net/v1")
        return
    
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        print("🧪 Тестирование конфигурации...")
        if check_env_file() and check_api_connection():
            print("✅ Конфигурация готова")
        else:
            print("❌ Проблемы с конфигурацией")
        return
    
    try:
        launch_with_api()
    except KeyboardInterrupt:
        print("\n👋 Запуск прерван пользователем")
    except Exception as e:
        print(f"💥 Критическая ошибка: {e}")

if __name__ == "__main__":
    main()
