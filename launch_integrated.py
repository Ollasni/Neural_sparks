#!/usr/bin/env python3
"""
Скрипт запуска интегрированного BI-GPT Agent
Объединяет основную функциональность с backend архитектурой
"""

import os
import sys
import subprocess
from pathlib import Path

def show_logo():
    """Логотип системы"""
    print("""
╔══════════════════════════════════════════════════════════════╗
║                BI-GPT Agent - Integrated v1.0               ║
║              Natural Language to SQL System                  ║
║                                                              ║
║  🚀 Интегрированная версия:                                 ║
║  • Подключение к PostgreSQL                                 ║
║  • Просмотр схемы базы данных                               ║
║  • Генерация SQL на естественном языке                       ║
║  • Визуализация результатов                                 ║
║  • История запросов                                          ║
╚══════════════════════════════════════════════════════════════╝
""")

def check_requirements():
    """Проверка системных требований"""
    print("🔍 Проверка системных требований...")
    
    # Проверка Python версии
    if sys.version_info < (3, 8):
        print("❌ Требуется Python 3.8+")
        return False
    print(f"✅ Python {sys.version.split()[0]}")
    
    # Проверка зависимостей
    required_packages = [
        'streamlit', 'pandas', 'sqlalchemy', 'psycopg2', 
        'plotly', 'pydantic', 'openai'
    ]
    missing = []
    
    for package in required_packages:
        try:
            __import__(package)
            print(f"✅ {package}")
        except ImportError:
            missing.append(package)
            print(f"❌ {package}")
    
    if missing:
        print(f"\n⚠️  Отсутствуют пакеты: {', '.join(missing)}")
        print("Установите: pip install -r requirements.txt")
        return False
    
    return True

def check_database():
    """Проверка доступности базы данных"""
    print("\n🗄️  Проверка базы данных...")
    
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor
        
        # Пробуем подключиться к PostgreSQL
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            user="olgasnissarenko",
            database="bi_demo"
        )
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT version();")
            version = cur.fetchone()
            print(f"✅ PostgreSQL подключен: {version['version'][:50]}...")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка подключения к PostgreSQL: {e}")
        print("\n💡 Убедитесь что:")
        print("   • PostgreSQL запущен")
        print("   • База данных 'bi_demo' существует")
        print("   • Пользователь 'olgasnissarenko' имеет доступ")
        return False

def launch_app():
    """Запуск интегрированного приложения"""
    print("\n🚀 Запуск интегрированного приложения...")
    print("🌐 Приложение будет доступно по адресу: http://localhost:8501")
    print("⏹️  Для остановки нажмите Ctrl+C")
    print("-" * 60)
    
    try:
        # Запускаем Streamlit
        subprocess.run([
            sys.executable, "-m", "streamlit", "run", 
            "integrated_app.py",
            "--server.port=8501",
            "--server.address=0.0.0.0",
            "--server.headless=true"
        ], check=True)
    except KeyboardInterrupt:
        print("\n👋 Приложение остановлено пользователем")
    except subprocess.CalledProcessError as e:
        print(f"❌ Ошибка запуска приложения: {e}")
        return False
    except Exception as e:
        print(f"❌ Неожиданная ошибка: {e}")
        return False
    
    return True

def main():
    """Главная функция"""
    show_logo()
    
    # Проверка аргументов командной строки
    if len(sys.argv) > 1:
        if sys.argv[1] == "--help":
            print("Использование:")
            print("  python launch_integrated.py          # Запуск приложения")
            print("  python launch_integrated.py --check  # Только проверка")
            print("  python launch_integrated.py --help   # Эта справка")
            return
        elif sys.argv[1] == "--check":
            print("🔍 Проверка конфигурации...")
            if check_requirements() and check_database():
                print("✅ Все проверки пройдены успешно!")
            else:
                print("❌ Есть проблемы с конфигурацией")
            return
    
    # Проверка требований
    if not check_requirements():
        print("\n❌ Не все требования выполнены")
        return
    
    # Проверка базы данных
    if not check_database():
        print("\n⚠️  Проблемы с базой данных, но продолжаем...")
        print("Приложение попытается подключиться при запуске")
    
    # Запуск приложения
    success = launch_app()
    
    if success:
        print("\n✅ Приложение завершено успешно")
    else:
        print("\n❌ Приложение завершено с ошибками")

if __name__ == "__main__":
    main()
