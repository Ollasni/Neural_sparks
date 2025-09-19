#!/usr/bin/env python3
"""
Универсальный скрипт запуска BI-GPT Agent
Поддерживает два варианта: fine-tuned модель и OpenAI API
"""

import os
import sys
import subprocess
from pathlib import Path

def show_logo():
    """Логотип системы"""
    print("""
╔══════════════════════════════════════════════════════════════╗
║                    BI-GPT Agent v1.0                        ║
║              Natural Language to SQL System                  ║
║                                                              ║
║  🚀 Два варианта запуска:                                   ║
║  1. Fine-tuned модель (Phi-3 + LoRA)                        ║
║  2. Llama 4 API (RunPod)                                    ║
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
    required_packages = ['openai', 'streamlit', 'pandas', 'sqlalchemy']
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

def check_finetuned_model():
    """Проверка наличия fine-tuned модели"""
    finetuned_path = Path("finetuning/phi3_bird_lora")
    if finetuned_path.exists():
        print("✅ Fine-tuned модель найдена")
        return True
    else:
        print("❌ Fine-tuned модель не найдена")
        return False

def check_api_key():
    """Проверка наличия API ключа для Llama 4"""
    key = os.getenv("LOCAL_API_KEY")
    if key:
        print(f"✅ Llama 4 API ключ найден: {key[:10]}...")
        return True
    else:
        print("❌ Llama 4 API ключ не найден")
        return False

def launch_finetuned():
    """Запуск с fine-tuned моделью"""
    print("\n🤖 Запуск с fine-tuned моделью (Phi-3 + LoRA)")
    print("=" * 50)
    
    try:
        subprocess.run([sys.executable, "launch_finetuned.py"])
    except Exception as e:
        print(f"❌ Ошибка запуска fine-tuned модели: {e}")

def launch_api():
    """Запуск с Llama 4 API"""
    print("\n🌐 Запуск с Llama 4 API (RunPod)")
    print("=" * 50)
    
    try:
        subprocess.run([sys.executable, "launch_api.py"])
    except Exception as e:
        print(f"❌ Ошибка запуска Llama 4 API: {e}")

def show_help():
    """Показать справку"""
    print("""
📖 Справка по запуску BI-GPT Agent

Доступные команды:
  python launch.py                    # Интерактивный выбор
  python launch.py --finetuned       # Запуск с fine-tuned моделью
  python launch.py --api             # Запуск с Llama 4 API
  python launch.py --check           # Проверка системы
  python launch.py --help            # Эта справка

Варианты запуска:

1. Fine-tuned модель (Phi-3 + LoRA):
   - Локальная модель, обученная на BIRD-SQL
   - Требует запущенный сервер модели (Ollama, vLLM, etc.)
   - Быстрее и дешевле для больших объемов

2. Llama 4 API (RunPod):
   - Использует облачный API через RunPod
   - Требует API ключ для доступа
   - Высокое качество, настраиваемые тарифы

Настройка:
  - Для fine-tuned: запустите обучение в finetuning/
  - Для Llama 4 API: установите LOCAL_API_KEY
""")

def check_system():
    """Проверка всей системы"""
    print("🔍 Полная проверка системы")
    print("=" * 30)
    
    # Системные требования
    if not check_requirements():
        print("\n❌ Системные требования не выполнены")
        return False
    
    print("\n📋 Доступные варианты:")
    
    # Fine-tuned модель
    finetuned_available = check_finetuned_model()
    
    # Llama 4 API
    api_available = check_api_key()
    
    print(f"\n📊 Статус:")
    print(f"  Fine-tuned модель: {'✅ Доступна' if finetuned_available else '❌ Недоступна'}")
    print(f"  Llama 4 API: {'✅ Доступен' if api_available else '❌ Недоступен'}")
    
    if not finetuned_available and not api_available:
        print("\n❌ Ни один вариант не доступен!")
        print("\nДля настройки:")
        print("1. Fine-tuned: cd finetuning && python finetune_bird_phi3.py")
        print("2. Llama 4 API: export LOCAL_API_KEY=your_key")
        return False
    
    print("\n✅ Система готова к работе")
    return True

def interactive_menu():
    """Интерактивное меню выбора"""
    print("\n🎯 Выберите вариант запуска:")
    print("1. Fine-tuned модель (Phi-3 + LoRA)")
    print("2. Llama 4 API (RunPod)")
    print("3. Проверка системы")
    print("4. Справка")
    print("5. Выход")
    
    while True:
        try:
            choice = input("\nВаш выбор (1-5): ").strip()
            
            if choice == "1":
                launch_finetuned()
                break
            elif choice == "2":
                launch_api()
                break
            elif choice == "3":
                check_system()
                break
            elif choice == "4":
                show_help()
                break
            elif choice == "5":
                print("👋 До свидания!")
                break
            else:
                print("Пожалуйста, введите число от 1 до 5")
                
        except KeyboardInterrupt:
            print("\n👋 До свидания!")
            break
        except Exception as e:
            print(f"Ошибка: {e}")

def main():
    """Главная функция"""
    show_logo()
    
    # Обработка аргументов командной строки
    if len(sys.argv) > 1:
        arg = sys.argv[1].lower()
        
        if arg in ["--help", "-h"]:
            show_help()
            return
        elif arg in ["--check", "-c"]:
            check_system()
            return
        elif arg in ["--finetuned", "-f"]:
            launch_finetuned()
            return
        elif arg in ["--api", "-a"]:
            launch_api()
            return
        else:
            print(f"❌ Неизвестный аргумент: {arg}")
            print("Используйте --help для справки")
            return
    
    # Интерактивный режим
    try:
        interactive_menu()
    except Exception as e:
        print(f"💥 Критическая ошибка: {e}")

if __name__ == "__main__":
    main()
