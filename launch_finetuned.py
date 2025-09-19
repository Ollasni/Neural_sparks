#!/usr/bin/env python3
"""
Скрипт запуска BI-GPT Agent с fine-tuned моделью (Phi-3 + LoRA)
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

def setup_finetuned_env():
    """Проверка настроек для fine-tuned модели"""
    print("🔧 Проверка настроек для fine-tuned модели")
    print("=" * 50)
    
    # Проверяем наличие fine-tuned модели
    finetuned_path = Path("finetuning/phi3_bird_lora")
    base_model_path = Path("finetuning/phi3-mini")
    
    if not finetuned_path.exists():
        print("❌ Fine-tuned LoRA адаптер не найден!")
        print("Путь:", finetuned_path.absolute())
        print("\nДля обучения модели запустите:")
        print("cd finetuning && python finetune_bird_phi3.py")
        return False
    
    if not base_model_path.exists():
        print("❌ Базовая модель Phi-3 не найдена!")
        print("Путь:", base_model_path.absolute())
        print("\nСкачайте модель Phi-3 или убедитесь, что она находится в правильной папке")
        return False
    
    print(f"✅ Fine-tuned LoRA адаптер найден: {finetuned_path}")
    print(f"✅ Базовая модель Phi-3 найдена: {base_model_path}")
    
    # Проверяем необходимые библиотеки
    try:
        import torch
        import transformers
        import peft
        print("✅ Необходимые библиотеки установлены")
    except ImportError as e:
        print(f"❌ Не установлены необходимые библиотеки: {e}")
        print("\nУстановите зависимости:")
        print("pip install torch transformers peft")
        return False
    
    # Проверяем наличие GPU/MPS
    if torch.cuda.is_available():
        print("✅ CUDA GPU доступен")
    elif torch.backends.mps.is_available():
        print("✅ Apple MPS доступен")
    else:
        print("⚠️  GPU недоступен, будет использоваться CPU (медленно)")
    
    return True

def test_finetuned_model():
    """Быстрый тест fine-tuned модели"""
    print("\n🧪 Тестирование fine-tuned модели...")
    
    try:
        from finetuned_sql_generator import FineTunedSQLGenerator
        
        # Создаем тестовый генератор
        generator = FineTunedSQLGenerator()
        
        # Тестовый запрос
        test_query = "покажи всех клиентов"
        print(f"Тестовый запрос: {test_query}")
        
        sql, exec_time = generator.generate_sql(test_query)
        
        if sql:
            print(f"✅ Модель работает! SQL: {sql}")
            print(f"⏱️  Время генерации: {exec_time:.2f}с")
            generator.cleanup()
            return True
        else:
            print("❌ Модель не смогла сгенерировать SQL")
            generator.cleanup()
            return False
            
    except Exception as e:
        print(f"❌ Ошибка тестирования модели: {e}")
        return False

def launch_with_finetuned():
    """Запуск системы с fine-tuned моделью"""
    print("\n🚀 Запуск BI-GPT Agent с fine-tuned моделью")
    print("=" * 50)
    
    # Настройка окружения
    if not setup_finetuned_env():
        return False
    
    # Быстрый тест модели
    if not test_finetuned_model():
        print("\n⚠️  Тест модели не прошел, но продолжаем...")
        print("Система попытается загрузить модель при первом запросе")
    
    # Устанавливаем переменную окружения для использования fine-tuned модели
    os.environ["USE_FINETUNED_MODEL"] = "true"
    
    # Запуск системы
    print("\n🌐 Запуск веб-интерфейса...")
    print("Откроется: http://localhost:8501")
    
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
    print("🤖 BI-GPT Agent - Fine-tuned Model Launcher")
    print("Использует Phi-3 с LoRA адаптером для SQL генерации")
    print("")
    
    if len(sys.argv) > 1 and sys.argv[1] == "--help":
        print("Использование:")
        print("  python launch_finetuned.py          # Запуск веб-интерфейса")
        print("  python launch_finetuned.py --test   # Только тестирование")
        print("  python launch_finetuned.py --help   # Эта справка")
        return
    
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        print("🧪 Тестирование конфигурации...")
        if setup_finetuned_env():
            print("✅ Конфигурация готова")
        else:
            print("❌ Проблемы с конфигурацией")
        return
    
    try:
        launch_with_finetuned()
    except KeyboardInterrupt:
        print("\n👋 Запуск прерван пользователем")
    except Exception as e:
        print(f"💥 Критическая ошибка: {e}")

if __name__ == "__main__":
    main()
