#!/usr/bin/env python3
"""
Скрипт для настройки переменных окружения BI-GPT Agent
Помогает безопасно настроить API ключи и другие секреты
"""

import os
import shutil
from pathlib import Path
import getpass


def setup_environment():
    """Интерактивная настройка переменных окружения"""
    print("🔧 Настройка переменных окружения BI-GPT Agent")
    print("=" * 50)
    
    env_file = Path(".env")
    env_example = Path("env.example")
    
    # Проверяем наличие примера
    if not env_example.exists():
        print("❌ Файл env.example не найден!")
        return False
    
    # Проверяем существующий .env
    if env_file.exists():
        print("⚠️  Файл .env уже существует")
        overwrite = input("Хотите перезаписать его? (y/N): ").lower()
        if overwrite != 'y':
            print("Отменено")
            return False
        
        # Создаем бэкап
        backup_file = Path(f".env.backup.{env_file.stat().st_mtime_ns}")
        shutil.copy2(env_file, backup_file)
        print(f"✅ Создан бэкап: {backup_file}")
    
    # Копируем пример
    shutil.copy2(env_example, env_file)
    print(f"✅ Создан файл .env из примера")
    
    # Интерактивная настройка ключевых параметров
    print("\n🔑 Настройка API ключей:")
    
    # Выбор провайдера модели
    print("\nВыберите провайдер модели:")
    print("1. Local Model (Llama-4-Scout)")
    print("2. OpenAI GPT-4")
    
    while True:
        choice = input("Ваш выбор (1-2): ").strip()
        if choice in ['1', '2']:
            break
        print("Пожалуйста, введите 1 или 2")
    
    env_vars = {}
    
    if choice == '1':
        # Настройка локальной модели
        env_vars['MODEL_PROVIDER'] = 'local'
        
        print("\n📡 Настройка локальной модели:")
        
        # API ключ
        while True:
            api_key = getpass.getpass("LOCAL_API_KEY (скрыт при вводе): ").strip()
            if api_key:
                env_vars['LOCAL_API_KEY'] = api_key
                break
            print("API ключ не может быть пустым")
        
        # Base URL
        while True:
            base_url = input("LOCAL_BASE_URL: ").strip()
            if base_url and (base_url.startswith('http://') or base_url.startswith('https://')):
                env_vars['LOCAL_BASE_URL'] = base_url
                break
            print("Введите корректный URL (http:// или https://)")
        
        # Модель
        model_name = input("LOCAL_MODEL_NAME [llama4scout]: ").strip()
        env_vars['LOCAL_MODEL_NAME'] = model_name or 'llama4scout'
        
    else:
        # Настройка OpenAI
        env_vars['MODEL_PROVIDER'] = 'openai'
        
        print("\n🤖 Настройка OpenAI:")
        
        while True:
            api_key = getpass.getpass("OPENAI_API_KEY (скрыт при вводе): ").strip()
            if api_key:
                env_vars['OPENAI_API_KEY'] = api_key
                break
            print("API ключ не может быть пустым")
        
        model = input("OPENAI_MODEL [gpt-4]: ").strip()
        env_vars['OPENAI_MODEL'] = model or 'gpt-4'
    
    # Дополнительные настройки
    print("\n⚙️  Дополнительные настройки:")
    
    # Окружение
    print("\nВыберите окружение:")
    print("1. Development")
    print("2. Production")
    
    while True:
        env_choice = input("Ваш выбор (1-2): ").strip()
        if env_choice in ['1', '2']:
            break
        print("Пожалуйста, введите 1 или 2")
    
    env_vars['APP_ENVIRONMENT'] = 'development' if env_choice == '1' else 'production'
    
    # Уровень логирования
    if env_choice == '1':
        env_vars['LOG_LEVEL'] = 'DEBUG'
        env_vars['DEBUG_MODE'] = 'true'
    else:
        env_vars['LOG_LEVEL'] = 'INFO'
        env_vars['DEBUG_MODE'] = 'false'
    
    # Применяем изменения к .env файлу
    print("\n💾 Применение настроек...")
    
    with open(env_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    for key, value in env_vars.items():
        # Заменяем значения в файле
        if f"{key}=" in content:
            # Находим строку и заменяем значение
            lines = content.split('\n')
            for i, line in enumerate(lines):
                if line.strip().startswith(f"{key}=") and not line.strip().startswith('#'):
                    lines[i] = f"{key}={value}"
                    break
            content = '\n'.join(lines)
        else:
            # Добавляем новую переменную
            content += f"\n{key}={value}"
    
    with open(env_file, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print("✅ Настройки сохранены в .env файл")
    
    # Проверка настроек
    print("\n🧪 Проверка настроек...")
    
    try:
        # Загружаем переменные из .env
        with open(env_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key] = value
        
        # Проверяем конфигурацию
        try:
            from config import get_settings, validate_config
            
            settings = get_settings()
            errors = validate_config()
            
            if errors:
                print("⚠️  Найдены проблемы в конфигурации:")
                for error in errors:
                    print(f"   - {error}")
            else:
                print("✅ Конфигурация валидна")
                
            print(f"✅ Провайдер модели: {settings.model_provider.value}")
            print(f"✅ Окружение: {settings.environment.value}")
            
        except ImportError:
            print("⚠️  Расширенная валидация недоступна (нет pydantic)")
            print("✅ Базовая проверка пройдена")
            
    except Exception as e:
        print(f"❌ Ошибка при проверке: {e}")
        return False
    
    # Инструкции по использованию
    print("\n🚀 Готово! Инструкции по использованию:")
    print("=" * 50)
    print("1. Запуск веб-интерфейса:")
    print("   python3 start_system.py")
    print("")
    print("2. Запуск из командной строки:")
    print("   python3 bi_gpt_agent.py --query 'покажи всех клиентов'")
    print("")
    print("3. Тестирование:")
    print("   python3 simple_demo.py")
    print("")
    print("🔒 Безопасность:")
    print("   - Файл .env добавлен в .gitignore")
    print("   - Не публикуйте API ключи в репозитории")
    print("   - Используйте разные ключи для dev/prod")
    
    return True


def show_current_config():
    """Показывает текущую конфигурацию (без секретов)"""
    print("📋 Текущая конфигурация:")
    print("=" * 30)
    
    env_file = Path(".env")
    if not env_file.exists():
        print("❌ Файл .env не найден")
        print("Запустите: python3 setup_env.py")
        return
    
    try:
        with open(env_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    
                    # Скрываем секретные значения
                    if any(secret in key.upper() for secret in ['KEY', 'SECRET', 'PASSWORD', 'TOKEN']):
                        if value:
                            display_value = value[:4] + '*' * (len(value) - 4) if len(value) > 4 else '***'
                        else:
                            display_value = '<не задано>'
                    else:
                        display_value = value or '<не задано>'
                    
                    print(f"   {key}: {display_value}")
                    
    except Exception as e:
        print(f"❌ Ошибка чтения конфигурации: {e}")


def main():
    """Главная функция"""
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == 'show':
        show_current_config()
        return
    
    print("🔧 BI-GPT Agent - Настройка переменных окружения")
    print("")
    print("Этот скрипт поможет вам безопасно настроить API ключи")
    print("и другие переменные окружения для BI-GPT Agent.")
    print("")
    
    if input("Продолжить? (y/N): ").lower() != 'y':
        print("Отменено")
        return
    
    try:
        success = setup_environment()
        if success:
            print("\n🎉 Настройка завершена успешно!")
        else:
            print("\n❌ Настройка не завершена")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n\n👋 Настройка прервана пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Критическая ошибка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
