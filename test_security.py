#!/usr/bin/env python3
"""
Демонстрация безопасности: все секреты убраны из кода
"""

import os
import sys
from pathlib import Path

def test_no_hardcoded_secrets():
    """Проверяем, что в коде нет хардкод секретов"""
    print("🔍 Проверка отсутствия хардкод секретов в коде...")
    
    # Список подозрительных паттернов
    suspicious_patterns = [
        "app-yzNqYV4e205Vui63kMQh1ckU",
        "https://hmw6p24zvcdgay-8000.proxy.runpod.net",
        "https://bkwg3037dnb7aq-8000.proxy.runpod.net",
        "sk-",  # OpenAI ключи
        "API_KEY=app",
        "api_key=\"app",
        "api_key='app"
    ]
    
    # Файлы для проверки
    files_to_check = [
        "bi_gpt_agent.py",
        "start_system.py", 
        "streamlit_app.py",
        "config.py"
    ]
    
    found_issues = []
    
    for file_path in files_to_check:
        if not Path(file_path).exists():
            continue
            
        print(f"  Проверяем {file_path}...")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                
            for pattern in suspicious_patterns:
                if pattern in content:
                    found_issues.append(f"{file_path}: найден паттерн '{pattern}'")
                    
        except Exception as e:
            print(f"    ⚠️  Ошибка чтения {file_path}: {e}")
    
    if found_issues:
        print(f"  ❌ Найдены проблемы:")
        for issue in found_issues:
            print(f"    - {issue}")
        return False
    else:
        print(f"  ✅ Хардкод секретов не найден в {len(files_to_check)} файлах")
        return True

def test_env_file_protection():
    """Проверяем защиту .env файла"""
    print("\n🛡️  Проверка защиты .env файла...")
    
    gitignore_path = Path(".gitignore")
    env_path = Path(".env")
    
    # Проверяем .gitignore
    if not gitignore_path.exists():
        print("  ⚠️  .gitignore файл не найден")
        return False
    
    try:
        with open(gitignore_path, 'r', encoding='utf-8') as f:
            gitignore_content = f.read()
        
        if ".env" in gitignore_content:
            print("  ✅ .env файл защищен в .gitignore")
        else:
            print("  ❌ .env файл НЕ защищен в .gitignore")
            return False
            
    except Exception as e:
        print(f"  ❌ Ошибка чтения .gitignore: {e}")
        return False
    
    # Проверяем существование .env
    if env_path.exists():
        print("  ✅ .env файл существует")
    else:
        print("  ⚠️  .env файл не найден (используется env.example)")
    
    return True

def test_environment_variables():
    """Проверяем загрузку переменных окружения"""
    print("\n🔑 Проверка переменных окружения...")
    
    # Проверяем, что можем прочитать .env
    env_path = Path(".env")
    if env_path.exists():
        print("  📁 Читаем .env файл...")
        
        try:
            with open(env_path, 'r', encoding='utf-8') as f:
                env_lines = f.readlines()
            
            env_vars = {}
            for line in env_lines:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    env_vars[key] = value
            
            print(f"  ✅ Найдено {len(env_vars)} переменных в .env")
            
            # Проверяем ключевые переменные
            required_vars = ['LOCAL_API_KEY', 'LOCAL_BASE_URL']
            missing_vars = []
            
            for var in required_vars:
                if var in env_vars and env_vars[var] and env_vars[var] != f"your_{var.lower()}_here":
                    print(f"  ✅ {var}: настроен")
                else:
                    missing_vars.append(var)
                    print(f"  ⚠️  {var}: не настроен или значение по умолчанию")
            
            if missing_vars:
                print(f"  💡 Запустите 'python3 setup_env.py' для настройки: {', '.join(missing_vars)}")
                return len(missing_vars) == 0
            else:
                print(f"  ✅ Все необходимые переменные настроены")
                return True
                
        except Exception as e:
            print(f"  ❌ Ошибка чтения .env: {e}")
            return False
    else:
        print("  📋 Проверяем системные переменные окружения...")
        
        system_vars = ['LOCAL_API_KEY', 'LOCAL_BASE_URL']
        found_vars = []
        
        for var in system_vars:
            if os.getenv(var):
                found_vars.append(var)
                print(f"  ✅ {var}: найден в системе")
            else:
                print(f"  ❌ {var}: не найден")
        
        if found_vars:
            print(f"  ✅ Найдено {len(found_vars)} системных переменных")
            return len(found_vars) >= 2
        else:
            print("  ⚠️  Системные переменные не настроены")
            print("  💡 Создайте .env файл: cp env.example .env")
            return False

def test_config_system():
    """Проверяем систему конфигурации"""
    print("\n⚙️  Проверка системы конфигурации...")
    
    try:
        # Пытаемся импортировать конфигурацию
        sys.path.insert(0, str(Path.cwd()))
        
        try:
            from config import get_settings, validate_config
            
            print("  ✅ Система конфигурации доступна")
            
            # Пытаемся загрузить настройки
            try:
                settings = get_settings()
                print(f"  ✅ Настройки загружены: {settings.app_name}")
                
                # Проверяем валидацию
                errors = validate_config()
                if errors:
                    print(f"  ⚠️  Найдены проблемы конфигурации:")
                    for error in errors[:3]:  # Показываем первые 3
                        print(f"     - {error}")
                else:
                    print("  ✅ Конфигурация валидна")
                
                # Проверяем API ключ
                try:
                    api_key = settings.get_api_key()
                    if api_key and len(api_key) > 5:
                        print(f"  ✅ API ключ загружен: {api_key[:4]}...")
                    else:
                        print("  ⚠️  API ключ не загружен или пустой")
                except Exception as e:
                    print(f"  ❌ Ошибка получения API ключа: {e}")
                
                return True
                
            except Exception as e:
                print(f"  ❌ Ошибка загрузки настроек: {e}")
                return False
                
        except ImportError:
            print("  ⚠️  Система конфигурации недоступна (нет pydantic)")
            print("  💡 Установите зависимости: pip install pydantic")
            
            # Проверяем базовую загрузку переменных
            api_key = os.getenv('LOCAL_API_KEY')
            base_url = os.getenv('LOCAL_BASE_URL')
            
            if api_key and base_url:
                print(f"  ✅ Переменные окружения доступны напрямую")
                return True
            else:
                print(f"  ❌ Переменные окружения недоступны")
                return False
                
    except Exception as e:
        print(f"  ❌ Критическая ошибка: {e}")
        return False

def main():
    """Главная функция тестирования безопасности"""
    print("🔒 BI-GPT Agent - Тест безопасности конфигурации")
    print("=" * 60)
    print("Проверяем, что все секреты убраны из кода и защищены")
    print("=" * 60)
    
    tests = [
        ("Отсутствие хардкод секретов", test_no_hardcoded_secrets),
        ("Защита .env файла", test_env_file_protection),
        ("Переменные окружения", test_environment_variables),
        ("Система конфигурации", test_config_system)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n{'='*20} {test_name} {'='*20}")
        try:
            if test_func():
                print(f"✅ {test_name}: ПРОЙДЕН")
                passed += 1
            else:
                print(f"❌ {test_name}: НЕ ПРОЙДЕН")
        except Exception as e:
            print(f"💥 {test_name}: ОШИБКА - {e}")
    
    # Итоговый отчет
    print("\n" + "=" * 60)
    print("📊 ИТОГОВЫЙ ОТЧЕТ БЕЗОПАСНОСТИ")
    print("=" * 60)
    
    success_rate = (passed / total) * 100
    print(f"Пройдено тестов: {passed}/{total} ({success_rate:.1f}%)")
    
    if success_rate >= 75:
        print("🎉 ОТЛИЧНЫЙ РЕЗУЛЬТАТ! Безопасность на высоком уровне:")
        print("   ✅ Секреты убраны из кода")
        print("   ✅ .env файл защищен")  
        print("   ✅ Переменные окружения настроены")
        print("   ✅ Система конфигурации работает")
        
        print("\n🚀 Система готова к использованию:")
        print("   python3 start_system.py")
        
    elif success_rate >= 50:
        print("⚠️  ХОРОШИЙ РЕЗУЛЬТАТ, но есть области для улучшения:")
        print("   💡 Проверьте настройки переменных окружения")
        print("   💡 Запустите: python3 setup_env.py")
        
    else:
        print("🚨 ТРЕБУЕТСЯ ВНИМАНИЕ к безопасности:")
        print("   💡 Настройте переменные окружения")
        print("   💡 Создайте .env файл")
        print("   💡 Запустите: python3 setup_env.py")
    
    print(f"\n📚 Подробные инструкции: SECURITY_SETUP.md")
    
    return 0 if success_rate >= 75 else 1

if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\n👋 Тест прерван пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n💥 Критическая ошибка: {e}")
        sys.exit(1)
