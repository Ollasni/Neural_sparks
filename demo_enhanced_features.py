#!/usr/bin/env python3
"""
Демонстрация улучшенных функций BI-GPT Agent
Показывает работу новых систем: конфигурации, логирования, валидации SQL и обработки ошибок
"""

import os
import sys
from pathlib import Path

# Добавляем текущую директорию в путь
sys.path.insert(0, str(Path(__file__).parent))

def demo_config_system():
    """Демонстрация системы конфигурации"""
    print("="*60)
    print("🔧 ДЕМО: Система конфигурации")
    print("="*60)
    
    try:
        from config import get_settings, validate_config, config_manager
        
        settings = get_settings()
        print(f"✅ Настройки загружены:")
        print(f"   - Приложение: {settings.app_name} v{settings.app_version}")
        print(f"   - Окружение: {settings.environment.value}")
        print(f"   - Провайдер модели: {settings.model_provider.value}")
        print(f"   - База данных: {settings.database_url}")
        print(f"   - Лог-файл: {settings.log_file}")
        
        # Валидация конфигурации
        print(f"\n🔍 Валидация конфигурации:")
        errors = validate_config()
        if errors:
            print(f"   ⚠️  Найдены проблемы:")
            for error in errors:
                print(f"     - {error}")
        else:
            print(f"   ✅ Конфигурация валидна")
        
        # Сводка конфигурации
        print(f"\n📊 Сводка конфигурации:")
        summary = config_manager.get_config_summary()
        for section, data in summary.items():
            print(f"   {section}:")
            for key, value in data.items():
                if isinstance(value, dict):
                    continue
                print(f"     - {key}: {value}")
        
        return True
        
    except ImportError as e:
        print(f"❌ Система конфигурации недоступна: {e}")
        return False
    except Exception as e:
        print(f"❌ Ошибка демо конфигурации: {e}")
        return False


def demo_logging_system():
    """Демонстрация системы логирования"""
    print("\n" + "="*60)
    print("📝 ДЕМО: Система логирования")
    print("="*60)
    
    try:
        from logging_config import get_logger, setup_logging, log_exception, log_performance, log_user_action
        from exceptions import ValidationError, create_error_context
        
        # Настраиваем логирование
        setup_logging()
        logger = get_logger('demo')
        
        print(f"✅ Система логирования настроена")
        
        # Демо различных типов логов
        logger.info("Это информационное сообщение")
        logger.warning("Это предупреждение")
        
        # Демо логирования действий пользователя
        log_user_action(
            'demo_action',
            user_id='demo_user',
            session_id='demo_session',
            details={'feature': 'logging_demo'}
        )
        print(f"✅ Логирование действий пользователя")
        
        # Демо логирования метрик производительности
        log_performance(
            'demo_operation',
            0.123,
            success=True,
            extra_data={'rows_processed': 100}
        )
        print(f"✅ Логирование метрик производительности")
        
        # Демо логирования исключений
        try:
            raise ValidationError(
                "Демо ошибка валидации",
                field="demo_field",
                value="invalid_value",
                context=create_error_context(
                    user_id='demo_user',
                    query='demo query'
                )
            )
        except ValidationError as e:
            log_exception(e, 'demo')
            print(f"✅ Логирование исключений с контекстом")
        
        return True
        
    except ImportError as e:
        print(f"❌ Система логирования недоступна: {e}")
        return False
    except Exception as e:
        print(f"❌ Ошибка демо логирования: {e}")
        return False


def demo_sql_validation():
    """Демонстрация продвинутой валидации SQL"""
    print("\n" + "="*60)
    print("🛡️  ДЕМО: Продвинутая валидация SQL")
    print("="*60)
    
    try:
        from advanced_sql_validator import validate_sql_query, ValidationResult
        
        # Тестовые SQL запросы разной сложности и безопасности
        test_queries = [
            {
                'name': 'Безопасный простой запрос',
                'sql': 'SELECT * FROM customers LIMIT 100',
                'expected': 'allowed'
            },
            {
                'name': 'Безопасный сложный запрос',
                'sql': 'SELECT c.name, AVG(o.amount) FROM customers c JOIN orders o ON c.id = o.customer_id GROUP BY c.name LIMIT 50',
                'expected': 'allowed'
            },
            {
                'name': 'Опасный запрос (DROP)',
                'sql': 'DROP TABLE customers',
                'expected': 'blocked'
            },
            {
                'name': 'SQL инъекция',
                'sql': "SELECT * FROM users WHERE id = 1 OR 1=1",
                'expected': 'blocked'
            },
            {
                'name': 'Слишком сложный запрос',
                'sql': 'SELECT * FROM t1 JOIN t2 ON t1.id = t2.id JOIN t3 ON t2.id = t3.id JOIN t4 ON t3.id = t4.id JOIN t5 ON t4.id = t5.id JOIN t6 ON t5.id = t6.id',
                'expected': 'blocked'
            },
            {
                'name': 'Запрос с предупреждениями',
                'sql': 'SELECT * FROM information_schema.tables',
                'expected': 'warning'
            }
        ]
        
        print(f"🧪 Тестируем {len(test_queries)} SQL запросов:")
        
        passed = 0
        for i, test in enumerate(test_queries, 1):
            print(f"\n{i}. {test['name']}")
            print(f"   SQL: {test['sql'][:60]}{'...' if len(test['sql']) > 60 else ''}")
            
            analysis = validate_sql_query(test['sql'])
            
            print(f"   Результат: {analysis.validation_result.value}")
            print(f"   Уровень риска: {analysis.risk_level.value}")
            print(f"   Сложность: {analysis.complexity_score}")
            
            if analysis.errors:
                print(f"   Ошибки: {len(analysis.errors)}")
                for error in analysis.errors[:2]:  # Показываем первые 2
                    print(f"     - {error}")
            
            if analysis.warnings:
                print(f"   Предупреждения: {len(analysis.warnings)}")
                for warning in analysis.warnings[:2]:  # Показываем первые 2
                    print(f"     - {warning}")
            
            if analysis.recommendations:
                print(f"   Рекомендации: {len(analysis.recommendations)}")
                for rec in analysis.recommendations[:1]:  # Показываем первую
                    print(f"     - {rec}")
            
            # Проверяем соответствие ожиданиям
            result_map = {
                ValidationResult.ALLOWED: 'allowed',
                ValidationResult.WARNING: 'warning',
                ValidationResult.BLOCKED: 'blocked'
            }
            
            actual = result_map.get(analysis.validation_result, 'unknown')
            if actual == test['expected'] or (actual == 'warning' and test['expected'] == 'allowed'):
                print(f"   ✅ Тест пройден")
                passed += 1
            else:
                print(f"   ❌ Тест не пройден (ожидали: {test['expected']}, получили: {actual})")
        
        print(f"\n📊 Результаты валидации: {passed}/{len(test_queries)} тестов пройдено")
        return passed == len(test_queries)
        
    except ImportError as e:
        print(f"❌ Система валидации SQL недоступна: {e}")
        return False
    except Exception as e:
        print(f"❌ Ошибка демо валидации SQL: {e}")
        return False


def demo_exception_handling():
    """Демонстрация улучшенной обработки ошибок"""
    print("\n" + "="*60)
    print("⚠️  ДЕМО: Обработка исключений")
    print("="*60)
    
    try:
        from exceptions import (
            ValidationError, SecurityError, SQLValidationError, ModelError,
            DatabaseError, PerformanceError, NetworkError,
            create_error_context, handle_exception
        )
        
        print(f"✅ Система исключений загружена")
        
        # Демо различных типов исключений
        exception_demos = [
            {
                'name': 'Ошибка валидации',
                'exception': ValidationError(
                    "Неверный формат email",
                    field="email",
                    value="invalid-email"
                )
            },
            {
                'name': 'Ошибка безопасности',
                'exception': SecurityError(
                    "Обнаружена SQL инъекция",
                    threat_type="sql_injection"
                )
            },
            {
                'name': 'Ошибка SQL валидации',
                'exception': SQLValidationError(
                    "Слишком сложный запрос",
                    sql_query="SELECT * FROM table1 JOIN table2 ..."
                )
            },
            {
                'name': 'Ошибка модели',
                'exception': ModelError(
                    "Превышен лимит токенов",
                    model_name="gpt-4"
                )
            }
        ]
        
        for demo in exception_demos:
            print(f"\n🔍 {demo['name']}:")
            exc = demo['exception']
            
            print(f"   Код ошибки: {exc.error_code}")
            print(f"   Категория: {exc.category.value}")
            print(f"   Серьезность: {exc.severity.value}")
            print(f"   Сообщение для пользователя: {exc.user_message}")
            print(f"   Рекомендации: {len(exc.recovery_suggestions)}")
            
            # Показываем структурированные данные
            error_dict = exc.to_dict()
            print(f"   Структурированные данные: {len(error_dict)} полей")
        
        # Демо обработки обычных исключений
        print(f"\n🔄 Обработка обычного исключения:")
        try:
            raise ValueError("Обычная ошибка Python")
        except ValueError as e:
            bi_exception = handle_exception(
                e,
                context=create_error_context(
                    user_id='demo_user',
                    query='demo query'
                )
            )
            print(f"   Преобразовано в: {type(bi_exception).__name__}")
            print(f"   Код ошибки: {bi_exception.error_code}")
            print(f"   Категория: {bi_exception.category.value}")
        
        return True
        
    except ImportError as e:
        print(f"❌ Система исключений недоступна: {e}")
        return False
    except Exception as e:
        print(f"❌ Ошибка демо исключений: {e}")
        return False


def main():
    """Основная функция демонстрации"""
    print("🚀 BI-GPT Agent - Демонстрация улучшенных функций")
    print("="*60)
    
    # Проверяем рабочую директорию
    current_dir = Path.cwd()
    print(f"📁 Рабочая директория: {current_dir}")
    
    # Проверяем наличие требуемых файлов
    required_files = ['config.py', 'exceptions.py', 'logging_config.py', 'advanced_sql_validator.py']
    missing_files = []
    for file in required_files:
        if not (current_dir / file).exists():
            missing_files.append(file)
    
    if missing_files:
        print(f"❌ Отсутствуют файлы: {', '.join(missing_files)}")
        print("Убедитесь, что вы находитесь в правильной директории")
        return 1
    
    print(f"✅ Все требуемые файлы найдены")
    
    # Запускаем демонстрации
    demos = [
        ('Система конфигурации', demo_config_system),
        ('Система логирования', demo_logging_system),
        ('Валидация SQL', demo_sql_validation),
        ('Обработка исключений', demo_exception_handling)
    ]
    
    results = []
    for name, demo_func in demos:
        try:
            success = demo_func()
            results.append((name, success))
        except Exception as e:
            print(f"❌ Критическая ошибка в демо '{name}': {e}")
            results.append((name, False))
    
    # Итоговый отчет
    print("\n" + "="*60)
    print("📊 ИТОГОВЫЙ ОТЧЕТ")
    print("="*60)
    
    passed = 0
    for name, success in results:
        status = "✅ ПРОЙДЕНО" if success else "❌ НЕ ПРОЙДЕНО"
        print(f"{status} - {name}")
        if success:
            passed += 1
    
    success_rate = (passed / len(results)) * 100
    print(f"\n🎯 Общий результат: {passed}/{len(results)} ({success_rate:.1f}%)")
    
    if success_rate >= 75:
        print("🏆 Отличный результат! Все основные системы работают корректно.")
    elif success_rate >= 50:
        print("⚠️  Хороший результат, но есть проблемы с некоторыми компонентами.")
    else:
        print("🚨 Множественные проблемы. Требуется дополнительная настройка.")
    
    print(f"\n💡 Для полной функциональности установите зависимости:")
    print(f"   pip install -r requirements.txt")
    
    return 0 if success_rate >= 75 else 1


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\n👋 Демонстрация прервана пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n💥 Критическая ошибка: {e}")
        sys.exit(1)
