#!/usr/bin/env python3
"""
Тестовый скрипт для проверки динамического извлечения схемы
"""

import os
import sys
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_dynamic_schema_extractor():
    """Тестирует динамический экстрактор схемы"""
    print("🧪 Тестируем динамический экстрактор схемы...")
    
    try:
        from dynamic_schema_extractor import create_dynamic_extractor
        
        # Используем строку подключения по умолчанию
        connection_string = "postgresql://olgasnissarenko@localhost:5432/bi_demo"
        
        print(f"📊 Подключение к БД: {connection_string}")
        extractor = create_dynamic_extractor(connection_string, cache_ttl=60)
        
        # Получаем схему
        schema = extractor.get_schema()
        
        print(f"✅ Схема успешно извлечена!")
        print(f"   Тип БД: {schema.database_type}")
        print(f"   Количество таблиц: {len(schema.tables)}")
        print(f"   Общее количество колонок: {sum(len(table.columns) for table in schema.tables)}")
        print(f"   Внешние ключи: {len(schema.foreign_keys)}")
        
        print(f"\n📋 Схема в формате промпта:")
        print(schema.to_prompt_format())
        
        print(f"\n🏷️ Таблицы и колонки:")
        for table in schema.tables:
            print(f"  {table.name}:")
            for col in table.columns[:3]:  # Показываем только первые 3 колонки
                print(f"    - {col.name} ({col.type}) {'PK' if col.primary_key else ''} {'FK' if col.foreign_key else ''}")
            if len(table.columns) > 3:
                print(f"    ... и еще {len(table.columns) - 3} колонок")
        
        # Тестируем кэширование
        print(f"\n🔄 Тестируем кэширование...")
        schema2 = extractor.get_schema()  # Должно использовать кэш
        if schema2 == schema:
            print("✅ Кэширование работает корректно")
        
        # Сохраняем в файл
        output_file = "test_dynamic_schema.json"
        extractor.save_schema_to_file(output_file)
        print(f"💾 Схема сохранена в файл: {output_file}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка тестирования экстрактора: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_dynamic_sql_generation():
    """Тестирует генерацию SQL с динамической схемой"""
    print("\n🧪 Тестируем генерацию SQL с динамической схемой...")
    
    try:
        from bi_gpt_agent import SQLGenerator
        
        # Инициализируем генератор с динамической схемой (без API ключей)
        # Просто тестируем создание промптов
        connection_string = "postgresql://olgasnissarenko@localhost:5432/bi_demo"
        
        print("🔧 Создаем SQLGenerator с динамической схемой...")
        generator = SQLGenerator(
            api_key="test",  # Фиктивный ключ для теста
            connection_string=connection_string,
            use_dynamic_schema=True
        )
        
        print("✅ SQLGenerator создан успешно")
        
        # Тестируем получение схемы для промпта
        schema_str = generator._get_schema_for_prompt()
        print(f"📋 Схема для промпта получена:")
        print(schema_str[:200] + "..." if len(schema_str) > 200 else schema_str)
        
        # Тестируем создание промптов
        test_query = "покажи всех клиентов"
        few_shot_prompt = generator._create_few_shot_prompt(schema_str)
        one_shot_prompt = generator._create_one_shot_prompt(schema_str)
        
        print(f"\n📝 Few-shot промпт создан (размер: {len(few_shot_prompt)} символов)")
        print(f"📝 One-shot промпт создан (размер: {len(one_shot_prompt)} символов)")
        
        print("✅ Тестирование создания промптов прошло успешно")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка тестирования генератора SQL: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_finetuned_dynamic_schema():
    """Тестирует fine-tuned генератор с динамической схемой"""
    print("\n🧪 Тестируем fine-tuned генератор с динамической схемой...")
    
    try:
        # Проверяем доступность fine-tuned модели
        model_path = "finetuning/phi3-mini"
        adapter_path = "finetuning/phi3_bird_lora"
        
        if not os.path.exists(model_path):
            print(f"⚠️  Fine-tuned модель не найдена: {model_path}")
            print("   Пропускаем тест fine-tuned генератора")
            return True
        
        from finetuned_sql_generator import FineTunedSQLGenerator
        
        connection_string = "postgresql://olgasnissarenko@localhost:5432/bi_demo"
        
        print("🔧 Создаем FineTunedSQLGenerator с динамической схемой...")
        
        # Инициализируем без загрузки модели (только схему)
        generator = FineTunedSQLGenerator(
            model_path=model_path,
            adapter_path=adapter_path,
            connection_string=connection_string,
            use_dynamic_schema=True
        )
        
        print("✅ FineTunedSQLGenerator создан успешно")
        
        # Тестируем получение схемы для промпта
        schema_str = generator._get_schema_for_prompt()
        print(f"📋 Схема для промпта получена:")
        print(schema_str[:200] + "..." if len(schema_str) > 200 else schema_str)
        
        # Тестируем создание промпта
        test_query = "покажи всех клиентов"
        prompt = generator._create_prompt(test_query)
        
        print(f"\n📝 Промпт создан (размер: {len(prompt)} символов)")
        print("✅ Тестирование fine-tuned генератора прошло успешно")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка тестирования fine-tuned генератора: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Основная функция тестирования"""
    print("🚀 Запускаем тесты динамической схемы...")
    
    tests = [
        ("Динамический экстрактор схемы", test_dynamic_schema_extractor),
        ("Генерация SQL с динамической схемой", test_dynamic_sql_generation),
        ("Fine-tuned генератор с динамической схемой", test_finetuned_dynamic_schema),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n{'='*60}")
        print(f"Тест: {test_name}")
        print('='*60)
        
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ Критическая ошибка в тесте '{test_name}': {e}")
            results.append((test_name, False))
    
    # Подводим итоги
    print(f"\n{'='*60}")
    print("ИТОГИ ТЕСТИРОВАНИЯ")
    print('='*60)
    
    passed = 0
    for test_name, result in results:
        status = "✅ ПРОЙДЕН" if result else "❌ ПРОВАЛЕН"
        print(f"{status}: {test_name}")
        if result:
            passed += 1
    
    print(f"\nРезультат: {passed}/{len(results)} тестов пройдено")
    
    if passed == len(results):
        print("🎉 Все тесты прошли успешно!")
        return 0
    else:
        print("⚠️  Некоторые тесты провалились")
        return 1


if __name__ == "__main__":
    sys.exit(main())
