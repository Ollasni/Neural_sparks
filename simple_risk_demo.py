#!/usr/bin/env python3
"""
Упрощенная демонстрация анализа риска SQL запросов
Работает без внешних зависимостей
"""

import sys
import os
sys.path.append(os.path.dirname(__file__))

def demo_risk_analysis():
    """Демонстрирует анализ риска для различных SQL запросов"""
    
    print("🔍 Демонстрация анализа риска SQL запросов")
    print("=" * 50)
    
    try:
        from advanced_sql_validator import validate_sql_query, RiskLevel
        print("✅ Модуль анализа риска загружен")
    except Exception as e:
        print(f"❌ Ошибка загрузки модуля: {e}")
        return
    
    # Тестовые SQL запросы с разным уровнем риска
    test_queries = [
        {
            "sql": "SELECT * FROM customers LIMIT 100",
            "description": "Простой SELECT - низкий риск"
        },
        {
            "sql": "SELECT c.name, SUM(s.revenue) FROM customers c JOIN sales s ON c.id = s.customer_id GROUP BY c.id",
            "description": "Сложный запрос с JOIN - средний риск"
        },
        {
            "sql": "DELETE FROM customers WHERE id = 1",
            "description": "DELETE запрос - высокий риск"
        },
        {
            "sql": "DROP TABLE customers",
            "description": "Критический запрос - должен быть заблокирован"
        },
        {
            "sql": "SELECT * FROM customers WHERE name = 'test' OR 1=1",
            "description": "SQL инъекция - критический риск"
        }
    ]
    
    for i, test_case in enumerate(test_queries, 1):
        print(f"\n📝 Тест {i}: {test_case['description']}")
        print(f"SQL: {test_case['sql']}")
        print("-" * 30)
        
        try:
            # Анализируем риск
            analysis = validate_sql_query(test_case['sql'])
            
            # Получаем иконку и цвет
            risk_icon = "❓"
            risk_color = "#6c757d"
            risk_text = "Неизвестно"
            
            if analysis.risk_level == RiskLevel.LOW:
                risk_icon = "✅"
                risk_color = "#28a745"
                risk_text = "Низкий риск"
            elif analysis.risk_level == RiskLevel.MEDIUM:
                risk_icon = "⚠️"
                risk_color = "#ffc107"
                risk_text = "Средний риск"
            elif analysis.risk_level == RiskLevel.HIGH:
                risk_icon = "🔶"
                risk_color = "#fd7e14"
                risk_text = "Высокий риск"
            elif analysis.risk_level == RiskLevel.CRITICAL:
                risk_icon = "🚨"
                risk_color = "#dc3545"
                risk_text = "Критический риск"
            
            print(f"{risk_icon} Уровень риска: {risk_text}")
            print(f"📊 Сложность: {analysis.complexity_score}")
            print(f"🔗 JOIN'ов: {analysis.join_count}")
            print(f"📋 Подзапросов: {analysis.subquery_count}")
            print(f"🎯 Результат: {analysis.validation_result.value}")
            
            if analysis.warnings:
                print("⚠️ Предупреждения:")
                for warning in analysis.warnings[:3]:
                    print(f"  • {warning}")
            
            if analysis.errors:
                print("❌ Ошибки:")
                for error in analysis.errors[:3]:
                    print(f"  • {error}")
            
            if analysis.recommendations:
                print("💡 Рекомендации:")
                for rec in analysis.recommendations[:3]:
                    print(f"  • {rec}")
                    
        except Exception as e:
            print(f"❌ Ошибка анализа: {e}")
        
        print()

def demo_model_parameters():
    """Демонстрирует различные параметры модели"""
    
    print("\n🎛️ Демонстрация параметров модели")
    print("=" * 50)
    
    print("📋 Доступные параметры:")
    print("• Temperature (0.0 - 2.0):")
    print("  - 0.0 = Детерминированная генерация")
    print("  - 0.3 = Сбалансированная генерация")
    print("  - 0.7 = Креативная генерация")
    print("  - 1.0+ = Очень случайная генерация")
    
    print("\n• Max Tokens (50 - 1000):")
    print("  - 200 = Короткие запросы")
    print("  - 400 = Стандартные запросы")
    print("  - 600 = Сложные запросы")
    print("  - 800+ = Очень сложные запросы")
    
    print("\n🔧 Быстрые настройки:")
    print("• 🎯 Precise (0.0, 200) - для точных запросов")
    print("• ⚖️ Balanced (0.3, 400) - для обычных запросов")
    print("• 🎨 Creative (0.7, 600) - для креативных решений")
    print("• 🚀 Complex (0.1, 800) - для сложных запросов")

def main():
    """Основная функция демонстрации"""
    print("🚀 BI-GPT Agent - Демонстрация новых возможностей")
    print("=" * 60)
    
    # Демонстрация анализа риска
    demo_risk_analysis()
    
    # Демонстрация параметров модели
    demo_model_parameters()
    
    print("\n✅ Демонстрация завершена!")
    print("\n💡 Теперь запустите Streamlit для интерактивного интерфейса:")
    print("   streamlit run streamlit_app.py")
    print("\n📋 Новые возможности:")
    print("• 🔍 Анализ риска для всех SQL запросов")
    print("• 🎛️ Настройка параметров модели (temperature, max_tokens)")
    print("• ⚠️ Предупреждения вместо блокировки запросов")
    print("• 📊 Детальная статистика сложности")
    print("• 💡 Рекомендации по улучшению")

if __name__ == "__main__":
    main()

