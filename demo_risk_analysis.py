#!/usr/bin/env python3
"""
Демонстрация новых возможностей BI-GPT Agent:
- Отображение уровня риска для всех SQL запросов
- Настройка параметров модели (temperature, max_tokens)
- Расширенная валидация без блокировки запросов
"""

import sys
import os
sys.path.append(os.path.dirname(__file__))

from bi_gpt_agent import BIGPTAgent
from advanced_sql_validator import validate_sql_query, RiskLevel

def demo_risk_analysis():
    """Демонстрирует анализ риска для различных SQL запросов"""
    
    print("🔍 Демонстрация анализа риска SQL запросов")
    print("=" * 50)
    
    # Инициализируем агента
    try:
        agent = BIGPTAgent()
        print("✅ BI-GPT Agent инициализирован")
    except Exception as e:
        print(f"❌ Ошибка инициализации: {e}")
        return
    
    # Тестовые запросы с разным уровнем риска
    test_queries = [
        {
            "query": "покажи всех клиентов",
            "description": "Простой запрос - низкий риск"
        },
        {
            "query": "покажи прибыль за последние 2 дня с группировкой по категориям",
            "description": "Сложный запрос с JOIN - средний риск"
        },
        {
            "query": "удали всех клиентов без заказов",
            "description": "DELETE запрос - высокий риск"
        },
        {
            "query": "DROP TABLE customers",
            "description": "Критический запрос - должен быть заблокирован"
        }
    ]
    
    for i, test_case in enumerate(test_queries, 1):
        print(f"\n📝 Тест {i}: {test_case['description']}")
        print(f"Запрос: {test_case['query']}")
        print("-" * 30)
        
        try:
            # Генерируем SQL
            result = agent.process_query(test_case['query'])
            
            if 'error' in result:
                print(f"❌ Ошибка: {result['error']}")
            else:
                print(f"✅ SQL сгенерирован: {result['sql']}")
            
            # Анализируем риск
            if result.get('risk_analysis'):
                analysis = result['risk_analysis']
                
                # Получаем иконку и цвет
                risk_icon = "❓"
                risk_color = "#6c757d"
                
                if analysis.risk_level == RiskLevel.LOW:
                    risk_icon = "✅"
                    risk_color = "#28a745"
                elif analysis.risk_level == RiskLevel.MEDIUM:
                    risk_icon = "⚠️"
                    risk_color = "#ffc107"
                elif analysis.risk_level == RiskLevel.HIGH:
                    risk_icon = "🔶"
                    risk_color = "#fd7e14"
                elif analysis.risk_level == RiskLevel.CRITICAL:
                    risk_icon = "🚨"
                    risk_color = "#dc3545"
                
                print(f"{risk_icon} Уровень риска: {analysis.risk_level.value.upper()}")
                print(f"📊 Сложность: {analysis.complexity_score}")
                print(f"🔗 JOIN'ов: {analysis.join_count}")
                print(f"📋 Подзапросов: {analysis.subquery_count}")
                
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
            else:
                print("ℹ️ Анализ риска недоступен")
                
        except Exception as e:
            print(f"❌ Ошибка обработки: {e}")
        
        print()

def demo_model_parameters():
    """Демонстрирует влияние параметров модели на генерацию"""
    
    print("\n🎛️ Демонстрация параметров модели")
    print("=" * 50)
    
    try:
        agent = BIGPTAgent()
        
        test_query = "покажи топ клиентов по выручке"
        
        # Разные настройки параметров
        parameter_sets = [
            {"temperature": 0.0, "max_tokens": 200, "name": "Precise"},
            {"temperature": 0.3, "max_tokens": 400, "name": "Balanced"},
            {"temperature": 0.7, "max_tokens": 600, "name": "Creative"},
            {"temperature": 0.1, "max_tokens": 800, "name": "Complex"}
        ]
        
        for params in parameter_sets:
            print(f"\n🔧 {params['name']} (T={params['temperature']}, Tokens={params['max_tokens']})")
            print("-" * 40)
            
            try:
                result = agent.process_query(
                    test_query, 
                    temperature=params['temperature'],
                    max_tokens=params['max_tokens']
                )
                
                if 'error' in result:
                    print(f"❌ Ошибка: {result['error']}")
                else:
                    print(f"✅ SQL: {result['sql']}")
                    
                    if result.get('risk_analysis'):
                        analysis = result['risk_analysis']
                        print(f"📊 Сложность: {analysis.complexity_score}")
                        print(f"🎯 Риск: {analysis.risk_level.value}")
                
            except Exception as e:
                print(f"❌ Ошибка: {e}")
                
    except Exception as e:
        print(f"❌ Ошибка инициализации агента: {e}")

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

if __name__ == "__main__":
    main()

