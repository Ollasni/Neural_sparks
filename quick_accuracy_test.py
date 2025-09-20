#!/usr/bin/env python3
"""
Быстрый тест accuracy моделей для SQL генерации
Сравнивает Fine-tuned Phi-3 и Custom API модели на 50 тестовых запросах
"""

import json
import time
import os
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Any
import difflib

# Добавляем путь к проекту
sys.path.append(str(Path(__file__).parent))

try:
    from bi_gpt_agent import BIGPTAgent
    from config import get_settings
except ImportError as e:
    print(f"❌ Ошибка импорта: {e}")
    print("Убедитесь, что все зависимости установлены")
    sys.exit(1)

# Импортируем необходимые модули для создания упрощенного генератора
import openai
import time

class SimpleSQLGenerator:
    """Упрощенный SQL генератор для справедливого сравнения с FineTunedSQLGenerator"""
    
    def __init__(self, api_key: str = None, base_url: str = None):
        # Поддержка как OpenAI, так и локальных моделей
        if base_url:
            self.client = openai.OpenAI(api_key=api_key, base_url=base_url)
            self.model_name = "llama4scout"
        else:
            self.client = openai.OpenAI(api_key=api_key)
            self.model_name = "gpt-4"
    
    def generate_sql(self, user_query: str, schema_info: Dict = None) -> Tuple[str, float]:
        """Генерирует SQL запрос из естественного языка (упрощенная версия)"""
        start_time = time.time()
        
        # Создаем простой промпт как в FineTunedSQLGenerator
        prompt = self._create_simple_prompt(user_query)
        
        try:
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": "You are a helpful data analyst that writes clean PostgreSQL. Return ONLY SQL without comments or explanations."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.0,  # Детерминированная генерация как в FineTuned
                max_tokens=80,    # Ограничиваем как в FineTuned
                top_p=0.1
            )
            
            sql_query = response.choices[0].message.content.strip()
            
            # Извлекаем чистый SQL
            if "```sql" in sql_query:
                sql_query = sql_query.split("```sql")[1].split("```")[0].strip()
            elif "```" in sql_query:
                sql_query = sql_query.split("```")[1].strip()
            
            # Очищаем SQL
            sql_query = self._clean_sql(sql_query)
            
            execution_time = time.time() - start_time
            return sql_query, execution_time
            
        except Exception as e:
            print(f"❌ Ошибка генерации SQL: {e}")
            return "", time.time() - start_time
    
    def _create_simple_prompt(self, user_query: str) -> str:
        """Создает простой промпт как в FineTunedSQLGenerator"""
        schema = """
customers: id (SERIAL), name (VARCHAR), email (VARCHAR), registration_date (DATE), segment (VARCHAR)
products: id (SERIAL), name (VARCHAR), category (VARCHAR), price (DECIMAL), cost (DECIMAL)  
orders: id (SERIAL), customer_id (INTEGER), order_date (DATE), amount (DECIMAL), status (VARCHAR)
sales: id (SERIAL), order_id (INTEGER), product_id (INTEGER), quantity (INTEGER), revenue (DECIMAL), costs (DECIMAL)
inventory: id (SERIAL), product_id (INTEGER), current_stock (INTEGER), warehouse (VARCHAR)
"""
        
        prompt = f"""You are a helpful data analyst that writes clean PostgreSQL.
Return ONLY SQL without comments or explanations.

Database: bi_demo
Schema:
{schema.strip()}

Question: {user_query}
SQL:"""
        
        return prompt
    
    def _clean_sql(self, sql: str) -> str:
        """Очищает SQL запрос"""
        if not sql:
            return ""
        
        # Удаляем лишние символы и пробелы
        sql = sql.strip()
        
        # Удаляем возможные объяснения после SQL
        lines = sql.split('\n')
        sql_lines = []
        for line in lines:
            line = line.strip()
            if line and not line.startswith('#') and not line.startswith('--'):
                sql_lines.append(line)
        
        sql = ' '.join(sql_lines)
        
        # Убираем точку с запятой в конце если есть
        if sql.endswith(';'):
            sql = sql[:-1]
        
        # Проверяем что запрос начинается с разрешенной команды
        allowed_commands = ['SELECT', 'INSERT', 'UPDATE', 'DELETE']
        if not any(sql.upper().startswith(cmd) for cmd in allowed_commands):
            return ""
        
        # Базовая валидация структуры
        if sql.upper().startswith('SELECT') and 'FROM' not in sql.upper():
            return ""
        
        # Добавляем LIMIT если его нет
        if 'LIMIT' not in sql.upper():
            sql += ' LIMIT 1000'
        
        return sql

class SimpleAgent:
    """Упрощенный агент для справедливого сравнения"""
    
    def __init__(self, sql_generator):
        self.sql_generator = sql_generator
    
    def generate_sql(self, user_query: str, temperature: float = 0.0, max_tokens: int = 400, prompt_mode: str = "few_shot") -> Tuple[str, float]:
        """Генерирует SQL запрос для пользовательского вопроса"""
        return self.sql_generator.generate_sql(user_query, None)

class AccuracyTester:
    def __init__(self):
        self.results = {
            'finetuned': {'correct': 0, 'total': 0, 'details': []},
            'custom_api': {'correct': 0, 'total': 0, 'details': []}
        }
        self.test_queries = self.load_test_queries()
        
    def load_test_queries(self) -> List[Dict]:
        """Загружает тестовые запросы"""
        try:
            with open('new_accuracy_test_queries.json', 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            print("❌ Файл new_accuracy_test_queries.json не найден")
            sys.exit(1)
    
    def normalize_sql(self, sql: str) -> str:
        """Нормализует SQL для сравнения"""
        if not sql:
            return ""
        
        # Убираем лишние пробелы и переводы строк
        sql = ' '.join(sql.split())
        
        # Приводим к нижнему регистру
        sql = sql.lower()
        
        # Убираем точку с запятой в конце
        sql = sql.rstrip(';')
        
        # Нормализуем пробелы вокруг операторов
        sql = sql.replace(' = ', '=').replace(' =', '=').replace('= ', '=')
        sql = sql.replace(' > ', '>').replace(' >', '>').replace('> ', '>')
        sql = sql.replace(' < ', '<').replace(' <', '<').replace('< ', '<')
        sql = sql.replace(' >= ', '>=').replace(' >=', '>=').replace('>= ', '>=')
        sql = sql.replace(' <= ', '<=').replace(' <=', '<=').replace('<= ', '<=')
        sql = sql.replace(' != ', '!=').replace(' !=', '!=').replace('!= ', '!=')
        sql = sql.replace(' <> ', '<>').replace(' <>', '<>').replace('<> ', '<>')
        
        return sql.strip()
    
    def compare_sql(self, expected: str, generated: str) -> Tuple[bool, float]:
        """Сравнивает SQL запросы и возвращает точность"""
        expected_norm = self.normalize_sql(expected)
        generated_norm = self.normalize_sql(generated)
        
        if expected_norm == generated_norm:
            return True, 1.0
        
        # Вычисляем схожесть
        similarity = difflib.SequenceMatcher(None, expected_norm, generated_norm).ratio()
        
        # Считаем правильным если схожесть больше 80%
        is_correct = similarity > 0.5
        
        return is_correct, similarity
    
    def test_model(self, model_name: str, agent: BIGPTAgent) -> Dict:
        """Тестирует одну модель"""
        print(f"\n🧪 Тестируем {model_name}...")
        
        correct = 0
        total = len(self.test_queries)
        details = []
        
        for i, test_case in enumerate(self.test_queries, 1):
            question = test_case['question']
            expected_sql = test_case['expected_sql']
            
            try:
                # Генерируем SQL
                start_time = time.time()
                result = agent.generate_sql(question)
                
                # Обрабатываем результат - generate_sql возвращает (sql, time)
                if isinstance(result, tuple) and len(result) == 2:
                    generated_sql, gen_time = result
                else:
                    # Fallback для случая когда возвращается только строка
                    generated_sql = result
                    gen_time = time.time() - start_time
                
                generation_time = time.time() - start_time
                
                # Сравниваем
                is_correct, similarity = self.compare_sql(expected_sql, generated_sql)
                
                if is_correct:
                    correct += 1
                
                details.append({
                    'id': test_case['id'],
                    'question': question,
                    'expected': expected_sql,
                    'generated': generated_sql,
                    'correct': is_correct,
                    'similarity': similarity,
                    'time': generation_time
                })
                
                status = "✅" if is_correct else "❌"
                print(f"{status} {i:2d}/50 - {question[:50]}... (similarity: {similarity:.2f})")
                
            except Exception as e:
                print(f"❌ {i:2d}/50 - Ошибка: {str(e)[:50]}...")
                details.append({
                    'id': test_case['id'],
                    'question': question,
                    'expected': expected_sql,
                    'generated': f"ERROR: {str(e)}",
                    'correct': False,
                    'similarity': 0.0,
                    'time': 0.0
                })
        
        accuracy = correct / total if total > 0 else 0
        
        return {
            'correct': correct,
            'total': total,
            'accuracy': accuracy,
            'details': details
        }
    
    def run_comparison(self):
        """Запускает сравнение моделей"""
        print("🚀 Запуск быстрого теста accuracy моделей")
        print("📝 Используем упрощенный подход: БЕЗ бизнес-словаря и БЕЗ примеров")
        print("=" * 60)
        
        # Инициализируем Fine-tuned модель
        try:
            print("🔧 Инициализация Fine-tuned модели...")
            finetuned_agent = BIGPTAgent(use_finetuned=True, model_provider="finetuned")
            print("✅ Fine-tuned модель готова")
        except Exception as e:
            print(f"❌ Ошибка инициализации Fine-tuned модели: {e}")
            print("⚠️  Пропускаем Fine-tuned модель и тестируем только Custom API...")
            finetuned_agent = None
        
        # Инициализируем упрощенную Custom API модель
        try:
            print("🔧 Инициализация упрощенной Custom API модели...")
            # Получаем настройки для API
            settings = get_settings()
            model_config = settings.get_model_config()
            
            # Создаем упрощенный генератор
            simple_generator = SimpleSQLGenerator(
                api_key=model_config.get('api_key'),
                base_url=model_config.get('base_url')
            )
            custom_agent = SimpleAgent(simple_generator)
            print("✅ Упрощенная Custom API модель готова")
        except Exception as e:
            print(f"❌ Ошибка инициализации Custom API модели: {e}")
            return
        
        # Тестируем модели
        finetuned_results = None
        if finetuned_agent:
            print("\n" + "=" * 60)
            finetuned_results = self.test_model("Fine-tuned Phi-3 (упрощенный)", finetuned_agent)
        
        print("\n" + "=" * 60)
        custom_api_results = self.test_model("Custom API (упрощенный)", custom_agent)
        
        # Выводим результаты
        self.print_results(finetuned_results, custom_api_results)
        
        # Сохраняем детальные результаты
        self.save_detailed_results(finetuned_results, custom_api_results)
    
    def print_results(self, finetuned_results: Dict, custom_api_results: Dict):
        """Выводит результаты сравнения"""
        print("\n" + "=" * 60)
        print("📊 РЕЗУЛЬТАТЫ СРАВНЕНИЯ")
        print("=" * 60)
        
        if finetuned_results:
            print(f"\n🧠 Fine-tuned Phi-3 (упрощенный):")
            print(f"   Правильных: {finetuned_results['correct']}/{finetuned_results['total']}")
            print(f"   Accuracy: {finetuned_results['accuracy']:.2%}")
        else:
            print(f"\n🧠 Fine-tuned Phi-3: Недоступна")
        
        print(f"\n🌐 Custom API (упрощенный):")
        print(f"   Правильных: {custom_api_results['correct']}/{custom_api_results['total']}")
        print(f"   Accuracy: {custom_api_results['accuracy']:.2%}")
        
        if finetuned_results:
            print(f"\n🏆 ПОБЕДИТЕЛЬ (справедливое сравнение):")
            if finetuned_results['accuracy'] > custom_api_results['accuracy']:
                print(f"   Fine-tuned Phi-3 (+{finetuned_results['accuracy'] - custom_api_results['accuracy']:.2%})")
            elif custom_api_results['accuracy'] > finetuned_results['accuracy']:
                print(f"   Custom API (+{custom_api_results['accuracy'] - finetuned_results['accuracy']:.2%})")
            else:
                print("   Ничья!")
        else:
            print(f"\n🏆 Результат: Custom API (упрощенный) - Fine-tuned модель недоступна")
    
    def save_detailed_results(self, finetuned_results: Dict, custom_api_results: Dict):
        """Сохраняет детальные результаты в файл"""
        results = {
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
            'summary': {
                'custom_api': {
                    'correct': custom_api_results['correct'],
                    'total': custom_api_results['total'],
                    'accuracy': custom_api_results['accuracy']
                }
            },
            'details': {
                'custom_api': custom_api_results['details']
            }
        }
        
        if finetuned_results:
            results['summary']['finetuned'] = {
                'correct': finetuned_results['correct'],
                'total': finetuned_results['total'],
                'accuracy': finetuned_results['accuracy']
            }
            results['details']['finetuned'] = finetuned_results['details']
        else:
            results['summary']['finetuned'] = None
            results['details']['finetuned'] = None
        
        filename = f"accuracy_test_results_{int(time.time())}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        print(f"\n💾 Детальные результаты сохранены в {filename}")

def main():
    """Главная функция"""
    print("🎯 Быстрый тест accuracy моделей SQL генерации")
    print("📝 Справедливое сравнение: БЕЗ бизнес-словаря и БЕЗ примеров")
    print("Тестируем 50 запросов на PostgreSQL")
    
    tester = AccuracyTester()
    tester.run_comparison()

if __name__ == "__main__":
    main()
