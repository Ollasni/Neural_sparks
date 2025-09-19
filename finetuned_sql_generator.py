"""
Fine-tuned SQL Generator для прямого использования Phi-3 + LoRA модели
Без API сервера, работает напрямую с моделью
"""

import os
import time
import torch
from typing import Tuple, Dict, List
from pathlib import Path
from transformers import AutoModelForCausalLM, AutoTokenizer
from peft import PeftModel

# Импортируем BusinessDictionary для совместимости
class BusinessDictionary:
    """Упрощенный бизнес-словарь для совместимости"""
    
    def __init__(self):
        self.terms = {
            # Финансовые метрики
            'прибыль': 'revenue - costs',
            'маржинальность': '(revenue - costs) / revenue * 100',
            'средний чек': 'AVG(order_amount)',
            'выручка': 'SUM(revenue)',
            'остатки': 'current_stock',
            'оборот': 'SUM(turnover)',
            'рентабельность': '(profit / revenue) * 100',
            
            # Временные периоды
            'сегодня': 'DATE(created_at) = CURRENT_DATE',
            'вчера': 'DATE(created_at) = CURRENT_DATE - 1',
            'за неделю': 'created_at >= CURRENT_DATE - INTERVAL \'7 days\'',
            'за месяц': 'created_at >= CURRENT_DATE - INTERVAL \'30 days\'',
            'за квартал': 'created_at >= CURRENT_DATE - INTERVAL \'90 days\'',
            'за год': 'created_at >= CURRENT_DATE - INTERVAL \'365 days\'',
            
            # Таблицы и поля
            'заказы': 'orders',
            'клиенты': 'customers', 
            'товары': 'products',
            'продажи': 'sales',
            'склад': 'inventory',
            'сотрудники': 'employees'
        }
        
    def translate_term(self, term: str) -> str:
        """Переводит бизнес-термин в SQL конструкцию"""
        term_lower = term.lower().strip()
        return self.terms.get(term_lower, term)
    
    def get_related_terms(self, query: str) -> List[str]:
        """Находит связанные бизнес-термины в запросе"""
        found_terms = []
        query_lower = query.lower()
        for term in self.terms.keys():
            if term in query_lower:
                found_terms.append(term)
        return found_terms


class FineTunedSQLGenerator:
    """Генератор SQL запросов с использованием fine-tuned Phi-3 + LoRA модели"""
    
    def __init__(self, model_path: str = "finetuning/phi3-mini", adapter_path: str = "finetuning/phi3_bird_lora"):
        """
        Инициализация fine-tuned модели
        
        Args:
            model_path: Путь к базовой модели Phi-3
            adapter_path: Путь к LoRA адаптеру
        """
        self.model_path = Path(model_path)
        self.adapter_path = Path(adapter_path)
        
        # Проверяем наличие модели и адаптера
        if not self.model_path.exists():
            raise FileNotFoundError(f"Базовая модель не найдена: {self.model_path}")
        
        if not self.adapter_path.exists():
            raise FileNotFoundError(f"LoRA адаптер не найден: {self.adapter_path}")
        
        print(f"🔧 Загружаем fine-tuned модель...")
        print(f"   Базовая модель: {self.model_path}")
        print(f"   LoRA адаптер: {self.adapter_path}")
        
        self._load_model()
        
        # Добавляем business_dict для совместимости с BIGPTAgent
        self.business_dict = BusinessDictionary()
        
    def _load_model(self):
        """Загружает модель и адаптер"""
        try:
            # Загружаем токенизатор
            print("   📝 Загружаем токенизатор...")
            self.tokenizer = AutoTokenizer.from_pretrained(str(self.model_path), use_fast=True)
            
            if self.tokenizer.pad_token is None:
                self.tokenizer.pad_token = self.tokenizer.eos_token
            
            # Загружаем базовую модель
            print("   🧠 Загружаем базовую модель...")
            self.model = AutoModelForCausalLM.from_pretrained(
                str(self.model_path),
                torch_dtype=torch.float16 if torch.backends.mps.is_available() else torch.float32,
                device_map="auto",
                trust_remote_code=True,
                attn_implementation="eager"  # Используем eager attention для лучшей совместимости
            )
            
            # Загружаем LoRA адаптер
            print("   🔗 Подключаем LoRA адаптер...")
            self.model = PeftModel.from_pretrained(self.model, str(self.adapter_path))
            
            # Переводим в режим инференса
            self.model.eval()
            
            print("   ✅ Модель успешно загружена!")
            
        except Exception as e:
            print(f"   ❌ Ошибка загрузки модели: {e}")
            raise
    
    def generate_sql(self, user_query: str, schema_info: Dict = None) -> Tuple[str, float]:
        """
        Генерирует SQL запрос из естественного языка
        
        Args:
            user_query: Запрос пользователя на русском языке
            schema_info: Информация о схеме БД (не используется в данной версии)
            
        Returns:
            Tuple[str, float]: (SQL запрос, время выполнения)
        """
        start_time = time.time()
        
        # Создаем промпт как в обучении
        prompt = self._create_prompt(user_query)
        
        try:
            # Токенизируем промпт
            inputs = self.tokenizer(
                prompt, 
                return_tensors="pt", 
                truncation=True, 
                max_length=1024
            )
            
            # Перемещаем на устройство модели
            device = next(self.model.parameters()).device
            inputs = {k: v.to(device) for k, v in inputs.items()}
            
            # Генерируем ответ с улучшенными параметрами
            with torch.no_grad():
                try:
                    outputs = self.model.generate(
                        inputs['input_ids'],
                        attention_mask=inputs.get('attention_mask'),
                        max_new_tokens=80,  # Уменьшили для предотвращения галлюцинаций
                        do_sample=False,  # Детерминированная генерация
                        pad_token_id=self.tokenizer.pad_token_id,
                        eos_token_id=self.tokenizer.eos_token_id,
                        use_cache=False,
                        # Добавляем stop tokens для остановки генерации
                        early_stopping=True
                    )
                except Exception as cache_error:
                    print(f"⚠️  Ошибка с кэшем, пробуем без attention_mask: {cache_error}")
                    # Fallback без attention_mask
                    outputs = self.model.generate(
                        inputs['input_ids'],
                        max_new_tokens=80,  # Уменьшили
                        do_sample=False,
                        pad_token_id=self.tokenizer.pad_token_id,
                        eos_token_id=self.tokenizer.eos_token_id,
                        use_cache=False,
                        early_stopping=True
                    )
            
            # Декодируем только новые токены (без исходного промпта)
            input_length = inputs['input_ids'].shape[1]
            new_tokens = outputs[0][input_length:]
            generated_text = self.tokenizer.decode(new_tokens, skip_special_tokens=True)
            
            # Отладочная информация
            print(f"📝 Новые токены (без промпта): {generated_text}")
            
            # Извлекаем только SQL из ответа (теперь без исходного промпта)
            sql_query = self._extract_sql_from_generated(generated_text)
            
            execution_time = time.time() - start_time
            
            if sql_query:
                print(f"✅ Извлеченный SQL: {sql_query}")
            else:
                print("❌ SQL не удалось извлечь")
            
            return sql_query, execution_time
            
        except Exception as e:
            print(f"❌ Ошибка генерации SQL: {e}")
            import traceback
            traceback.print_exc()  # Печатаем полный стектрейс для отладки
            return "", time.time() - start_time
    
    def _extract_sql_from_generated(self, generated_text: str) -> str:
        """Извлекает SQL из уже очищенного сгенерированного текста (без промпта)"""
        try:
            sql_part = generated_text.strip()
            
            # Останавливаемся на стоп-словах (модель может продолжить генерировать примеры)
            stop_words = [
                'Question:', 'SQL:', 'Database:', 'Schema:', 'Answer:', 'Explanation:', 
                '\n\nQuestion', '\n\nDatabase', '\n\nSchema', '\nQuestion', '\nDatabase',
                'Question', 'Database'  # Даже без двоеточия
            ]
            
            for stop_word in stop_words:
                if stop_word in sql_part:
                    sql_part = sql_part.split(stop_word)[0].strip()
                    break
            
            # Берем только первую строку если есть переносы
            if '\n' in sql_part:
                lines = sql_part.split('\n')
                # Ищем первую строку которая выглядит как SQL
                for line in lines:
                    line = line.strip()
                    if line and line.upper().startswith('SELECT'):
                        sql_part = line
                        break
                else:
                    sql_part = lines[0].strip()
            
            # Убираем точку с запятой в конце
            if sql_part.endswith(';'):
                sql_part = sql_part[:-1]
            
            # Проверяем что это валидный SQL
            if not sql_part.upper().startswith('SELECT'):
                print(f"⚠️  Сгенерированный текст не содержит SELECT: {sql_part[:50]}...")
                return ""
            
            # Проверяем что нет мусора
            invalid_keywords = ['Question', 'Database', 'Schema', 'Answer', 'Explanation']
            for keyword in invalid_keywords:
                if keyword in sql_part:
                    print(f"⚠️  Обнаружен мусор в SQL: {keyword}")
                    return ""
            
            # Добавляем LIMIT если его нет
            if 'LIMIT' not in sql_part.upper():
                sql_part += ' LIMIT 1000'
            
            return sql_part
            
        except Exception as e:
            print(f"❌ Ошибка извлечения SQL из сгенерированного текста: {e}")
            return ""
    
    def _create_prompt(self, user_query: str) -> str:
        """Создает промпт для модели"""
        # Используем PostgreSQL схему
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
    
    def _extract_sql(self, generated_text: str, original_prompt: str) -> str:
        """Извлекает SQL из сгенерированного текста"""
        try:
            # Убираем исходный промпт
            if original_prompt in generated_text:
                sql_part = generated_text.split(original_prompt, 1)[1].strip()
            else:
                sql_part = generated_text.strip()
            
            # Ищем SQL после "SQL:" - модель обучена именно на таком формате
            if 'SQL:' in sql_part:
                # Берем только первое вхождение SQL:
                sql_part = sql_part.split('SQL:', 1)[1].strip()
            
            # Останавливаемся на стоп-словах (включая повторные примеры)
            stop_words = [
                'Question:', 'SQL:', 'Database:', 'Schema:', 'Answer:', 'Explanation:', 
                '\n\n', '\nQuestion', '\nDatabase', '\nSchema', 'Question', 'Database'
            ]
            
            for stop_word in stop_words:
                if stop_word in sql_part:
                    sql_part = sql_part.split(stop_word)[0].strip()
                    break  # Останавливаемся на первом найденном
            
            # Очищаем от лишних символов и пробелов
            sql_part = sql_part.strip()
            
            # Убираем точку с запятой в конце
            if sql_part.endswith(';'):
                sql_part = sql_part[:-1]
            
            # Берем только первую строку если есть перенос
            if '\n' in sql_part:
                sql_part = sql_part.split('\n')[0].strip()
            
            # Проверяем что это валидный SQL
            if not sql_part.upper().startswith('SELECT'):
                print(f"⚠️  Сгенерированный текст не содержит SELECT: {sql_part[:100]}...")
                return ""
            
            # Проверяем что нет мусора
            invalid_keywords = ['Question', 'Database', 'Schema', 'Answer']
            for keyword in invalid_keywords:
                if keyword in sql_part:
                    print(f"⚠️  Обнаружен мусор в SQL: {keyword}")
                    return ""
            
            # Добавляем LIMIT если его нет
            if 'LIMIT' not in sql_part.upper():
                sql_part += ' LIMIT 1000'
            
            return sql_part
            
        except Exception as e:
            print(f"❌ Ошибка извлечения SQL: {e}")
            return ""
    
    def cleanup(self):
        """Очищает ресурсы модели"""
        if hasattr(self, 'model'):
            del self.model
        if hasattr(self, 'tokenizer'):
            del self.tokenizer
        
        # Очищаем кэш GPU
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
        elif torch.backends.mps.is_available():
            torch.mps.empty_cache()


def test_finetuned_generator():
    """Тестирует fine-tuned генератор"""
    print("🧪 Тестируем fine-tuned SQL генератор...")
    
    try:
        generator = FineTunedSQLGenerator()
        
        test_queries = [
            "покажи всех клиентов",
            "количество заказов",
            "средний чек клиентов",
            "топ 3 клиента по выручке"
        ]
        
        for query in test_queries:
            print(f"\n📝 Запрос: {query}")
            sql, exec_time = generator.generate_sql(query)
            print(f"⏱️  Время: {exec_time:.2f}с")
            print(f"🔍 SQL: {sql}")
        
        generator.cleanup()
        print("\n✅ Тест завершен успешно!")
        
    except Exception as e:
        print(f"❌ Ошибка теста: {e}")


if __name__ == "__main__":
    test_finetuned_generator()
