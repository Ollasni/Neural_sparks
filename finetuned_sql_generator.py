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
            'клиент': 'customer',
            'пользователи': 'users',
            'пользователь': 'user',
            'товары': 'products',
            'продажи': 'sales',
            'склад': 'inventory',
            'сотрудники': 'employees',
            'цена': 'price',
            'количество': 'quantity'
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
    
    def __init__(self, model_path: str = "finetuning/phi3-mini", adapter_path: str = "finetuning/phi3_bird_lora", 
                 connection_string: str = None, use_dynamic_schema: bool = True):
        """
        Инициализация fine-tuned модели
        
        Args:
            model_path: Путь к базовой модели Phi-3
            adapter_path: Путь к LoRA адаптеру
            connection_string: Строка подключения к БД
            use_dynamic_schema: Использовать динамическую схему
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
        
        # Настройка динамической схемы
        self.use_dynamic_schema = use_dynamic_schema
        self.dynamic_schema_extractor = None
        
        if use_dynamic_schema:
            try:
                from dynamic_schema_extractor import create_dynamic_extractor
                self.dynamic_schema_extractor = create_dynamic_extractor(connection_string)
                print("   ✅ Dynamic schema extractor initialized")
            except ImportError as e:
                print(f"   ⚠️  Cannot import dynamic schema extractor: {e}")
                self.use_dynamic_schema = False
            except Exception as e:
                print(f"   ⚠️  Failed to initialize dynamic schema extractor: {e}")
                self.use_dynamic_schema = False
        
    def _load_model(self):
        """Загружает модель и адаптер"""
        try:
            # Проверяем доступность необходимых библиотек
            try:
                import torch
                print(f"   🔧 PyTorch версия: {torch.__version__}")
            except ImportError:
                raise ImportError("PyTorch не установлен. Установите: pip install torch")
            
            try:
                from peft import PeftModel
                print("   🔧 PEFT доступен")
            except ImportError:
                raise ImportError("PEFT не установлен. Установите: pip install peft")
            
            try:
                from transformers import AutoModelForCausalLM, AutoTokenizer
                print("   🔧 Transformers доступен")
            except ImportError:
                raise ImportError("Transformers не установлен. Установите: pip install transformers")
            
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
                    print(f"🔍 Начинаем генерацию с входными токенами длиной: {inputs['input_ids'].shape[1]}")
                    
                    outputs = self.model.generate(
                        inputs['input_ids'],
                        attention_mask=inputs.get('attention_mask'),
                        max_new_tokens=40,  # Еще меньше токенов для фокуса на коротком SQL
                        do_sample=False,  # Детерминированная генерация
                        pad_token_id=self.tokenizer.pad_token_id,
                        eos_token_id=self.tokenizer.eos_token_id,
                        use_cache=False,
                        # Убираем проблемные параметры
                        num_beams=1,  # Greedy search
                        repetition_penalty=1.05  # Минимальный penalty
                    )
                except Exception as cache_error:
                    print(f"⚠️  Ошибка с кэшем, пробуем без attention_mask: {cache_error}")
                    # Fallback без attention_mask
                    outputs = self.model.generate(
                        inputs['input_ids'],
                        max_new_tokens=40,  # Соответствует основной генерации
                        do_sample=False,
                        pad_token_id=self.tokenizer.pad_token_id,
                        eos_token_id=self.tokenizer.eos_token_id,
                        use_cache=False,
                        num_beams=1,
                        repetition_penalty=1.05
                    )
            
            # Декодируем только новые токены (без исходного промпта)
            input_length = inputs['input_ids'].shape[1]
            new_tokens = outputs[0][input_length:]
            generated_text = self.tokenizer.decode(new_tokens, skip_special_tokens=True)
            
            # Детальная отладочная информация
            print(f"🔍 Входных токенов: {input_length}")
            print(f"🔍 Выходных токенов: {len(outputs[0])}")
            print(f"🔍 Новых токенов: {len(new_tokens)}")
            print(f"📝 Новые токены (без промпта): '{generated_text}'")
            print(f"🔍 Длина сгенерированного текста: {len(generated_text)}")
            
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
            
            # Детальная отладка для понимания что генерирует модель
            print(f"🔍 Отладка: исходный сгенерированный текст (длина {len(sql_part)}): '{sql_part}'")
            
            # Более мягкие стоп-слова - сначала удаляем очевидные разделители
            primary_stop_words = ['\n\nQuestion', '\n\nDatabase', '\n\nSchema']
            for stop_word in primary_stop_words:
                if stop_word in sql_part:
                    sql_part = sql_part.split(stop_word)[0].strip()
                    print(f"🔍 После удаления '{stop_word}': '{sql_part}'")
                    break
            
            # Если есть переносы строк, ищем валидный SQL среди строк
            if '\n' in sql_part:
                lines = [line.strip() for line in sql_part.split('\n') if line.strip()]
                print(f"🔍 Найдены строки: {lines}")
                
                valid_commands = ['SELECT', 'DELETE', 'UPDATE', 'INSERT', 'WITH']
                
                # Ищем первую строку которая начинается с SQL команды
                sql_start_index = -1
                for i, line in enumerate(lines):
                    if any(line.upper().startswith(cmd) for cmd in valid_commands):
                        sql_start_index = i
                        print(f"🔍 Найдена SQL строка на позиции {i}: '{line}'")
                        break
                
                if sql_start_index >= 0:
                    # Склеиваем SQL строки начиная с найденной
                    sql_lines = []
                    for i in range(sql_start_index, len(lines)):
                        line = lines[i]
                        # Останавливаемся если встретили очевидно не SQL строку
                        if any(stop in line for stop in ['Question:', 'Database:', 'Schema:']):
                            break
                        sql_lines.append(line)
                    
                    sql_part = ' '.join(sql_lines)
                    print(f"🔍 Склеенный SQL: '{sql_part}'")
                else:
                    # Если не нашли очевидного SQL, берем первую непустую строку
                    sql_part = lines[0] if lines else sql_part
                    print(f"🔍 Взята первая строка: '{sql_part}'")
            
            # Убираем точку с запятой в конце
            if sql_part.endswith(';'):
                sql_part = sql_part[:-1]
                print(f"🔍 После удаления ';': '{sql_part}'")
            
            # Проверяем наличие SQL ключевых слов (более мягкая проверка)
            sql_keywords = ['SELECT', 'DELETE', 'UPDATE', 'INSERT', 'WITH', 'FROM', 'WHERE', 'ORDER', 'GROUP']
            has_sql_keywords = any(keyword.upper() in sql_part.upper() for keyword in sql_keywords)
            
            if not has_sql_keywords:
                print(f"⚠️  Текст не содержит SQL ключевых слов: {sql_part[:100]}...")
                
                # Попробуем найти что-то похожее на SQL в исходном тексте
                original_lines = [line.strip() for line in generated_text.split('\n') if line.strip()]
                for line in original_lines:
                    if any(keyword.upper() in line.upper() for keyword in sql_keywords):
                        print(f"🔍 Найдена альтернативная SQL строка: '{line}'")
                        sql_part = line
                        if sql_part.endswith(';'):
                            sql_part = sql_part[:-1]
                        break
                else:
                    return ""
            
            # Убираем очевидный мусор в начале/конце
            cleanup_patterns = [
                'Question:', 'SQL:', 'Database:', 'Schema:', 'Answer:', 'Explanation:',
                'Question', 'Database', 'Schema'
            ]
            
            for pattern in cleanup_patterns:
                if sql_part.startswith(pattern):
                    sql_part = sql_part[len(pattern):].strip()
                    print(f"🔍 После удаления префикса '{pattern}': '{sql_part}'")
            
            # Окончательная проверка на SQL команды
            valid_commands = ['SELECT', 'DELETE', 'UPDATE', 'INSERT', 'WITH']
            starts_with_valid_command = any(sql_part.upper().startswith(cmd) for cmd in valid_commands)
            
            if not starts_with_valid_command:
                # Последняя попытка - ищем команду в середине строки (но только как отдельное слово)
                found_cmd = False
                for cmd in valid_commands:
                    # Ищем команду как отдельное слово (с пробелами или началом/концом строки)
                    import re
                    pattern = r'\b' + re.escape(cmd.upper()) + r'\b'
                    match = re.search(pattern, sql_part.upper())
                    if match:
                        cmd_index = match.start()
                        sql_part = sql_part[cmd_index:]
                        print(f"🔍 Найдена команда '{cmd}' как отдельное слово в позиции {cmd_index}: '{sql_part}'")
                        found_cmd = True
                        break
                
                if not found_cmd:
                    print(f"⚠️  Финальный текст не начинается с SQL команды: '{sql_part[:100]}...'")
                    return ""
            
            # Добавляем LIMIT только для SELECT запросов
            if sql_part.upper().startswith('SELECT') and 'LIMIT' not in sql_part.upper():
                sql_part += ' LIMIT 1000'
            
            # Базовая валидация SQL на распространенные ошибки
            validation_error = self._validate_basic_sql(sql_part)
            if validation_error:
                print(f"⚠️  SQL валидация не прошла: {validation_error}")
                return ""
            
            print(f"✅ Извлеченный SQL: '{sql_part}'")
            return sql_part
            
        except Exception as e:
            print(f"❌ Ошибка извлечения SQL из сгенерированного текста: {e}")
            import traceback
            traceback.print_exc()
            return ""
    
    def _get_schema_for_prompt(self) -> str:
        """Получает схему для промпта (динамическую или статическую)"""
        if self.use_dynamic_schema and self.dynamic_schema_extractor:
            try:
                schema = self.dynamic_schema_extractor.get_schema()
                # Преобразуем в нужный формат для fine-tuned модели
                lines = []
                for table in schema.tables:
                    table_name = table.name.split('.')[-1] if '.' in table.name else table.name
                    columns_str = ", ".join([
                        f"{col.name} ({col.type})" if col.type else col.name
                        for col in table.columns
                    ])
                    lines.append(f"{table_name}: {columns_str}")
                return "\n".join(lines)
            except Exception as e:
                print(f"⚠️  Failed to get dynamic schema, falling back to static: {e}")
        
        # Fallback к статической схеме
        return """customers: id (SERIAL), name (VARCHAR), email (VARCHAR), registration_date (DATE), segment (VARCHAR)
products: id (SERIAL), name (VARCHAR), price (DECIMAL)
orders: id (SERIAL), user_id (INTEGER), product_id (INTEGER), quantity (INTEGER), created_at (TIMESTAMP)
users: id (SERIAL), name (VARCHAR), email (VARCHAR)
sales: id (SERIAL), order_id (INTEGER), product_id (INTEGER), quantity (INTEGER), revenue (DECIMAL), costs (DECIMAL)
inventory: id (SERIAL), product_id (INTEGER), current_stock (INTEGER), warehouse (VARCHAR)"""

    def _create_prompt(self, user_query: str) -> str:
        """Создает промпт для модели"""
        # Получаем актуальную схему
        schema = self._get_schema_for_prompt()
        
        # Улучшенный промпт с примерами для правильной генерации SQL
        prompt = f"""Database: bi_demo
Schema:
{schema.strip()}

Examples:
Question: покажи всех клиентов
SQL: SELECT * FROM customers LIMIT 1000

Question: клиенты с заказами
SQL: SELECT c.name, c.email FROM customers c INNER JOIN orders o ON c.id = o.customer_id LIMIT 1000

Question: {user_query}
SQL:"""
        
        print(f"🔍 Созданный промпт (длина {len(prompt)}):")
        print(f"'{prompt}'")
        print(f"🔍 Конец промпта")
        
        return prompt
    
    def _validate_basic_sql(self, sql: str) -> str:
        """Базовая валидация SQL для обнаружения распространенных ошибок"""
        try:
            sql_upper = sql.upper()
            
            # Проверяем неопределенные алиасы в SELECT запросах
            if sql_upper.startswith('SELECT'):
                import re
                
                # Ищем алиасы таблиц (TABLE AS ALIAS)
                alias_pattern = r'\b(\w+)\s+AS\s+(\w+)\b'
                aliases = {}
                for match in re.finditer(alias_pattern, sql_upper):
                    table_name = match.group(1)
                    alias_name = match.group(2)
                    aliases[alias_name] = table_name
                
                # Ищем использование алиасов в SELECT и других местах
                select_part = sql_upper.split('FROM')[0] if 'FROM' in sql_upper else sql_upper
                
                # Ищем паттерн ALIAS.COLUMN
                column_refs = re.findall(r'\b([A-Z]\d+)\.', sql_upper)
                
                for alias_ref in set(column_refs):
                    if alias_ref not in aliases:
                        return f"Неопределенный алиас '{alias_ref}' используется в запросе"
            
            # Другие базовые проверки можно добавить здесь
            
            return ""  # Нет ошибок
            
        except Exception as e:
            print(f"⚠️  Ошибка валидации SQL: {e}")
            return ""  # Пропускаем валидацию при ошибке
    
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
            
            # Проверяем что это валидный SQL (поддерживаем SELECT, DELETE, UPDATE, INSERT)
            valid_commands = ['SELECT', 'DELETE', 'UPDATE', 'INSERT']
            if not any(sql_part.upper().startswith(cmd) for cmd in valid_commands):
                print(f"⚠️  Сгенерированный текст не содержит валидную SQL команду: {sql_part[:100]}...")
                return ""
            
            # Проверяем что нет мусора
            invalid_keywords = ['Question', 'Database', 'Schema', 'Answer']
            for keyword in invalid_keywords:
                if keyword in sql_part:
                    print(f"⚠️  Обнаружен мусор в SQL: {keyword}")
                    return ""
            
            # Добавляем LIMIT только для SELECT запросов
            if sql_part.upper().startswith('SELECT') and 'LIMIT' not in sql_part.upper():
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
