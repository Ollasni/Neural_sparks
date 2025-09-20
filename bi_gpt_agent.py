"""
BI-GPT Agent: Natural Language to SQL converter for corporate BI
"""

import os
import re
import psycopg2
from psycopg2.extras import RealDictCursor
import hashlib
import time
import logging
import argparse
import uuid
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from datetime import datetime, timedelta
import json

# Загружаем переменные окружения из .env файла
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv не установлен

import openai
# Langchain imports removed - not used in current implementation
import pandas as pd
from sqlalchemy import create_engine, text, MetaData, inspect
from pydantic import BaseModel, Field
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

# Импорт новых систем (с обработкой ошибок импорта)
try:
    from config import get_settings, validate_config
    from exceptions import (
        BIGPTException, ValidationError, SecurityError, SQLValidationError,
        ModelError, DatabaseError, PerformanceError, NetworkError,
        create_error_context, handle_exception
    )
    from logging_config import get_logger, setup_logging, log_exception, log_performance, log_user_action
    from advanced_sql_validator import validate_sql_query, ValidationResult
    
    # Настройка логирования
    setup_logging()
    logger = get_logger(__name__)
    
    ENHANCED_FEATURES_AVAILABLE = True
except ImportError as e:
    # Fallback на стандартное логирование если новые модули недоступны
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.warning(f"Enhanced features not available: {e}")
    
    ENHANCED_FEATURES_AVAILABLE = False


@dataclass
class QueryMetrics:
    """Метрики для отслеживания качества запросов"""
    execution_time: float
    sql_accuracy: bool
    has_errors: bool
    pii_detected: bool
    business_terms_used: int
    aggregation_accuracy: float
    
    # Новые поля для улучшенной аналитики
    request_id: str = ""
    validation_result: str = "unknown"
    risk_level: str = "unknown"
    complexity_score: int = 0
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    timestamp: Optional[datetime] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
        if not self.request_id:
            self.request_id = str(uuid.uuid4())[:8]
    
    
class BusinessDictionary:
    """Бизнес-словарь для маппинга терминов"""
    
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
            'за неделю': 'created_at >= CURRENT_DATE - INTERVAL 7 DAY',
            'за месяц': 'created_at >= CURRENT_DATE - INTERVAL 30 DAY',
            'за квартал': 'created_at >= CURRENT_DATE - INTERVAL 90 DAY',
            'за год': 'created_at >= CURRENT_DATE - INTERVAL 365 DAY',
            
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


class SecurityValidator:
    """Валидатор безопасности SQL запросов"""
    
    def __init__(self):
        self.dangerous_keywords = [
            'DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE',
            'TRUNCATE', 'EXEC', 'EXECUTE', 'sp_', 'xp_'
        ]
        
        self.pii_patterns = [
            r'\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b',  # Credit card
            r'\b\d{3}-\d{2}-\d{4}\b',  # SSN
            r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',  # Email
            r'\b\+?1?[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b'  # Phone
        ]
    
    def validate_sql(self, sql: str) -> Tuple[bool, List[str]]:
        """Проверяет SQL на безопасность"""
        errors = []
        sql_upper = sql.upper()
        
        # Проверка на опасные команды
        for keyword in self.dangerous_keywords:
            if keyword in sql_upper:
                errors.append(f"Обнаружена опасная команда: {keyword}")
        
        # Проверка на SQL инъекции
        if "'" in sql and ("OR" in sql_upper or "UNION" in sql_upper):
            errors.append("Возможная SQL инъекция")
            
        # Ограничение сложности
        if sql.count('JOIN') > 5:
            errors.append("Слишком сложный запрос (много JOIN)")
            
        return len(errors) == 0, errors
    
    def detect_pii(self, text: str) -> bool:
        """Обнаруживает персональные данные"""
        for pattern in self.pii_patterns:
            if re.search(pattern, text):
                return True
        return False


class SQLGenerator:
    """Генератор SQL запросов из естественного языка"""
    
    def __init__(self, api_key: str = None, base_url: str = None):
        # Поддержка как OpenAI, так и локальных моделей
        if base_url:
            # Локальная модель (например, Llama-4-Scout)
            self.client = openai.OpenAI(
                api_key=api_key,
                base_url=base_url
            )
            self.model_name = "llama4scout"
        else:
            # OpenAI GPT-4
            self.client = openai.OpenAI(api_key=api_key)
            self.model_name = "gpt-4"
            
        self.business_dict = BusinessDictionary()
        self.security = SecurityValidator()
        self.logger = logger  # Добавляем logger для совместимости
        
        # Few-shot промпт с примерами (сложный)
        self.sql_prompt_few_shot = """
Ты эксперт по PostgreSQL SQL. Переведи запрос на русском языке в точный PostgreSQL SQL запрос.

СХЕМА БАЗЫ ДАННЫХ (PostgreSQL):
orders: id, customer_id, order_date, amount, status
customers: id, name, email, registration_date, segment  
products: id, name, category, price, cost
sales: id, order_id, product_id, quantity, revenue, costs
inventory: id, product_id, current_stock, warehouse

БИЗНЕС-ТЕРМИНЫ:
{business_terms}

ПРИМЕРЫ SELECT:
Запрос: "покажи всех клиентов"
SQL: SELECT * FROM customers LIMIT 1000;

Запрос: "прибыль за последние 2 дня"
SQL: SELECT SUM(revenue - costs) as profit FROM sales s JOIN orders o ON s.order_id = o.id WHERE o.order_date >= CURRENT_DATE - INTERVAL '2 days' LIMIT 1000;

Запрос: "средний чек клиентов"
SQL: SELECT AVG(amount) as avg_check FROM orders LIMIT 1000;

Запрос: "остатки товаров на складе"
SQL: SELECT p.name, i.current_stock, i.warehouse FROM inventory i JOIN products p ON i.product_id = p.id LIMIT 1000;

Запрос: "количество заказов"
SQL: SELECT COUNT(*) as order_count FROM orders LIMIT 1000;

Запрос: "топ 3 клиента по выручке"
SQL: SELECT c.name, SUM(s.revenue) as total_revenue FROM customers c JOIN orders o ON c.id = o.customer_id JOIN sales s ON o.id = s.order_id GROUP BY c.id, c.name ORDER BY total_revenue DESC LIMIT 3;

Запрос: "средняя маржинальность по категориям"
SQL: SELECT p.category, AVG((s.revenue - s.costs) / s.revenue * 100) as avg_margin FROM products p JOIN sales s ON p.id = s.product_id GROUP BY p.category LIMIT 1000;

Запрос: "заказы за сегодня"
SQL: SELECT o.id, o.amount, c.name FROM orders o JOIN customers c ON o.customer_id = c.id WHERE DATE(o.order_date) = CURRENT_DATE LIMIT 1000;

Запрос: "клиенты премиум сегмента"
SQL: SELECT name, email, registration_date FROM customers WHERE segment = 'Premium' LIMIT 1000;

Запрос: "товары с низкими остатками"
SQL: SELECT p.name, p.category, i.current_stock FROM products p JOIN inventory i ON p.id = i.product_id WHERE i.current_stock < 10 LIMIT 1000;

ПРИМЕРЫ INSERT:
Запрос: "добавь нового клиента Иванов Иван"
SQL: INSERT INTO customers (name, email, segment) VALUES ('Иванов Иван', 'ivan@example.com', 'Standard');

Запрос: "создай заказ на 1000 рублей для клиента 1"
SQL: INSERT INTO orders (customer_id, amount, status) VALUES (1, 1000, 'pending');

ПРИМЕРЫ UPDATE:
Запрос: "обнови статус заказа 1 на выполнен"
SQL: UPDATE orders SET status = 'completed' WHERE id = 1;

Запрос: "увеличь цену всех товаров на 10%"
SQL: UPDATE products SET price = price * 1.1;

ПРИМЕРЫ DELETE:
Запрос: "удали заказ с id 1"
SQL: DELETE FROM orders WHERE id = 1;

Запрос: "удали всех клиентов без заказов"
SQL: DELETE FROM customers WHERE id NOT IN (SELECT DISTINCT customer_id FROM orders);

Запрос: "покажи топ 10 товаров по выручке"
SQL: SELECT product_id, SUM(revenue) as total_revenue FROM sales GROUP BY product_id ORDER BY total_revenue DESC LIMIT 10;

ПРАВИЛА POSTGRESQL:
1. Разрешены SELECT, INSERT, UPDATE, DELETE запросы
2. Для SELECT обязательно LIMIT 1000
3. Для UPDATE и DELETE всегда используй WHERE клаузулу
4. Используй правильные JOIN между таблицами
5. Для дат используй PostgreSQL функции: CURRENT_DATE, CURRENT_TIMESTAMP, INTERVAL
6. Точные имена полей из схемы PostgreSQL
7. В ORDER BY всегда указывай полное имя колонки (например, T1.name, а не T1.)
8. Проверяй синтаксис ORDER BY - каждая ссылка должна иметь имя колонки
9. Используй PostgreSQL синтаксис для дат: CURRENT_DATE - INTERVAL 'N days'
10. Для строк используй одинарные кавычки, для идентификаторов - двойные
11. НЕ используй EXPLAIN, DESCRIBE, SHOW или другие диагностические команды
12. НЕ используй SELECT TOP (используй LIMIT)
13. Верни только PostgreSQL SQL код без объяснений

ЗАПРОС: {user_query}
SQL:"""

        # One-shot промпт без примеров (простой)
        self.sql_prompt_one_shot = """
Ты эксперт по PostgreSQL SQL. Переведи запрос на русском языке в точный PostgreSQL SQL запрос.

СХЕМА БАЗЫ ДАННЫХ (PostgreSQL):
orders: id, customer_id, order_date, amount, status
customers: id, name, email, registration_date, segment  
products: id, name, category, price, cost
sales: id, order_id, product_id, quantity, revenue, costs
inventory: id, product_id, current_stock, warehouse

БИЗНЕС-ТЕРМИНЫ:
{business_terms}

ПРАВИЛА POSTGRESQL:
1. Разрешены SELECT, INSERT, UPDATE, DELETE запросы
2. Для SELECT обязательно LIMIT 1000
3. Для UPDATE и DELETE всегда используй WHERE клаузулу
4. Используй правильные JOIN между таблицами
5. Для дат используй PostgreSQL функции: CURRENT_DATE, CURRENT_TIMESTAMP, INTERVAL
6. Используй PostgreSQL синтаксис для дат: CURRENT_DATE - INTERVAL 'N days'
7. Для строк используй одинарные кавычки, для идентификаторов - двойные
8. НЕ используй EXPLAIN, DESCRIBE, SHOW или другие диагностические команды
9. НЕ используй SELECT TOP (используй LIMIT)
10. Верни только PostgreSQL SQL код без объяснений

ЗАПРОС: {user_query}
SQL:"""

    def generate_sql(self, user_query: str, temperature: float = 0.0, max_tokens: int = 400, prompt_mode: str = "few_shot") -> Tuple[str, float]:
        """Генерирует SQL запрос из естественного языка"""
        start_time = time.time()
        
        # Подготовка бизнес-терминов для промпта
        related_terms = self.business_dict.get_related_terms(user_query)
        business_terms_str = "\n".join([
            f"- {term}: {self.business_dict.translate_term(term)}" 
            for term in related_terms
        ])
        
        # Выбираем промпт в зависимости от режима
        if prompt_mode == "one_shot":
            selected_prompt = self.sql_prompt_one_shot
        else:  # few_shot по умолчанию
            selected_prompt = self.sql_prompt_few_shot
        
        try:
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": "Ты эксперт по PostgreSQL SQL. Отвечай только валидным PostgreSQL SQL кодом без объяснений."},
                    {"role": "user", "content": selected_prompt.format(
                        business_terms=business_terms_str,
                        user_query=user_query
                    )}
                ],
                temperature=temperature,  # Настраиваемая температура
                max_tokens=max_tokens,   # Настраиваемое количество токенов
                top_p=0.1        # Более детерминированные ответы
            )
            
            sql_query = response.choices[0].message.content.strip()
            
            # Извлекаем чистый SQL
            if "```sql" in sql_query:
                sql_query = sql_query.split("```sql")[1].split("```")[0].strip()
            elif "```" in sql_query:
                sql_query = sql_query.split("```")[1].strip()
            
            # Дополнительная очистка и валидация
            sql_query = self._clean_and_validate_sql(sql_query)
                
            execution_time = time.time() - start_time
            return sql_query, execution_time
            
        except Exception as e:
            logger.error(f"Ошибка генерации SQL: {e}")
            return "", time.time() - start_time
    
    def _clean_and_validate_sql(self, sql: str) -> str:
        """Очищает и валидирует SQL запрос"""
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
        
        # Удаляем нежелательные префиксы
        sql = self._remove_unwanted_prefixes(sql)
        
        # Убираем точку с запятой в конце если есть
        if sql.endswith(';'):
            sql = sql[:-1]
        
        # Проверяем что запрос начинается с разрешенной команды
        allowed_commands = ['SELECT', 'INSERT', 'UPDATE', 'DELETE']
        if not any(sql.upper().startswith(cmd) for cmd in allowed_commands):
            raise ValueError(f"Запрос должен начинаться с одной из команд: {', '.join(allowed_commands)}")
        
        # Базовая валидация структуры
        if sql.upper().startswith('SELECT') and 'FROM' not in sql.upper():
            raise ValueError("SELECT запрос должен содержать FROM")
        
        # Исправляем неполные ORDER BY клаузулы
        sql = self._fix_order_by_clause(sql)
        
        # Добавляем LIMIT если его нет
        if 'LIMIT' not in sql.upper():
            sql += ' LIMIT 1000'
        
        return sql
    
    def _remove_unwanted_prefixes(self, sql_query: str) -> str:
        """Удаляет нежелательные префиксы из SQL запроса"""
        # Список нежелательных префиксов (в порядке от длинных к коротким)
        unwanted_prefixes = [
            'EXPLAIN QUERY PLAN ',
            'WITH RECURSIVE ',
            'EXPLAIN ',
            'DESCRIBE ',
            'DESC ',
            'SHOW ',
            'WITH ',
        ]
        
        # Проверяем и удаляем префиксы
        original_query = sql_query
        for prefix in unwanted_prefixes:
            if sql_query.upper().startswith(prefix.upper()):
                sql_query = sql_query[len(prefix):].strip()
                logger.info(f"Удален префикс '{prefix}' из SQL: {original_query[:50]}...")
                break
        
        # Специальная обработка для SELECT TOP (SQL Server синтаксис)
        if re.match(r'^SELECT\s+TOP\s+\d+\s+', sql_query, re.IGNORECASE):
            # Заменяем SELECT TOP N на SELECT с LIMIT
            match = re.match(r'^SELECT\s+TOP\s+(\d+)\s+(.*)', sql_query, re.IGNORECASE | re.DOTALL)
            if match:
                limit_num = match.group(1)
                rest_query = match.group(2)
                sql_query = f"SELECT {rest_query} LIMIT {limit_num}"
                logger.info(f"Заменен SELECT TOP {limit_num} на SELECT ... LIMIT {limit_num}")
        
        return sql_query
    
    def _fix_order_by_clause(self, sql: str) -> str:
        """Исправляет неполные ORDER BY клаузулы"""
        import re
        
        # Ищем ORDER BY клаузулы с неполными ссылками на колонки
        order_by_pattern = r'ORDER\s+BY\s+([^LIMIT]+?)(?=\s+LIMIT|\s*$)'
        match = re.search(order_by_pattern, sql, re.IGNORECASE | re.DOTALL)
        
        if match:
            order_by_part = match.group(1).strip()
            
            # Проверяем на неполные ссылки типа "T1." без указания колонки
            incomplete_refs = re.findall(r'\b\w+\.\s*(?=\s*[,LIMIT]|\s*$)', order_by_part)
            
            if incomplete_refs:
                # Удаляем неполные ссылки
                for incomplete_ref in incomplete_refs:
                    # Удаляем неполную ссылку и запятую перед ней если есть
                    order_by_part = re.sub(rf'\s*{re.escape(incomplete_ref)}\s*,?\s*', '', order_by_part)
                    order_by_part = re.sub(r',\s*$', '', order_by_part)  # Убираем лишние запятые
                
                # Если ORDER BY стал пустым, удаляем всю клаузулу
                if not order_by_part.strip():
                    sql = re.sub(r'\s*ORDER\s+BY\s+[^LIMIT]+?(?=\s+LIMIT|\s*$)', '', sql, flags=re.IGNORECASE | re.DOTALL)
                else:
                    # Заменяем исправленную ORDER BY клаузулу
                    sql = re.sub(
                        r'ORDER\s+BY\s+[^LIMIT]+?(?=\s+LIMIT|\s*$)',
                        f'ORDER BY {order_by_part.strip()}',
                        sql,
                        flags=re.IGNORECASE | re.DOTALL
                    )
        
        return sql


class BIGPTAgent:
    """Основной класс BI-GPT агента"""
    
    def __init__(self, api_key: str = None, base_url: str = None, use_finetuned: bool = False, model_provider: str = None):
        # Улучшенная инициализация с новыми системами
        if ENHANCED_FEATURES_AVAILABLE:
            try:
                self.settings = get_settings()
                self.logger = get_logger('bi_gpt_agent')
                
                # Используем URL базы данных из настроек
                self.db_url = self.settings.database_url if self.settings else f"postgresql://olgasnissarenko@localhost:5432/bi_demo"
                
                # Валидация конфигурации
                try:
                    config_errors = validate_config()
                    if config_errors and hasattr(self.settings, 'is_production') and self.settings.is_production:
                        raise ValueError(f"Configuration validation failed: {'; '.join(config_errors)}")
                except Exception as e:
                    self.logger.warning(f"Configuration validation skipped: {e}")
                    
                self.logger.info("BI-GPT Agent initializing with enhanced features")
                
            except Exception as e:
                # Fallback если новые системы не работают
                logger.warning(f"Enhanced initialization failed, using legacy mode: {e}")
                self.db_url = f"postgresql://olgasnissarenko@localhost:5432/bi_demo"
                self.settings = None
                self.logger = logger
        else:
            self.db_url = f"postgresql://olgasnissarenko@localhost:5432/bi_demo"
            self.settings = None
            self.logger = logger
        
        # Инициализация генератора SQL с поддержкой выбора модели
        self.use_finetuned = use_finetuned
        self.model_provider = model_provider
        
        # Определяем провайдера модели
        if model_provider:
            # Если явно указан провайдер, используем его
            provider = model_provider
            print(f"🔧 Используем явно указанный провайдер: {provider}")
        elif ENHANCED_FEATURES_AVAILABLE and self.settings:
            model_config = self.settings.get_model_config()
            provider = model_config.get('provider', 'openai')
            print(f"🔧 Используем провайдер из настроек: {provider}")
        else:
            provider = 'openai' if not use_finetuned else 'finetuned'
            print(f"🔧 Используем провайдер по умолчанию: {provider}")
        
        if provider == 'finetuned' or use_finetuned:
            # Используем fine-tuned модель напрямую
            try:
                from finetuned_sql_generator import FineTunedSQLGenerator
                if ENHANCED_FEATURES_AVAILABLE and self.settings:
                    model_config = self.settings.get_model_config()
                    self.sql_generator = FineTunedSQLGenerator(
                        model_path=model_config.get('model_path', 'finetuning/phi3-mini'),
                        adapter_path=model_config.get('adapter_path', 'finetuning/phi3_bird_lora')
                    )
                else:
                    self.sql_generator = FineTunedSQLGenerator()
                print("✅ Используется fine-tuned модель Phi-3 + LoRA")
                self.use_finetuned = True
            except Exception as e:
                print(f"❌ Ошибка загрузки fine-tuned модели: {e}")
                print("⚠️  Переключаемся на API модель...")
                if base_url:
                    self.sql_generator = SQLGenerator(api_key, base_url)
                    print(f"✅ Используется пользовательская API модель: {base_url}")
                else:
                    print("❌ Не удалось определить API модель для fallback")
                    raise Exception("Fine-tuned модель недоступна и нет настроек для API модели")
                self.use_finetuned = False
        elif provider == 'openai':
            # Используем OpenAI GPT-4
            self.sql_generator = SQLGenerator(api_key or os.getenv("OPENAI_API_KEY"))
            print("✅ Используется OpenAI GPT-4")
        else:
            # Используем пользовательскую API модель
            if ENHANCED_FEATURES_AVAILABLE and self.settings:
                # Получаем конфигурацию из настроек (включая env переменные)
                try:
                    model_config = self.settings.get_model_config()
                    api_key = model_config.get('api_key')
                    base_url = model_config.get('base_url')
                    
                    self.sql_generator = SQLGenerator(api_key, base_url)
                    print(f"✅ Используется пользовательская API модель: {base_url}")
                except Exception as e:
                    print(f"❌ Ошибка получения конфигурации: {e}")
                    # Fallback к переменным окружения
                    import os
                    env_base_url = os.getenv("LOCAL_BASE_URL")
                    env_api_key = os.getenv("LOCAL_API_KEY")
                    
                    if env_base_url:
                        self.sql_generator = SQLGenerator(env_api_key, env_base_url)
                        print(f"✅ Используется пользовательская API модель из env: {env_base_url}")
                    else:
                        raise Exception("Не удалось получить конфигурацию для пользовательской API модели")
            elif base_url:
                # Fallback для случаев без настроек
                self.sql_generator = SQLGenerator(api_key, base_url)
                print(f"✅ Используется пользовательская API модель: {base_url}")
            else:
                # Последняя попытка - переменные окружения
                import os
                env_base_url = os.getenv("LOCAL_BASE_URL")
                env_api_key = os.getenv("LOCAL_API_KEY")
                
                if env_base_url:
                    self.sql_generator = SQLGenerator(env_api_key, env_base_url)
                    print(f"✅ Используется пользовательская API модель из env: {env_base_url}")
                else:
                    print("❌ Не указан base_url для пользовательской API модели")
                    raise Exception("Для пользовательской API модели требуется base_url")
            
        self.security = SecurityValidator()
        self.metrics_history = []
        
        # Инициализация базы данных
        self._init_demo_database()
        
        if hasattr(self, 'logger'):
            self.logger.info(f"BI-GPT Agent initialized successfully", extra={
                'database_url': self.db_url,
                'enhanced_features': ENHANCED_FEATURES_AVAILABLE,
                'use_finetuned': self.use_finetuned
            })
        
    def _init_demo_database(self):
        """Создает демо базу данных PostgreSQL с тестовыми данными"""
        try:
            # Подключаемся к PostgreSQL
            conn = psycopg2.connect(self.db_url)
            cursor = conn.cursor()
            
            # Создание таблиц (PostgreSQL синтаксис)
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255) UNIQUE,
                registration_date DATE,
                segment VARCHAR(50)
            );
            """)
            
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                category VARCHAR(100),
                price DECIMAL(10,2),
                cost DECIMAL(10,2)
            );
            """)
            
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id SERIAL PRIMARY KEY,
                customer_id INTEGER REFERENCES customers(id),
                order_date DATE,
                amount DECIMAL(10,2),
                status VARCHAR(50)
            );
            """)
            
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS sales (
                id SERIAL PRIMARY KEY,
                order_id INTEGER REFERENCES orders(id),
                product_id INTEGER REFERENCES products(id),
                quantity INTEGER,
                revenue DECIMAL(10,2),
                costs DECIMAL(10,2)
            );
            """)
            
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS inventory (
                id SERIAL PRIMARY KEY,
                product_id INTEGER REFERENCES products(id),
                current_stock INTEGER,
                warehouse VARCHAR(100)
            );
            """)
            
            # Вставка тестовых данных (PostgreSQL синтаксис)
            cursor.execute("""
            INSERT INTO customers (id, name, email, registration_date, segment) VALUES 
            (1, 'Иван Иванов', 'ivan@email.com', '2023-01-15', 'Premium'),
            (2, 'Мария Петрова', 'maria@email.com', '2023-02-20', 'Standard'),
            (3, 'Алексей Сидоров', 'alex@email.com', '2023-03-10', 'Premium')
            ON CONFLICT (id) DO NOTHING;
            """)
            
            cursor.execute("""
            INSERT INTO products (id, name, category, price, cost) VALUES
            (1, 'Ноутбук ASUS', 'Электроника', 50000, 35000),
            (2, 'Мышь Logitech', 'Электроника', 2000, 1200),
            (3, 'Клавиатура', 'Электроника', 3000, 2000)
            ON CONFLICT (id) DO NOTHING;
            """)
            
            cursor.execute("""
            INSERT INTO orders (id, customer_id, order_date, amount, status) VALUES
            (1, 1, '2024-09-15', 52000, 'completed'),
            (2, 2, '2024-09-14', 5000, 'completed'),
            (3, 3, '2024-09-13', 50000, 'pending')
            ON CONFLICT (id) DO NOTHING;
            """)
            
            cursor.execute("""
            INSERT INTO sales (id, order_id, product_id, quantity, revenue, costs) VALUES
            (1, 1, 1, 1, 50000, 35000),
            (2, 1, 2, 1, 2000, 1200),
            (3, 2, 2, 1, 2000, 1200),
            (4, 2, 3, 1, 3000, 2000),
            (5, 3, 1, 1, 50000, 35000)
            ON CONFLICT (id) DO NOTHING;
            """)
            
            cursor.execute("""
            INSERT INTO inventory (id, product_id, current_stock, warehouse) VALUES
            (1, 1, 10, 'Москва'),
            (2, 2, 50, 'Москва'),
            (3, 3, 30, 'СПб')
            ON CONFLICT (id) DO NOTHING;
            """)
            
            # Обновляем последовательности SERIAL после вставки
            cursor.execute("SELECT setval('customers_id_seq', (SELECT MAX(id) FROM customers));")
            cursor.execute("SELECT setval('products_id_seq', (SELECT MAX(id) FROM products));")
            cursor.execute("SELECT setval('orders_id_seq', (SELECT MAX(id) FROM orders));")
            cursor.execute("SELECT setval('sales_id_seq', (SELECT MAX(id) FROM sales));")
            cursor.execute("SELECT setval('inventory_id_seq', (SELECT MAX(id) FROM inventory));")
            
            conn.commit()
            cursor.close()
            conn.close()
            
            print("✅ PostgreSQL демо база данных инициализирована")
            
        except psycopg2.Error as e:
            print(f"❌ Ошибка подключения к PostgreSQL: {e}")
            print("💡 Убедитесь, что PostgreSQL запущен и доступен по адресу:", self.db_url)
            raise Exception(f"Не удалось подключиться к PostgreSQL: {e}")
        except Exception as e:
            print(f"❌ Ошибка инициализации базы данных: {e}")
            raise Exception(f"Ошибка инициализации PostgreSQL: {e}")
    
    def process_query(self, user_query: str, user_id: str = None, session_id: str = None, temperature: float = 0.0, max_tokens: int = 400, prompt_mode: str = "few_shot") -> Dict[str, Any]:
        """Обрабатывает пользовательский запрос"""
        start_time = time.time()
        request_id = str(uuid.uuid4())[:8]
        
        # Логирование действия пользователя
        if ENHANCED_FEATURES_AVAILABLE and hasattr(self, 'logger'):
            try:
                log_user_action(
                    'query_submitted',
                    user_id=user_id,
                    session_id=session_id,
                    details={'query_length': len(user_query), 'request_id': request_id}
                )
                
                self.logger.info(f"Processing user query", extra={
                    'request_id': request_id,
                    'user_id': user_id,
                    'session_id': session_id,
                    'query_length': len(user_query)
                })
            except Exception as e:
                logger.warning(f"Enhanced logging failed: {e}")
        
        # Проверка на PII
        pii_detected = self.security.detect_pii(user_query)
        if pii_detected:
            if ENHANCED_FEATURES_AVAILABLE:
                try:
                    error = SecurityError(
                        "Personal data detected in query",
                        threat_type="pii_exposure",
                        context=create_error_context(
                            user_id=user_id,
                            session_id=session_id,
                            query=user_query,
                            request_id=request_id
                        )
                    )
                    log_exception(error)
                except Exception as e:
                    logger.warning(f"Enhanced error handling failed: {e}")
            
            return {
                'error': 'Обнаружены персональные данные в запросе',
                'request_id': request_id,
                'sql': '',
                'results': None,
                'metrics': None
            }
        
        # Генерация SQL с повторными попытками
        sql_query, gen_time, attempts_info = self._generate_sql_with_retry(user_query, max_retries=2, temperature=temperature, max_tokens=max_tokens, prompt_mode=prompt_mode)
        
        if not sql_query:
            return {
                'error': 'Не удалось сгенерировать валидный SQL запрос',
                'sql': '',
                'results': None,
                'metrics': None,
                'attempts_info': attempts_info
            }
        
        # Проверка безопасности SQL с расширенной валидацией
        if ENHANCED_FEATURES_AVAILABLE:
            try:
                from advanced_sql_validator import validate_sql_query
                sql_analysis = validate_sql_query(sql_query, {
                    'user_id': user_id,
                    'session_id': session_id,
                    'request_id': request_id
                })
                
                # Проверяем только критические ошибки
                if sql_analysis.validation_result == ValidationResult.BLOCKED:
                    return {
                        'error': f'SQL заблокирован: {"; ".join(sql_analysis.errors[:3])}',
                        'sql': sql_query,
                        'results': None,
                        'metrics': None,
                        'risk_analysis': sql_analysis
                    }
                
                # Сохраняем анализ риска для отображения
                risk_analysis = sql_analysis
            except Exception as e:
                logger.warning(f"Enhanced validation failed, using basic validation: {e}")
                # Fallback на базовую валидацию
                is_safe, security_errors = self.security.validate_sql(sql_query)
                if not is_safe:
                    return {
                        'error': f'Небезопасный SQL: {"; ".join(security_errors)}',
                        'sql': sql_query,
                        'results': None,
                        'metrics': None
                    }
                risk_analysis = None
        else:
            # Базовая валидация
            is_safe, security_errors = self.security.validate_sql(sql_query)
            if not is_safe:
                return {
                    'error': f'Небезопасный SQL: {"; ".join(security_errors)}',
                    'sql': sql_query,
                    'results': None,
                    'metrics': None
                }
            risk_analysis = None
        
        # Выполнение запроса
        try:
            # Используем PostgreSQL с SQLAlchemy
            from sqlalchemy import create_engine
            engine = create_engine(self.db_url)
            results_df = pd.read_sql_query(sql_query, engine)
            engine.dispose()
            
            execution_time = time.time() - start_time
            
            # Создание метрик
            business_terms = self.sql_generator.business_dict.get_related_terms(user_query)
            metrics = QueryMetrics(
                execution_time=execution_time,
                sql_accuracy=True,
                has_errors=False,
                pii_detected=pii_detected,
                business_terms_used=len(business_terms),
                aggregation_accuracy=1.0  # Упрощенная метрика
            )
            
            self.metrics_history.append(metrics)
            
            return {
                'sql': sql_query,
                'results': results_df,
                'metrics': metrics,
                'business_terms': business_terms,
                'explanation': self._generate_explanation(user_query, results_df),
                'risk_analysis': risk_analysis,
                'attempts_info': attempts_info
            }
            
        except Exception as e:
            logger.error(f"Ошибка выполнения SQL: {e}")
            return {
                'error': f'Ошибка выполнения: {str(e)}',
                'sql': sql_query,
                'results': None,
                'metrics': None,
                'attempts_info': attempts_info
            }
    
    def _generate_explanation(self, query: str, results: pd.DataFrame) -> str:
        """Генерирует объяснение результатов"""
        if results.empty:
            return "Запрос не вернул результатов."
        
        rows_count = len(results)
        cols_count = len(results.columns)
        
        explanation = f"Найдено {rows_count} записей с {cols_count} полями. "
        
        # Добавляем статистику по числовым колонкам
        numeric_cols = results.select_dtypes(include=['number']).columns
        if len(numeric_cols) > 0:
            for col in numeric_cols[:2]:  # Первые 2 числовые колонки
                mean_val = results[col].mean()
                total_val = results[col].sum()
                explanation += f"{col}: среднее {mean_val:.2f}, сумма {total_val:.2f}. "
        
        return explanation.strip()
    
    def _remove_unwanted_prefixes(self, sql_query: str) -> str:
        """Удаляет нежелательные префиксы из SQL запроса"""
        # Список нежелательных префиксов (в порядке от длинных к коротким)
        unwanted_prefixes = [
            'EXPLAIN QUERY PLAN ',
            'WITH RECURSIVE ',
            'EXPLAIN ',
            'DESCRIBE ',
            'DESC ',
            'SHOW ',
            'WITH ',
        ]
        
        # Проверяем и удаляем префиксы
        original_query = sql_query
        for prefix in unwanted_prefixes:
            if sql_query.upper().startswith(prefix.upper()):
                sql_query = sql_query[len(prefix):].strip()
                logger.info(f"Удален префикс '{prefix}' из SQL: {original_query[:50]}...")
                break
        
        # Специальная обработка для SELECT TOP (SQL Server синтаксис)
        if re.match(r'^SELECT\s+TOP\s+\d+\s+', sql_query, re.IGNORECASE):
            match = re.match(r'^SELECT\s+TOP\s+(\d+)\s+(.*)', sql_query, re.IGNORECASE | re.DOTALL)
            if match:
                limit_num = match.group(1)
                rest_query = match.group(2)
                sql_query = f"SELECT {rest_query} LIMIT {limit_num}"
                logger.info(f"Заменен SELECT TOP {limit_num} на SELECT ... LIMIT {limit_num}")
        
        return sql_query
    
    def _generate_sql_with_retry(self, user_query: str, max_retries: int = 2, temperature: float = 0.0, max_tokens: int = 400, prompt_mode: str = "few_shot") -> Tuple[str, float, List[Dict]]:
        """Генерирует SQL с повторными попытками при ошибках"""
        total_time = 0
        last_error = None
        attempts_info = []
        
        for attempt in range(max_retries + 1):
            attempt_start = time.time()
            try:
                # Вызываем generate_sql в зависимости от типа генератора
                if hasattr(self.sql_generator, 'generate_sql'):
                    # Проверяем сигнатуру метода
                    import inspect
                    sig = inspect.signature(self.sql_generator.generate_sql)
                    params = list(sig.parameters.keys())
                    
                    if len(params) >= 4:  # SQLGenerator: (self, user_query, temperature, max_tokens, prompt_mode)
                        sql_query, gen_time = self.sql_generator.generate_sql(user_query, temperature, max_tokens, prompt_mode)
                    else:  # FineTunedSQLGenerator: (self, user_query, schema_info)
                        sql_query, gen_time = self.sql_generator.generate_sql(user_query, None)
                else:
                    raise AttributeError("SQL generator does not have generate_sql method")
                total_time += gen_time
                
                if sql_query:
                    # Очищаем SQL от нежелательных префиксов перед валидацией
                    cleaned_sql = self._remove_unwanted_prefixes(sql_query)
                    if cleaned_sql != sql_query:
                        logger.info(f"SQL очищен: '{sql_query[:50]}...' → '{cleaned_sql[:50]}...'")
                    
                    # Проверяем что SQL можно выполнить в PostgreSQL
                    validation_error = None
                    try:
                        from sqlalchemy import create_engine, text
                        engine = create_engine(self.db_url)
                        with engine.connect() as connection:
                            connection.execute(text("EXPLAIN " + cleaned_sql))
                        engine.dispose()
                    except Exception as validation_e:
                        validation_error = str(validation_e)
                        raise validation_e
                    
                    # Успешная попытка
                    attempts_info.append({
                        'attempt': attempt + 1,
                        'success': True,
                        'sql': cleaned_sql,
                        'generation_time': gen_time,
                        'total_time': total_time,
                        'error': None
                    })
                    return cleaned_sql, total_time, attempts_info
                    
            except Exception as e:
                last_error = str(e)
                attempt_time = time.time() - attempt_start
                total_time += attempt_time
                
                # Записываем информацию о неудачной попытке
                attempts_info.append({
                    'attempt': attempt + 1,
                    'success': False,
                    'sql': None,
                    'generation_time': attempt_time,
                    'total_time': total_time,
                    'error': str(e),
                    'error_type': type(e).__name__
                })
                
                logger.warning(f"Попытка {attempt + 1} не удалась: {e}")
                continue
        
        logger.error(f"Все попытки генерации SQL не удались. Последняя ошибка: {last_error}")
        return "", total_time, attempts_info
    
    def generate_sql(self, user_query: str, temperature: float = 0.0, max_tokens: int = 400, prompt_mode: str = "few_shot") -> Tuple[str, float]:
        """Генерирует SQL запрос для пользовательского вопроса"""
        if not hasattr(self, 'sql_generator') or not self.sql_generator:
            raise Exception("SQL генератор не инициализирован")
        
        # Проверяем сигнатуру метода генератора
        import inspect
        sig = inspect.signature(self.sql_generator.generate_sql)
        params = list(sig.parameters.keys())
        
        if len(params) >= 4:  # SQLGenerator: (self, user_query, temperature, max_tokens, prompt_mode)
            return self.sql_generator.generate_sql(user_query, temperature, max_tokens, prompt_mode)
        else:  # FineTunedSQLGenerator: (self, user_query, schema_info)
            return self.sql_generator.generate_sql(user_query, None)
    
    def get_performance_metrics(self) -> Dict[str, float]:
        """Возвращает метрики производительности"""
        if not self.metrics_history:
            return {}
        
        recent_metrics = self.metrics_history[-10:]  # Последние 10 запросов
        
        return {
            'avg_execution_time': sum(m.execution_time for m in recent_metrics) / len(recent_metrics),
            'sql_accuracy_rate': sum(m.sql_accuracy for m in recent_metrics) / len(recent_metrics),
            'error_rate': sum(m.has_errors for m in recent_metrics) / len(recent_metrics),
            'business_terms_usage': sum(m.business_terms_used for m in recent_metrics) / len(recent_metrics),
            'total_queries': len(self.metrics_history)
        }


def main():
    """Основная функция для демонстрации"""
    # Парсинг аргументов командной строки
    parser = argparse.ArgumentParser(description='BI-GPT Agent - Natural Language to SQL')
    parser.add_argument('--api_key', type=str, 
                       help='API key for the model (or set LOCAL_API_KEY/OPENAI_API_KEY env var)')
    parser.add_argument('--base_url', type=str,
                       help='Base URL for the model API (or set LOCAL_BASE_URL env var)')
    parser.add_argument('--query', type=str,
                       help='Single query to execute')
    
    args = parser.parse_args()
    
    print("BI-GPT Agent - Natural Language to SQL")
    print(f"Model: {args.base_url}")
    print(f"API Key: {args.api_key[:10]}...")
    
    # Инициализация агента с параметрами
    agent = BIGPTAgent(
        api_key=args.api_key,
        base_url=args.base_url
    )
    
    # Если передан одиночный запрос
    if args.query:
        print(f"\nExecuting query: {args.query}")
        result = agent.process_query(args.query)
        
        if 'error' in result:
            print(f"Error: {result['error']}")
            return 1
        else:
            print(f"SQL: {result['sql']}")
            print(f"Results: {len(result['results'])} rows")
            if not result['results'].empty:
                print("\nData:")
                print(result['results'].to_string())
            return 0
    
    # Тестовые запросы (если не передан конкретный запрос)
    test_queries = [
        "покажи всех клиентов",
        "количество заказов",
        "средний чек клиентов"
    ]
    
    print(f"\nTesting queries:")
    successful = 0
    
    for query in test_queries:
        print(f"\nQuery: {query}")
        result = agent.process_query(query)
        
        if 'error' in result:
            print(f"Error: {result['error']}")
        else:
            successful += 1
            print(f"SQL: {result['sql']}")
            print(f"Results: {len(result['results'])} rows")
    
    # Показ метрик
    print(f"\nSuccess rate: {successful}/{len(test_queries)}")
    metrics = agent.get_performance_metrics()
    if metrics:
        print(f"Performance metrics:")
        for key, value in metrics.items():
            print(f"  {key}: {value:.3f}")
    
    return 0


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
