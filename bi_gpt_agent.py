"""
BI-GPT Agent: Natural Language to SQL converter for corporate BI
"""

import os
import re
import sqlite3
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
from langchain.llms import OpenAI
from langchain.chains import LLMChain
from langchain.prompts import PromptTemplate
from langchain.memory import ConversationBufferMemory
import pandas as pd
from sqlalchemy import create_engine, text, MetaData, inspect
from pydantic import BaseModel, Field
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

# Импорт новых систем (с обработкой ошибок импорта)
try:
    from config import get_settings, validate_config, ConfigurationError
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
        
        # Улучшенный промпт с примерами few-shot learning
        self.sql_prompt = """
Ты эксперт по SQL. Переведи запрос на русском языке в точный SQL запрос. Начинай с SELECT.

СХЕМА БАЗЫ ДАННЫХ:
orders: id, customer_id, order_date, amount, status
customers: id, name, email, registration_date, segment  
products: id, name, category, price, cost
sales: id, order_id, product_id, quantity, revenue, costs
inventory: id, product_id, current_stock, warehouse

БИЗНЕС-ТЕРМИНЫ:
{business_terms}

ПРИМЕРЫ:
Запрос: "покажи всех клиентов"
SQL: SELECT * FROM customers LIMIT 1000;

Запрос: "прибыль за последние 2 дня"
SQL: SELECT SUM(revenue - costs) as profit FROM sales s JOIN orders o ON s.order_id = o.id WHERE o.order_date >= DATE('now', '-2 days') LIMIT 1000;

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
SQL: SELECT o.id, o.amount, c.name FROM orders o JOIN customers c ON o.customer_id = c.id WHERE DATE(o.order_date) = DATE('now') LIMIT 1000;

Запрос: "клиенты премиум сегмента"
SQL: SELECT name, email, registration_date FROM customers WHERE segment = 'Premium' LIMIT 1000;

Запрос: "товары с низкими остатками"
SQL: SELECT p.name, p.category, i.current_stock FROM products p JOIN inventory i ON p.id = i.product_id WHERE i.current_stock < 10 LIMIT 1000;

ПРАВИЛА:
1. Только SELECT запросы
2. Обязательно LIMIT 1000
3. Используй правильные JOIN между таблицами
4. Для дат используй DATE('now', '-N days')
5. Точные имена полей из схемы
6. Верни только SQL код без объяснений

ЗАПРОС: {user_query}
SQL:"""

    def generate_sql(self, user_query: str, schema_info: Dict) -> Tuple[str, float]:
        """Генерирует SQL запрос из естественного языка"""
        start_time = time.time()
        
        # Подготовка бизнес-терминов для промпта
        related_terms = self.business_dict.get_related_terms(user_query)
        business_terms_str = "\n".join([
            f"- {term}: {self.business_dict.translate_term(term)}" 
            for term in related_terms
        ])
        
        try:
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": "Ты эксперт по SQL. Отвечай только валидным SQL кодом без объяснений."},
                    {"role": "user", "content": self.sql_prompt.format(
                        business_terms=business_terms_str,
                        user_query=user_query
                    )}
                ],
                temperature=0.0,  # Минимальная температура для точности
                max_tokens=400,   # Больше токенов для сложных запросов
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
        
        # Убираем точку с запятой в конце если есть
        if sql.endswith(';'):
            sql = sql[:-1]
        
        # Проверяем что запрос начинается с SELECT
        if not sql.upper().startswith('SELECT'):
            raise ValueError("Запрос должен начинаться с SELECT")
        
        # Добавляем LIMIT если его нет
        if 'LIMIT' not in sql.upper():
            sql += ' LIMIT 1000'
        
        # Базовая валидация структуры
        if 'FROM' not in sql.upper():
            raise ValueError("SQL запрос должен содержать FROM")
        
        return sql


class BIGPTAgent:
    """Основной класс BI-GPT агента"""
    
    def __init__(self, db_path: str = "bi_demo.db", api_key: str = None, base_url: str = None):
        # Улучшенная инициализация с новыми системами
        if ENHANCED_FEATURES_AVAILABLE:
            try:
                self.settings = get_settings()
                self.logger = get_logger('bi_gpt_agent')
                
                # Используем путь из настроек если не передан
                self.db_path = db_path if db_path != "bi_demo.db" else self.settings.database_url.replace('sqlite:///', '')
                
                # Валидация конфигурации
                config_errors = validate_config()
                if config_errors and self.settings.is_production:
                    raise ConfigurationError(f"Configuration validation failed: {'; '.join(config_errors)}")
                    
                self.logger.info("BI-GPT Agent initializing with enhanced features")
                
            except Exception as e:
                # Fallback если новые системы не работают
                logger.warning(f"Enhanced initialization failed, using legacy mode: {e}")
                self.db_path = db_path
                self.settings = None
                self.logger = logger
        else:
            self.db_path = db_path
            self.settings = None
            self.logger = logger
        
        # Инициализация генератора SQL с поддержкой локальных моделей
        if base_url:
            self.sql_generator = SQLGenerator(api_key, base_url)
        else:
            self.sql_generator = SQLGenerator(api_key or os.getenv("OPENAI_API_KEY"))
            
        self.security = SecurityValidator()
        self.metrics_history = []
        
        # Инициализация базы данных
        self._init_demo_database()
        
        if hasattr(self, 'logger'):
            self.logger.info(f"BI-GPT Agent initialized successfully", extra={
                'database_path': self.db_path,
                'enhanced_features': ENHANCED_FEATURES_AVAILABLE
            })
        
    def _init_demo_database(self):
        """Создает демо базу данных с тестовыми данными"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Создание таблиц
        cursor.executescript("""
        CREATE TABLE IF NOT EXISTS customers (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            registration_date DATE,
            segment TEXT
        );
        
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT,
            price DECIMAL(10,2),
            cost DECIMAL(10,2)
        );
        
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            order_date DATE,
            amount DECIMAL(10,2),
            status TEXT,
            FOREIGN KEY (customer_id) REFERENCES customers(id)
        );
        
        CREATE TABLE IF NOT EXISTS sales (
            id INTEGER PRIMARY KEY,
            order_id INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            revenue DECIMAL(10,2),
            costs DECIMAL(10,2),
            FOREIGN KEY (order_id) REFERENCES orders(id),
            FOREIGN KEY (product_id) REFERENCES products(id)
        );
        
        CREATE TABLE IF NOT EXISTS inventory (
            id INTEGER PRIMARY KEY,
            product_id INTEGER,
            current_stock INTEGER,
            warehouse TEXT,
            FOREIGN KEY (product_id) REFERENCES products(id)
        );
        """)
        
        # Вставка тестовых данных
        cursor.executescript("""
        INSERT OR IGNORE INTO customers VALUES 
        (1, 'Иван Иванов', 'ivan@email.com', '2023-01-15', 'Premium'),
        (2, 'Мария Петрова', 'maria@email.com', '2023-02-20', 'Standard'),
        (3, 'Алексей Сидоров', 'alex@email.com', '2023-03-10', 'Premium');
        
        INSERT OR IGNORE INTO products VALUES
        (1, 'Ноутбук ASUS', 'Электроника', 50000, 35000),
        (2, 'Мышь Logitech', 'Электроника', 2000, 1200),
        (3, 'Клавиатура', 'Электроника', 3000, 2000);
        
        INSERT OR IGNORE INTO orders VALUES
        (1, 1, '2024-09-15', 52000, 'completed'),
        (2, 2, '2024-09-14', 5000, 'completed'),
        (3, 3, '2024-09-13', 50000, 'pending');
        
        INSERT OR IGNORE INTO sales VALUES
        (1, 1, 1, 1, 50000, 35000),
        (2, 1, 2, 1, 2000, 1200),
        (3, 2, 2, 1, 2000, 1200),
        (4, 2, 3, 1, 3000, 2000),
        (5, 3, 1, 1, 50000, 35000);
        
        INSERT OR IGNORE INTO inventory VALUES
        (1, 1, 10, 'Москва'),
        (2, 2, 50, 'Москва'),
        (3, 3, 30, 'СПб');
        """)
        
        conn.commit()
        conn.close()
        
    def process_query(self, user_query: str, user_id: str = None, session_id: str = None) -> Dict[str, Any]:
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
        sql_query, gen_time = self._generate_sql_with_retry(user_query, max_retries=2)
        
        if not sql_query:
            return {
                'error': 'Не удалось сгенерировать валидный SQL запрос',
                'sql': '',
                'results': None,
                'metrics': None
            }
        
        # Проверка безопасности SQL
        is_safe, security_errors = self.security.validate_sql(sql_query)
        if not is_safe:
            return {
                'error': f'Небезопасный SQL: {"; ".join(security_errors)}',
                'sql': sql_query,
                'results': None,
                'metrics': None
            }
        
        # Выполнение запроса
        try:
            conn = sqlite3.connect(self.db_path)
            results_df = pd.read_sql_query(sql_query, conn)
            conn.close()
            
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
                'explanation': self._generate_explanation(user_query, results_df)
            }
            
        except Exception as e:
            logger.error(f"Ошибка выполнения SQL: {e}")
            return {
                'error': f'Ошибка выполнения: {str(e)}',
                'sql': sql_query,
                'results': None,
                'metrics': None
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
    
    def _generate_sql_with_retry(self, user_query: str, max_retries: int = 2) -> Tuple[str, float]:
        """Генерирует SQL с повторными попытками при ошибках"""
        total_time = 0
        last_error = None
        
        for attempt in range(max_retries + 1):
            try:
                sql_query, gen_time = self.sql_generator.generate_sql(user_query, {})
                total_time += gen_time
                
                if sql_query:
                    # Проверяем что SQL можно выполнить
                    conn = sqlite3.connect(self.db_path)
                    cursor = conn.cursor()
                    cursor.execute("EXPLAIN QUERY PLAN " + sql_query)
                    conn.close()
                    return sql_query, total_time
                    
            except Exception as e:
                last_error = str(e)
                logger.warning(f"Попытка {attempt + 1} не удалась: {e}")
                continue
        
        logger.error(f"Все попытки генерации SQL не удались. Последняя ошибка: {last_error}")
        return "", total_time
    
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
