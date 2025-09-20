"""
Утилита для выполнения SQL запросов в подключенной базе данных
"""

from typing import Optional, List, Dict, Any, Tuple
import pandas as pd
from sqlalchemy import text, Engine
from sqlalchemy.exc import SQLAlchemyError
import logging

logger = logging.getLogger(__name__)


class SQLExecutor:
    """Класс для выполнения SQL запросов"""
    
    def __init__(self, engine: Engine):
        self.engine = engine
    
    def execute_query(self, query: str, limit: Optional[int] = None) -> Tuple[bool, Any, Optional[str]]:
        """
        Выполняет SQL запрос
        
        Args:
            query: SQL запрос
            limit: Лимит строк для SELECT запросов
            
        Returns:
            Tuple[bool, Any, Optional[str]]: (success, result, error_message)
        """
        try:
            # Очищаем запрос
            query = query.strip()
            if not query.endswith(';'):
                query += ';'
            
            # Добавляем лимит для SELECT запросов если не указан
            if limit and query.upper().startswith('SELECT') and 'LIMIT' not in query.upper():
                query = f"{query.rstrip(';')} LIMIT {limit};"
            
            with self.engine.connect() as conn:
                query_upper = query.upper()
                
                if any(keyword in query_upper for keyword in ['SELECT', 'SHOW', 'DESCRIBE', 'EXPLAIN']):
                    # Читающий запрос
                    result = conn.execute(text(query))
                    
                    if result.returns_rows:
                        df = pd.DataFrame(result.fetchall(), columns=result.keys())
                        return True, df, None
                    else:
                        return True, "Query executed successfully", None
                else:
                    # Изменяющий запрос
                    result = conn.execute(text(query))
                    conn.commit()
                    return True, f"Query executed successfully. Rows affected: {result.rowcount}", None
                    
        except SQLAlchemyError as e:
            logger.error(f"SQL Error: {str(e)}")
            return False, None, str(e)
        except Exception as e:
            logger.error(f"General Error: {str(e)}")
            return False, None, str(e)
    
    def get_table_info(self, table_name: str, schema: str = "public") -> Tuple[bool, Any, Optional[str]]:
        """
        Получает информацию о таблице
        
        Args:
            table_name: Имя таблицы
            schema: Имя схемы
            
        Returns:
            Tuple[bool, Any, Optional[str]]: (success, result, error_message)
        """
        query = """
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length
            FROM information_schema.columns 
            WHERE table_schema = :schema 
            AND table_name = :table_name
            ORDER BY ordinal_position;
        """
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), {"schema": schema, "table_name": table_name})
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                return True, df, None
        except Exception as e:
            logger.error(f"Error getting table info: {str(e)}")
            return False, None, str(e)
    
    def get_tables_list(self, schema: str = "public") -> Tuple[bool, Any, Optional[str]]:
        """
        Получает список таблиц в схеме
        
        Args:
            schema: Имя схемы
            
        Returns:
            Tuple[bool, Any, Optional[str]]: (success, result, error_message)
        """
        query = """
            SELECT 
                table_name,
                table_type
            FROM information_schema.tables 
            WHERE table_schema = :schema
            ORDER BY table_name;
        """
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), {"schema": schema})
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                return True, df, None
        except Exception as e:
            logger.error(f"Error getting tables list: {str(e)}")
            return False, None, str(e)
    
    def test_connection(self) -> Tuple[bool, Optional[str]]:
        """
        Тестирует подключение к базе данных
        
        Returns:
            Tuple[bool, Optional[str]]: (success, error_message)
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                return True, None
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False, str(e)
    
    def get_database_info(self) -> Tuple[bool, Dict[str, Any], Optional[str]]:
        """
        Получает информацию о базе данных
        
        Returns:
            Tuple[bool, Dict[str, Any], Optional[str]]: (success, info_dict, error_message)
        """
        try:
            info = {}
            
            with self.engine.connect() as conn:
                # Версия PostgreSQL
                result = conn.execute(text("SELECT version();"))
                info['version'] = result.fetchone()[0]
                
                # Размер базы данных
                result = conn.execute(text("SELECT pg_size_pretty(pg_database_size(current_database()));"))
                info['size'] = result.fetchone()[0]
                
                # Активные подключения
                result = conn.execute(text("SELECT count(*) FROM pg_stat_activity;"))
                info['active_connections'] = result.fetchone()[0]
                
                # Количество таблиц
                result = conn.execute(text("""
                    SELECT count(*) 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public';
                """))
                info['tables_count'] = result.fetchone()[0]
                
                return True, info, None
                
        except Exception as e:
            logger.error(f"Error getting database info: {str(e)}")
            return False, {}, str(e)
    
    def validate_query(self, query: str) -> Tuple[bool, Optional[str]]:
        """
        Валидирует SQL запрос (базовая проверка)
        
        Args:
            query: SQL запрос
            
        Returns:
            Tuple[bool, Optional[str]]: (is_valid, error_message)
        """
        query_upper = query.upper().strip()
        
        # Проверяем на опасные операции
        dangerous_keywords = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE', 'INSERT', 'UPDATE']
        
        for keyword in dangerous_keywords:
            if keyword in query_upper:
                return False, f"Query contains potentially dangerous keyword: {keyword}"
        
        # Проверяем базовую структуру
        if not any(keyword in query_upper for keyword in ['SELECT', 'SHOW', 'DESCRIBE', 'EXPLAIN']):
            return False, "Query must be a SELECT, SHOW, DESCRIBE, or EXPLAIN statement"
        
        return True, None


class QueryValidator:
    """Класс для валидации SQL запросов"""
    
    @staticmethod
    def is_read_only_query(query: str) -> bool:
        """Проверяет, является ли запрос только для чтения"""
        query_upper = query.upper().strip()
        read_only_keywords = ['SELECT', 'SHOW', 'DESCRIBE', 'EXPLAIN', 'WITH']
        
        return any(keyword in query_upper for keyword in read_only_keywords)
    
    @staticmethod
    def contains_dangerous_operations(query: str) -> List[str]:
        """Проверяет наличие опасных операций в запросе"""
        query_upper = query.upper().strip()
        dangerous_keywords = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE', 'INSERT', 'UPDATE']
        
        found_keywords = []
        for keyword in dangerous_keywords:
            if keyword in query_upper:
                found_keywords.append(keyword)
        
        return found_keywords
    
    @staticmethod
    def get_query_type(query: str) -> str:
        """Определяет тип SQL запроса"""
        query_upper = query.upper().strip()
        
        if query_upper.startswith('SELECT'):
            return 'SELECT'
        elif query_upper.startswith('INSERT'):
            return 'INSERT'
        elif query_upper.startswith('UPDATE'):
            return 'UPDATE'
        elif query_upper.startswith('DELETE'):
            return 'DELETE'
        elif query_upper.startswith('CREATE'):
            return 'CREATE'
        elif query_upper.startswith('DROP'):
            return 'DROP'
        elif query_upper.startswith('ALTER'):
            return 'ALTER'
        else:
            return 'OTHER'
