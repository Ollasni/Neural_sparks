"""
Dynamic Schema Extractor для получения схемы БД в реальном времени
Поддерживает PostgreSQL, MySQL и другие БД через SQLAlchemy
"""

import logging
import time
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime
import json

# Импорты для работы с БД
try:
    import sqlalchemy as sa
    from sqlalchemy import create_engine, MetaData, inspect
    from sqlalchemy.exc import SQLAlchemyError
    HAS_SQLALCHEMY = True
except ImportError:
    HAS_SQLALCHEMY = False

# Fallback для PostgreSQL
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

logger = logging.getLogger(__name__)


@dataclass
class ColumnSchema:
    """Схема колонки"""
    name: str
    type: str
    nullable: bool = True
    primary_key: bool = False
    foreign_key: Optional[str] = None
    default: Any = None
    comment: Optional[str] = None


@dataclass
class TableSchema:
    """Схема таблицы"""
    name: str
    schema: str
    columns: List[ColumnSchema]
    row_count: Optional[int] = None
    comment: Optional[str] = None


@dataclass
class DatabaseSchema:
    """Полная схема базы данных"""
    database_type: str
    tables: List[TableSchema]
    foreign_keys: List[Dict[str, str]]
    extraction_time: datetime
    connection_string: str
    
    def to_prompt_format(self) -> str:
        """Преобразует схему в формат для промпта модели"""
        schema_lines = []
        
        for table in self.tables:
            # Формируем строку с колонками для таблицы
            columns_str = ", ".join([
                f"{col.name}" + (f" ({col.type})" if col.type else "")
                for col in table.columns
            ])
            
            # Добавляем имя таблицы без схемы для краткости
            table_name = table.name.split('.')[-1] if '.' in table.name else table.name
            schema_lines.append(f"{table_name}: {columns_str}")
        
        return "\n".join(schema_lines)
    
    def to_json_format(self) -> Dict[str, Any]:
        """Преобразует в формат, совместимый с schema.json"""
        tables_dict = {}
        fks = []
        
        for table in self.tables:
            table_key = table.name
            columns_list = []
            
            for col in table.columns:
                col_dict = {
                    "name": col.name,
                    "type": col.type,
                    "pk": col.primary_key,
                    "nullable": col.nullable,
                    "default": col.default,
                    "tags": self._generate_tags(col),
                    "description": col.comment
                }
                columns_list.append(col_dict)
                
                # Добавляем внешний ключ если есть
                if col.foreign_key:
                    fks.append({
                        "from": f"{table.name}.{col.name}",
                        "to": col.foreign_key,
                        "constraint": f"fk_{table.name}_{col.name}"
                    })
            
            tables_dict[table_key] = {
                "columns": columns_list,
                "description": table.comment,
                "row_count": table.row_count,
                "schema": table.schema
            }
        
        return {
            "db": self.database_type,
            "schemas": list(set([table.schema for table in self.tables])),
            "tables": tables_dict,
            "fks": fks,
            "metadata": {
                "extraction_timestamp": self.extraction_time.isoformat(),
                "connection_string": self._mask_connection_string(self.connection_string),
                "total_tables": len(self.tables),
                "total_columns": sum(len(table.columns) for table in self.tables),
                "total_foreign_keys": len(fks)
            }
        }
    
    def _generate_tags(self, col: ColumnSchema) -> List[str]:
        """Генерирует теги для колонки на основе её типа и имени"""
        tags = []
        
        # По типу данных
        if any(t in col.type.upper() for t in ['INT', 'SERIAL', 'NUMBER']):
            tags.append("number")
        if any(t in col.type.upper() for t in ['TEXT', 'VARCHAR', 'CHAR', 'STRING']):
            tags.append("text")
        if any(t in col.type.upper() for t in ['DATE', 'TIME', 'TIMESTAMP']):
            tags.append("time")
        if any(t in col.type.upper() for t in ['DECIMAL', 'MONEY', 'FLOAT', 'REAL']):
            tags.append("money")
        
        # По имени колонки
        col_name = col.name.lower()
        if 'id' in col_name:
            tags.append("id")
        if any(word in col_name for word in ['email', 'phone', 'contact']):
            tags.append("contact")
        if any(word in col_name for word in ['status', 'state', 'type', 'category']):
            tags.append("category")
        if any(word in col_name for word in ['price', 'cost', 'amount', 'revenue']):
            tags.append("measure")
        if col.primary_key:
            tags.append("key")
        
        return list(set(tags))  # Убираем дубликаты
    
    def _mask_connection_string(self, conn_str: str) -> str:
        """Маскирует пароль в строке подключения"""
        try:
            if '://' in conn_str:
                # postgresql://user:password@host:port/db -> postgresql://user:***@host:port/db
                parts = conn_str.split('://')
                if len(parts) == 2:
                    protocol = parts[0]
                    rest = parts[1]
                    if '@' in rest:
                        auth_part, host_part = rest.split('@', 1)
                        if ':' in auth_part:
                            user, _ = auth_part.split(':', 1)
                            return f"{protocol}://{user}:***@{host_part}"
            return conn_str
        except:
            return "***"


class DynamicSchemaExtractor:
    """Динамический экстрактор схемы БД"""
    
    def __init__(self, connection_string: str, cache_ttl: int = 300):
        """
        Args:
            connection_string: Строка подключения к БД
            cache_ttl: Время жизни кэша в секундах (по умолчанию 5 минут)
        """
        self.connection_string = connection_string
        self.cache_ttl = cache_ttl
        self._cached_schema: Optional[DatabaseSchema] = None
        self._cache_time: Optional[datetime] = None
        
        # Определяем тип БД
        self.database_type = self._detect_database_type()
        
    def _detect_database_type(self) -> str:
        """Определяет тип БД по строке подключения"""
        conn_str = self.connection_string.lower()
        if 'postgresql' in conn_str or 'postgres' in conn_str:
            return 'postgresql'
        elif 'mysql' in conn_str:
            return 'mysql'
        elif 'sqlite' in conn_str:
            return 'sqlite'
        elif 'oracle' in conn_str:
            return 'oracle'
        else:
            return 'unknown'
    
    def get_schema(self, force_refresh: bool = False) -> DatabaseSchema:
        """
        Получает схему БД (с кэшированием)
        
        Args:
            force_refresh: Принудительно обновить кэш
            
        Returns:
            DatabaseSchema: Схема базы данных
        """
        # Проверяем кэш
        if not force_refresh and self._is_cache_valid():
            logger.info("Using cached schema")
            return self._cached_schema
        
        logger.info("Extracting fresh schema from database")
        
        try:
            # Пытаемся использовать SQLAlchemy (предпочтительно)
            if HAS_SQLALCHEMY:
                schema = self._extract_with_sqlalchemy()
            elif self.database_type == 'postgresql' and HAS_PSYCOPG2:
                schema = self._extract_postgresql_direct()
            else:
                raise RuntimeError(
                    "No suitable database connector available. "
                    "Install sqlalchemy or psycopg2 for PostgreSQL"
                )
            
            # Кэшируем результат
            self._cached_schema = schema
            self._cache_time = datetime.now()
            
            logger.info(f"Schema extracted successfully: {len(schema.tables)} tables")
            return schema
            
        except Exception as e:
            logger.error(f"Failed to extract schema: {e}")
            raise
    
    def _is_cache_valid(self) -> bool:
        """Проверяет валидность кэша"""
        if self._cached_schema is None or self._cache_time is None:
            return False
        
        age = (datetime.now() - self._cache_time).total_seconds()
        return age < self.cache_ttl
    
    def _extract_with_sqlalchemy(self) -> DatabaseSchema:
        """Извлекает схему через SQLAlchemy"""
        engine = create_engine(self.connection_string)
        
        try:
            metadata = MetaData()
            inspector = inspect(engine)
            
            tables = []
            all_foreign_keys = []
            
            # Получаем список всех таблиц
            table_names = inspector.get_table_names()
            
            for table_name in table_names:
                # Получаем колонки
                columns_info = inspector.get_columns(table_name)
                pk_constraint = inspector.get_pk_constraint(table_name)
                primary_keys = pk_constraint['constrained_columns'] if pk_constraint else []
                foreign_keys = inspector.get_foreign_keys(table_name)
                
                columns = []
                for col_info in columns_info:
                    # Находим FK для этой колонки
                    fk_target = None
                    for fk in foreign_keys:
                        if col_info['name'] in fk['constrained_columns']:
                            ref_table = fk['referred_table']
                            ref_column = fk['referred_columns'][0]  # Берем первую
                            fk_target = f"{ref_table}.{ref_column}"
                            break
                    
                    column = ColumnSchema(
                        name=col_info['name'],
                        type=str(col_info['type']),
                        nullable=col_info['nullable'],
                        primary_key=col_info['name'] in primary_keys,
                        foreign_key=fk_target,
                        default=col_info.get('default'),
                        comment=col_info.get('comment')
                    )
                    columns.append(column)
                
                # Добавляем FK в общий список
                for fk in foreign_keys:
                    for i, constrained_col in enumerate(fk['constrained_columns']):
                        referred_col = fk['referred_columns'][i] if i < len(fk['referred_columns']) else fk['referred_columns'][0]
                        all_foreign_keys.append({
                            "from": f"{table_name}.{constrained_col}",
                            "to": f"{fk['referred_table']}.{referred_col}",
                            "constraint": fk.get('name', f"fk_{table_name}_{constrained_col}")
                        })
                
                # Пытаемся получить количество строк
                row_count = None
                try:
                    with engine.connect() as conn:
                        result = conn.execute(sa.text(f"SELECT COUNT(*) FROM {table_name}"))
                        row_count = result.scalar()
                except:
                    pass  # Игнорируем ошибки подсчета строк
                
                table = TableSchema(
                    name=table_name,
                    schema='public',  # Для простоты используем public
                    columns=columns,
                    row_count=row_count,
                    comment=None
                )
                tables.append(table)
            
            return DatabaseSchema(
                database_type=self.database_type,
                tables=tables,
                foreign_keys=all_foreign_keys,
                extraction_time=datetime.now(),
                connection_string=self.connection_string
            )
            
        finally:
            engine.dispose()
    
    def _extract_postgresql_direct(self) -> DatabaseSchema:
        """Извлекает схему PostgreSQL напрямую через psycopg2"""
        import psycopg2
        from psycopg2.extras import RealDictCursor
        
        conn = psycopg2.connect(self.connection_string)
        
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                tables = []
                all_foreign_keys = []
                
                # Получаем список таблиц
                cursor.execute("""
                    SELECT table_name, table_schema
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """)
                
                table_list = cursor.fetchall()
                
                for table_info in table_list:
                    table_name = table_info['table_name']
                    schema_name = table_info['table_schema']
                    
                    # Получаем колонки
                    cursor.execute("""
                        SELECT 
                            c.column_name,
                            c.data_type,
                            c.is_nullable,
                            c.column_default,
                            CASE WHEN tc.constraint_type = 'PRIMARY KEY' THEN true ELSE false END as is_pk,
                            col_description(pgc.oid, c.ordinal_position) as column_comment
                        FROM information_schema.columns c
                        LEFT JOIN information_schema.key_column_usage kcu 
                            ON c.table_name = kcu.table_name AND c.column_name = kcu.column_name
                        LEFT JOIN information_schema.table_constraints tc 
                            ON kcu.constraint_name = tc.constraint_name
                        LEFT JOIN pg_class pgc ON pgc.relname = c.table_name
                        WHERE c.table_schema = %s AND c.table_name = %s
                        ORDER BY c.ordinal_position
                    """, (schema_name, table_name))
                    
                    columns_info = cursor.fetchall()
                    columns = []
                    
                    for col_info in columns_info:
                        column = ColumnSchema(
                            name=col_info['column_name'],
                            type=col_info['data_type'],
                            nullable=col_info['is_nullable'] == 'YES',
                            primary_key=col_info['is_pk'],
                            default=col_info['column_default'],
                            comment=col_info['column_comment']
                        )
                        columns.append(column)
                    
                    # Получаем внешние ключи для таблицы
                    cursor.execute("""
                        SELECT
                            kcu.column_name,
                            ccu.table_name AS foreign_table_name,
                            ccu.column_name AS foreign_column_name,
                            tc.constraint_name
                        FROM information_schema.table_constraints AS tc
                        JOIN information_schema.key_column_usage AS kcu
                            ON tc.constraint_name = kcu.constraint_name
                        JOIN information_schema.constraint_column_usage AS ccu
                            ON ccu.constraint_name = tc.constraint_name
                        WHERE tc.constraint_type = 'FOREIGN KEY' 
                        AND tc.table_schema = %s 
                        AND tc.table_name = %s
                    """, (schema_name, table_name))
                    
                    foreign_keys = cursor.fetchall()
                    
                    # Обновляем информацию о FK в колонках
                    for fk in foreign_keys:
                        for column in columns:
                            if column.name == fk['column_name']:
                                column.foreign_key = f"{fk['foreign_table_name']}.{fk['foreign_column_name']}"
                                break
                        
                        all_foreign_keys.append({
                            "from": f"{table_name}.{fk['column_name']}",
                            "to": f"{fk['foreign_table_name']}.{fk['foreign_column_name']}",
                            "constraint": fk['constraint_name']
                        })
                    
                    # Получаем количество строк
                    row_count = None
                    try:
                        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                        row_count = cursor.fetchone()[0]
                    except:
                        pass
                    
                    table = TableSchema(
                        name=table_name,
                        schema=schema_name,
                        columns=columns,
                        row_count=row_count
                    )
                    tables.append(table)
                
                return DatabaseSchema(
                    database_type='postgresql',
                    tables=tables,
                    foreign_keys=all_foreign_keys,
                    extraction_time=datetime.now(),
                    connection_string=self.connection_string
                )
                
        finally:
            conn.close()
    
    def save_schema_to_file(self, output_file: str = "dynamic_schema.json", force_refresh: bool = False):
        """Сохраняет схему в файл в формате, совместимом с schema.json"""
        schema = self.get_schema(force_refresh)
        schema_dict = schema.to_json_format()
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(schema_dict, f, indent=2, ensure_ascii=False, default=str)
        
        logger.info(f"Schema saved to {output_file}")
        return schema_dict


def create_dynamic_extractor(connection_string: str = None, cache_ttl: int = 300) -> DynamicSchemaExtractor:
    """
    Создает экстрактор динамической схемы
    
    Args:
        connection_string: Строка подключения к БД
        cache_ttl: Время жизни кэша в секундах
        
    Returns:
        DynamicSchemaExtractor: Экстрактор схемы
    """
    if not connection_string:
        # Пытаемся получить из переменных окружения или конфига
        try:
            from config import Settings
            settings = Settings()
            connection_string = settings.database_url
        except:
            connection_string = "postgresql://olgasnissarenko@localhost:5432/bi_demo"
    
    return DynamicSchemaExtractor(connection_string, cache_ttl)


def main():
    """Тест функциональности"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Dynamic Schema Extractor')
    parser.add_argument('--connection', type=str, 
                       help='Database connection string')
    parser.add_argument('--output', type=str, default='dynamic_schema.json',
                       help='Output file path')
    parser.add_argument('--cache-ttl', type=int, default=300,
                       help='Cache TTL in seconds')
    
    args = parser.parse_args()
    
    try:
        extractor = create_dynamic_extractor(
            connection_string=args.connection,
            cache_ttl=args.cache_ttl
        )
        
        schema = extractor.get_schema()
        
        print(f"✅ Schema extracted successfully!")
        print(f"   Database type: {schema.database_type}")
        print(f"   Tables: {len(schema.tables)}")
        print(f"   Total columns: {sum(len(table.columns) for table in schema.tables)}")
        print(f"   Foreign keys: {len(schema.foreign_keys)}")
        
        # Сохраняем в файл
        extractor.save_schema_to_file(args.output)
        print(f"   Saved to: {args.output}")
        
        # Показываем схему в формате промпта
        print(f"\n📋 Schema for prompt:")
        print(schema.to_prompt_format())
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise


if __name__ == "__main__":
    main()
