"""
Schema Dump Module for BI-GPT Agent
Автоматический анализ и дамп схемы PostgreSQL БД в JSON формат
Поддерживает множественные схемы, FK связи, PII детекцию
"""

import json
import re
import logging
from typing import Dict, List, Any, Optional, Tuple, Set
from dataclasses import dataclass, asdict
from datetime import datetime
import sqlite3

# Опциональный импорт psycopg2 для PostgreSQL
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False
    logger = logging.getLogger(__name__)
    logger.warning("psycopg2 not available. PostgreSQL support disabled.")

# Настройка логирования
logger = logging.getLogger(__name__)


@dataclass
class ColumnInfo:
    """Информация о колонке таблицы"""
    name: str
    type: str
    pk: bool = False
    nullable: bool = True
    default: Optional[str] = None
    tags: List[str] = None
    description: Optional[str] = None
    
    def __post_init__(self):
        if self.tags is None:
            self.tags = []


@dataclass
class TableInfo:
    """Информация о таблице"""
    name: str
    schema: str
    columns: List[ColumnInfo]
    description: Optional[str] = None
    row_count: Optional[int] = None
    
    @property
    def full_name(self) -> str:
        """Полное имя таблицы с схемой"""
        return f"{self.schema}.{self.name}"


@dataclass
class ForeignKey:
    """Информация о внешнем ключе"""
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    constraint_name: str
    
    @property
    def from_full(self) -> str:
        """Полное имя исходной колонки"""
        return f"{self.from_table}.{self.from_column}"
    
    @property
    def to_full(self) -> str:
        """Полное имя целевой колонки"""
        return f"{self.to_table}.{self.to_column}"


@dataclass
class SchemaInfo:
    """Полная информация о схеме БД"""
    db_type: str
    schemas: List[str]
    tables: Dict[str, TableInfo]
    foreign_keys: List[ForeignKey]
    pii_columns: List[str]
    metadata: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        """Конвертация в словарь для JSON сериализации"""
        return {
            "db": self.db_type,
            "schemas": self.schemas,
            "tables": {
                table_name: {
                    "columns": [asdict(col) for col in table.columns],
                    "description": table.description,
                    "row_count": table.row_count,
                    "schema": table.schema
                }
                for table_name, table in self.tables.items()
            },
            "fks": [
                {
                    "from": fk.from_full,
                    "to": fk.to_full,
                    "constraint": fk.constraint_name
                }
                for fk in self.foreign_keys
            ],
            "pii_columns": self.pii_columns,
            "metadata": self.metadata
        }


class PIIDetector:
    """Детектор персональных данных в колонках"""
    
    def __init__(self):
        # Паттерны для определения PII по именам колонок
        self.pii_patterns = {
            'email': [r'email', r'e_mail', r'mail', r'почта'],
            'phone': [r'phone', r'tel', r'mobile', r'телефон', r'тел'],
            'passport': [r'passport', r'паспорт', r'document'],
            'ssn': [r'ssn', r'social', r'inn', r'инн', r'snils', r'снилс'],
            'name': [r'name', r'имя', r'фамилия', r'surname', r'lastname', r'firstname'],
            'address': [r'address', r'адрес', r'street', r'улица'],
            'card': [r'card', r'карта', r'credit', r'debit'],
            'ip': [r'ip_address', r'ipaddr'],
            'location': [r'location', r'coordinates', r'lat', r'lng', r'coord']
        }
    
    def detect_pii_column(self, column_name: str, table_name: str = "") -> bool:
        """Определяет, содержит ли колонка персональные данные"""
        column_lower = column_name.lower()
        table_lower = table_name.lower()
        
        # Исключения для системных таблиц
        if any(sys_prefix in table_lower for sys_prefix in ['log', 'audit', 'system', 'config']):
            return False
        
        # Проверка по паттернам
        for pii_type, patterns in self.pii_patterns.items():
            for pattern in patterns:
                if re.search(pattern, column_lower):
                    logger.debug(f"PII detected in {table_name}.{column_name} - type: {pii_type}")
                    return True
        
        return False


class ColumnTagger:
    """Автоматическая расстановка тегов для колонок"""
    
    def __init__(self):
        self.type_tags = {
            # Числовые типы
            'integer': ['measure', 'number'],
            'bigint': ['measure', 'number'],
            'numeric': ['measure', 'number'],
            'decimal': ['measure', 'money'],
            'real': ['measure', 'number'],
            'double': ['measure', 'number'],
            'serial': ['id', 'number'],
            'bigserial': ['id', 'number'],
            
            # Временные типы
            'date': ['date', 'time'],
            'timestamp': ['date', 'time'],
            'timestamptz': ['date', 'time'],
            'time': ['time'],
            'interval': ['time'],
            
            # Текстовые типы
            'text': ['text'],
            'varchar': ['text'],
            'char': ['text'],
            'json': ['json'],
            'jsonb': ['json'],
            
            # Логические
            'boolean': ['flag'],
            'bool': ['flag']
        }
        
        self.name_tags = {
            # Финансовые
            'amount': ['money', 'measure'],
            'price': ['money', 'measure'],
            'cost': ['money', 'measure'],
            'revenue': ['money', 'measure'],
            'profit': ['money', 'measure'],
            'sum': ['money', 'measure'],
            'total': ['money', 'measure'],
            
            # Количественные
            'count': ['measure', 'number'],
            'quantity': ['measure', 'number'],
            'qty': ['measure', 'number'],
            'num': ['measure', 'number'],
            'size': ['measure', 'number'],
            'length': ['measure', 'number'],
            'weight': ['measure', 'number'],
            
            # Временные
            'date': ['date', 'time'],
            'time': ['time'],
            'created': ['date', 'time'],
            'updated': ['date', 'time'],
            'modified': ['date', 'time'],
            
            # Идентификаторы
            'id': ['id', 'key'],
            'key': ['id', 'key'],
            'code': ['code'],
            'uuid': ['id', 'key'],
            
            # Статусы
            'status': ['status', 'category'],
            'state': ['status', 'category'],
            'type': ['category'],
            'category': ['category'],
            'segment': ['category']
        }
    
    def get_tags(self, column: ColumnInfo) -> List[str]:
        """Определяет теги для колонки"""
        tags = set()
        
        # Теги по типу данных
        column_type = column.type.lower()
        for db_type, type_tags in self.type_tags.items():
            if db_type in column_type:
                tags.update(type_tags)
                break
        
        # Теги по имени колонки
        column_name = column.name.lower()
        for name_pattern, name_tags in self.name_tags.items():
            if name_pattern in column_name:
                tags.update(name_tags)
        
        # Специальные правила
        if column.pk:
            tags.add('id')
            tags.add('key')
        
        if 'email' in column_name:
            tags.add('contact')
        
        if any(word in column_name for word in ['phone', 'tel', 'mobile']):
            tags.add('contact')
        
        return list(tags)


class PostgreSQLSchemaDumper:
    """Дампер схемы PostgreSQL БД"""
    
    def __init__(self, connection_string: str):
        if not HAS_PSYCOPG2:
            raise ImportError("psycopg2 is required for PostgreSQL support. Install with: pip install psycopg2-binary")
        
        self.connection_string = connection_string
        self.pii_detector = PIIDetector()
        self.column_tagger = ColumnTagger()
    
    def connect(self):
        """Создает соединение с БД"""
        return psycopg2.connect(self.connection_string, cursor_factory=RealDictCursor)
    
    def get_schemas(self, conn) -> List[str]:
        """Получает список схем"""
        cursor = conn.cursor()
        cursor.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            ORDER BY schema_name
        """)
        return [row['schema_name'] for row in cursor.fetchall()]
    
    def get_tables(self, conn, schema: str) -> List[Tuple[str, str]]:
        """Получает список таблиц в схеме"""
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_name, 
                   COALESCE(obj_description(c.oid), '') as description
            FROM information_schema.tables t
            LEFT JOIN pg_class c ON c.relname = t.table_name
            LEFT JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.table_schema
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """, (schema,))
        return [(row['table_name'], row['description']) for row in cursor.fetchall()]
    
    def get_columns(self, conn, schema: str, table: str) -> List[ColumnInfo]:
        """Получает информацию о колонках таблицы"""
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                c.column_name,
                c.data_type,
                c.is_nullable,
                c.column_default,
                CASE WHEN tc.constraint_type = 'PRIMARY KEY' THEN true ELSE false END as is_primary_key,
                COALESCE(col_description(pgc.oid, c.ordinal_position), '') as description
            FROM information_schema.columns c
            LEFT JOIN information_schema.table_constraints tc ON 
                tc.table_name = c.table_name AND 
                tc.table_schema = c.table_schema AND 
                tc.constraint_type = 'PRIMARY KEY'
            LEFT JOIN information_schema.key_column_usage kcu ON 
                kcu.constraint_name = tc.constraint_name AND 
                kcu.column_name = c.column_name
            LEFT JOIN pg_class pgc ON pgc.relname = c.table_name
            LEFT JOIN pg_namespace pgn ON pgn.oid = pgc.relnamespace AND pgn.nspname = c.table_schema
            WHERE c.table_schema = %s AND c.table_name = %s
            ORDER BY c.ordinal_position
        """, (schema, table))
        
        columns = []
        for row in cursor.fetchall():
            column = ColumnInfo(
                name=row['column_name'],
                type=row['data_type'],
                pk=row['is_primary_key'] or False,
                nullable=row['is_nullable'] == 'YES',
                default=row['column_default'],
                description=row['description']
            )
            
            # Добавляем теги
            column.tags = self.column_tagger.get_tags(column)
            
            columns.append(column)
        
        return columns
    
    def get_foreign_keys(self, conn) -> List[ForeignKey]:
        """Получает информацию о внешних ключах"""
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                tc.constraint_name,
                tc.table_schema || '.' || tc.table_name as from_table,
                kcu.column_name as from_column,
                ccu.table_schema || '.' || ccu.table_name as to_table,
                ccu.column_name as to_column
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage ccu 
                ON tc.constraint_name = ccu.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
            ORDER BY tc.constraint_name
        """)
        
        return [
            ForeignKey(
                from_table=row['from_table'],
                from_column=row['from_column'],
                to_table=row['to_table'],
                to_column=row['to_column'],
                constraint_name=row['constraint_name']
            )
            for row in cursor.fetchall()
        ]
    
    def get_table_row_count(self, conn, schema: str, table: str) -> Optional[int]:
        """Получает примерное количество строк в таблице"""
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
            return cursor.fetchone()[0]
        except Exception as e:
            logger.warning(f"Cannot get row count for {schema}.{table}: {e}")
            return None
    
    def dump_schema(self, output_file: str = "schema.json") -> SchemaInfo:
        """Выполняет полный дамп схемы БД"""
        logger.info(f"Starting schema dump for PostgreSQL database")
        
        with self.connect() as conn:
            # Получаем схемы
            schemas = self.get_schemas(conn)
            logger.info(f"Found {len(schemas)} schemas: {schemas}")
            
            # Собираем информацию о таблицах
            tables = {}
            pii_columns = []
            
            for schema in schemas:
                schema_tables = self.get_tables(conn, schema)
                logger.info(f"Processing {len(schema_tables)} tables in schema '{schema}'")
                
                for table_name, table_desc in schema_tables:
                    full_table_name = f"{schema}.{table_name}"
                    
                    # Получаем колонки
                    columns = self.get_columns(conn, schema, table_name)
                    
                    # Проверяем на PII
                    for column in columns:
                        if self.pii_detector.detect_pii_column(column.name, table_name):
                            pii_columns.append(f"{full_table_name}.{column.name}")
                    
                    # Получаем количество строк
                    row_count = self.get_table_row_count(conn, schema, table_name)
                    
                    tables[full_table_name] = TableInfo(
                        name=table_name,
                        schema=schema,
                        columns=columns,
                        description=table_desc,
                        row_count=row_count
                    )
            
            # Получаем внешние ключи
            foreign_keys = self.get_foreign_keys(conn)
            logger.info(f"Found {len(foreign_keys)} foreign key relationships")
            
            # Создаем итоговую структуру
            schema_info = SchemaInfo(
                db_type="postgresql",
                schemas=schemas,
                tables=tables,
                foreign_keys=foreign_keys,
                pii_columns=pii_columns,
                metadata={
                    "dump_timestamp": datetime.now().isoformat(),
                    "total_tables": len(tables),
                    "total_columns": sum(len(table.columns) for table in tables.values()),
                    "total_foreign_keys": len(foreign_keys),
                    "pii_columns_count": len(pii_columns)
                }
            )
        
        # Сохраняем в файл
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(schema_info.to_dict(), f, ensure_ascii=False, indent=2)
        
        logger.info(f"Schema dump completed successfully. Saved to {output_file}")
        logger.info(f"Summary: {len(tables)} tables, {len(foreign_keys)} FKs, {len(pii_columns)} PII columns")
        
        return schema_info


class SQLiteSchemaDumper:
    """Дампер схемы SQLite БД (для совместимости)"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.pii_detector = PIIDetector()
        self.column_tagger = ColumnTagger()
    
    def dump_schema(self, output_file: str = "schema.json") -> SchemaInfo:
        """Выполняет дамп схемы SQLite БД"""
        logger.info(f"Starting schema dump for SQLite database: {self.db_path}")
        
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Получаем список таблиц
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        table_names = [row['name'] for row in cursor.fetchall()]
        
        tables = {}
        pii_columns = []
        foreign_keys = []
        
        for table_name in table_names:
            # Получаем информацию о колонках
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns_info = cursor.fetchall()
            
            columns = []
            for col_info in columns_info:
                column = ColumnInfo(
                    name=col_info['name'],
                    type=col_info['type'],
                    pk=bool(col_info['pk']),
                    nullable=not bool(col_info['notnull']),
                    default=col_info['dflt_value']
                )
                
                # Добавляем теги
                column.tags = self.column_tagger.get_tags(column)
                
                # Проверяем на PII
                if self.pii_detector.detect_pii_column(column.name, table_name):
                    pii_columns.append(f"public.{table_name}.{column.name}")
                
                columns.append(column)
            
            # Получаем внешние ключи
            cursor.execute(f"PRAGMA foreign_key_list({table_name})")
            fk_info = cursor.fetchall()
            
            for fk in fk_info:
                foreign_keys.append(ForeignKey(
                    from_table=f"public.{table_name}",
                    from_column=fk['from'],
                    to_table=f"public.{fk['table']}",
                    to_column=fk['to'],
                    constraint_name=f"fk_{table_name}_{fk['from']}"
                ))
            
            # Получаем количество строк
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
            
            tables[f"public.{table_name}"] = TableInfo(
                name=table_name,
                schema="public",
                columns=columns,
                description=None,
                row_count=row_count
            )
        
        conn.close()
        
        # Создаем итоговую структуру
        schema_info = SchemaInfo(
            db_type="sqlite",
            schemas=["public"],
            tables=tables,
            foreign_keys=foreign_keys,
            pii_columns=pii_columns,
            metadata={
                "dump_timestamp": datetime.now().isoformat(),
                "db_path": self.db_path,
                "total_tables": len(tables),
                "total_columns": sum(len(table.columns) for table in tables.values()),
                "total_foreign_keys": len(foreign_keys),
                "pii_columns_count": len(pii_columns)
            }
        )
        
        # Сохраняем в файл
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(schema_info.to_dict(), f, ensure_ascii=False, indent=2)
        
        logger.info(f"Schema dump completed successfully. Saved to {output_file}")
        return schema_info


def create_schema_dump(connection_string: str = None, db_path: str = None, output_file: str = "schema.json") -> SchemaInfo:
    """
    Универсальная функция для создания дампа схемы
    
    Args:
        connection_string: Строка подключения к PostgreSQL
        db_path: Путь к SQLite файлу
        output_file: Имя выходного JSON файла
    
    Returns:
        SchemaInfo: Информация о схеме БД
    """
    if connection_string:
        dumper = PostgreSQLSchemaDumper(connection_string)
    elif db_path:
        dumper = SQLiteSchemaDumper(db_path)
    else:
        raise ValueError("Either connection_string or db_path must be provided")
    
    return dumper.dump_schema(output_file)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Database Schema Dumper')
    parser.add_argument('--postgres', type=str, help='PostgreSQL connection string')
    parser.add_argument('--sqlite', type=str, help='SQLite database file path')
    parser.add_argument('--output', type=str, default='schema.json', help='Output JSON file')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    
    try:
        if args.postgres:
            schema_info = create_schema_dump(connection_string=args.postgres, output_file=args.output)
        elif args.sqlite:
            schema_info = create_schema_dump(db_path=args.sqlite, output_file=args.output)
        else:
            # Используем демо SQLite БД по умолчанию
            schema_info = create_schema_dump(db_path="bi_demo.db", output_file=args.output)
        
        print(f"✅ Schema dump completed successfully!")
        print(f"📁 Output file: {args.output}")
        print(f"📊 Tables: {schema_info.metadata['total_tables']}")
        print(f"🔗 Foreign Keys: {schema_info.metadata['total_foreign_keys']}")
        print(f"🔒 PII Columns: {schema_info.metadata['pii_columns_count']}")
        
    except Exception as e:
        logger.error(f"Schema dump failed: {e}")
        exit(1)
