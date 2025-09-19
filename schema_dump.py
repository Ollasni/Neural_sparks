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

# Опциональный импорт psycopg2 для PostgreSQL
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

logger = logging.getLogger(__name__)


@dataclass
class ColumnInfo:
    """Информация о колонке"""
    name: str
    type: str
    pk: bool = False
    nullable: bool = True
    default: Any = None
    tags: List[str] = None
    
    def __post_init__(self):
        if self.tags is None:
            self.tags = []


@dataclass
class TableInfo:
    """Информация о таблице"""
    name: str
    schema: str
    columns: List[ColumnInfo]
    indexes: List[Dict] = None
    foreign_keys: List[Dict] = None
    row_count: int = 0
    
    def __post_init__(self):
        if self.indexes is None:
            self.indexes = []
        if self.foreign_keys is None:
            self.foreign_keys = []


@dataclass
class SchemaInfo:
    """Информация о схеме БД"""
    database_name: str
    schemas: List[str]
    tables: List[TableInfo]
    total_tables: int
    total_columns: int
    db_type: str
    generated_at: datetime
    pii_columns: List[str] = None
    foreign_keys: List[Dict] = None
    
    def __post_init__(self):
        if self.pii_columns is None:
            self.pii_columns = []
        if self.foreign_keys is None:
            self.foreign_keys = []


class PIIDetector:
    """Детектор персональных данных"""
    
    def __init__(self):
        self.pii_patterns = {
            'email': r'email|mail|e_mail',
            'phone': r'phone|tel|mobile|cell',
            'ssn': r'ssn|social_security|tax_id',
            'credit_card': r'card|credit|payment',
            'name': r'name|first_name|last_name|full_name',
            'address': r'address|street|city|zip|postal',
            'birth_date': r'birth|dob|date_of_birth',
            'id_number': r'id_number|passport|license'
        }
    
    def detect_pii_column(self, column_name: str, table_name: str) -> bool:
        """Определяет, содержит ли колонка PII данные"""
        combined_text = f"{table_name}.{column_name}".lower()
        
        for pii_type, pattern in self.pii_patterns.items():
            if re.search(pattern, combined_text):
                return True
        return False


class ColumnTagger:
    """Теггер колонок для категоризации"""
    
    def get_tags(self, column: ColumnInfo) -> List[str]:
        """Возвращает теги для колонки"""
        tags = []
        col_name = column.name.lower()
        
        # Теги по типу данных
        if 'int' in column.type.lower():
            tags.append('numeric')
        elif 'varchar' in column.type.lower() or 'text' in column.type.lower():
            tags.append('text')
        elif 'date' in column.type.lower():
            tags.append('date')
        elif 'bool' in column.type.lower():
            tags.append('boolean')
        
        # Теги по названию
        if 'id' in col_name:
            tags.append('identifier')
        if 'name' in col_name:
            tags.append('name')
        if 'date' in col_name or 'time' in col_name:
            tags.append('temporal')
        if 'amount' in col_name or 'price' in col_name or 'cost' in col_name:
            tags.append('monetary')
        
        # Теги по ограничениям
        if column.pk:
            tags.append('primary_key')
        if not column.nullable:
            tags.append('required')
        
        return tags


class PostgresSchemaDumper:
    """Дампер схемы PostgreSQL БД"""
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.pii_detector = PIIDetector()
        self.column_tagger = ColumnTagger()
    
    def dump_schema(self, output_file: str = "schema.json") -> SchemaInfo:
        """Выполняет дамп схемы PostgreSQL БД"""
        if not HAS_PSYCOPG2:
            raise ImportError("psycopg2 is required for PostgreSQL schema dumping")
        
        logger.info(f"Starting schema dump for PostgreSQL database")
        
        conn = psycopg2.connect(self.connection_string)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Получаем информацию о базе данных
        cursor.execute("SELECT current_database()")
        db_name = cursor.fetchone()['current_database']
        
        # Получаем список схем
        cursor.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        """)
        schemas = [row['schema_name'] for row in cursor.fetchall()]
        
        tables = []
        all_foreign_keys = []
        all_pii_columns = []
        
        for schema in schemas:
            # Получаем таблицы в схеме
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = %s AND table_type = 'BASE TABLE'
            """, (schema,))
            
            table_names = [row['table_name'] for row in cursor.fetchall()]
            
            for table_name in table_names:
                # Получаем информацию о колонках
                cursor.execute("""
                    SELECT 
                        column_name,
                        data_type,
                        is_nullable,
                        column_default,
                        CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_primary_key
                    FROM information_schema.columns c
                    LEFT JOIN (
                        SELECT ku.column_name
                        FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage ku 
                            ON tc.constraint_name = ku.constraint_name
                        WHERE tc.constraint_type = 'PRIMARY KEY' 
                            AND tc.table_schema = %s 
                            AND tc.table_name = %s
                    ) pk ON c.column_name = pk.column_name
                    WHERE c.table_schema = %s AND c.table_name = %s
                    ORDER BY c.ordinal_position
                """, (schema, table_name, schema, table_name))
                
                columns = []
                for col in cursor.fetchall():
                    column = ColumnInfo(
                        name=col['column_name'],
                        type=col['data_type'],
                        nullable=col['is_nullable'] == 'YES',
                        default=col['column_default'],
                        pk=col['is_primary_key']
                    )
                    
                    # Добавляем теги
                    column.tags = self.column_tagger.get_tags(column)
                    
                    # Проверяем на PII
                    if self.pii_detector.detect_pii_column(column.name, table_name):
                        all_pii_columns.append(f"{schema}.{table_name}.{column.name}")
                    
                    columns.append(column)
                
                # Получаем индексы
                cursor.execute("""
                    SELECT 
                        indexname,
                        indexdef
                    FROM pg_indexes 
                    WHERE schemaname = %s AND tablename = %s
                """, (schema, table_name))
                
                indexes = []
                for idx in cursor.fetchall():
                    indexes.append({
                        'name': idx['indexname'],
                        'definition': idx['indexdef']
                    })
                
                # Получаем внешние ключи
                cursor.execute("""
                    SELECT 
                        tc.constraint_name,
                        kcu.column_name,
                        ccu.table_name AS foreign_table_name,
                        ccu.column_name AS foreign_column_name
                    FROM information_schema.table_constraints AS tc
                    JOIN information_schema.key_column_usage AS kcu
                        ON tc.constraint_name = kcu.constraint_name
                    JOIN information_schema.constraint_column_usage AS ccu
                        ON ccu.constraint_name = tc.constraint_name
                    WHERE tc.constraint_type = 'FOREIGN KEY' 
                        AND tc.table_schema = %s 
                        AND tc.table_name = %s
                """, (schema, table_name))
                
                foreign_keys = []
                for fk in cursor.fetchall():
                    fk_info = {
                        'constraint_name': fk['constraint_name'],
                        'column': fk['column_name'],
                        'referenced_table': fk['foreign_table_name'],
                        'referenced_column': fk['foreign_column_name']
                    }
                    foreign_keys.append(fk_info)
                    all_foreign_keys.append(fk_info)
                
                # Получаем количество строк
                cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"')
                row_count = cursor.fetchone()['count']
                
                table = TableInfo(
                    name=table_name,
                    schema=schema,
                    columns=columns,
                    indexes=indexes,
                    foreign_keys=foreign_keys,
                    row_count=row_count
                )
                tables.append(table)
        
        conn.close()
        
        # Создаем схему
        schema_info = SchemaInfo(
            database_name=db_name,
            schemas=schemas,
            tables=tables,
            total_tables=len(tables),
            total_columns=sum(len(t.columns) for t in tables),
            db_type="postgresql",
            generated_at=datetime.now(),
            pii_columns=all_pii_columns,
            foreign_keys=all_foreign_keys
        )
        
        # Сохраняем в файл
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(asdict(schema_info), f, indent=2, ensure_ascii=False, default=str)
        
        logger.info(f"Schema dump completed successfully. Saved to {output_file}")
        logger.info(f"Summary: {len(tables)} tables, {len(all_foreign_keys)} FKs, {len(all_pii_columns)} PII columns")
        
        return schema_info


def create_schema_dump(connection_string: str = None, output_file: str = "schema.json") -> SchemaInfo:
    """
    Создает дамп схемы базы данных
    
    Args:
        connection_string: Строка подключения к PostgreSQL
        output_file: Путь к выходному файлу
    
    Returns:
        SchemaInfo: Информация о схеме БД
    """
    if not connection_string:
        connection_string = "postgresql://olgasnissarenko@localhost:5432/bi_demo"
    
    dumper = PostgresSchemaDumper(connection_string)
    return dumper.dump_schema(output_file)


def main():
    """Основная функция для запуска из командной строки"""
    import argparse
    
    parser = argparse.ArgumentParser(description='PostgreSQL Schema Dumper')
    parser.add_argument('--connection', type=str, 
                       help='PostgreSQL connection string')
    parser.add_argument('--output', type=str, default='schema.json',
                       help='Output file path')
    
    args = parser.parse_args()
    
    try:
        schema_info = create_schema_dump(
            connection_string=args.connection,
            output_file=args.output
        )
        print(f"✅ Schema dump completed: {args.output}")
        print(f"📊 Tables: {schema_info.total_tables}, Columns: {schema_info.total_columns}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
