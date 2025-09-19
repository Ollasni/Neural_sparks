from typing import List
from sqlalchemy import text
from sqlalchemy.engine import Engine, Row
from app.domain.models import Table, Column

class SchemaRepository:
    """
    Репозиторий для чтения схемы БД (PostgreSQL).
    """

    def __init__(self, engine: Engine, target_schema: str = "public"):
        self.engine = engine
        self.target_schema = target_schema

    def list_tables(self) -> List[Table]:
        tables_sql = text("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE' AND table_schema = :schema
        ORDER BY table_name;
        """)

        columns_sql = text("""
        SELECT
            c.table_schema,
            c.table_name,
            c.column_name,
            c.is_nullable,
            c.data_type,
            pg_get_expr(ad.adbin, ad.adrelid) AS column_default
        FROM information_schema.columns c
        LEFT JOIN pg_catalog.pg_attribute pa
          ON pa.attname = c.column_name
         AND pa.attrelid = (quote_ident(c.table_schema)||'.'||quote_ident(c.table_name))::regclass
        LEFT JOIN pg_catalog.pg_attrdef ad
          ON ad.adrelid = pa.attrelid AND ad.adnum = pa.attnum
        WHERE c.table_schema = :schema
        ORDER BY c.table_name, c.ordinal_position;
        """)

        try:
            with self.engine.connect() as conn:
                tbl_rows: List[Row] = list(conn.execute(tables_sql, {"schema": self.target_schema}))
                col_rows: List[Row] = list(conn.execute(columns_sql, {"schema": self.target_schema}))
        except Exception:
            # если что-то пошло не так — вернем пустой список, чтобы UI не падал
            return []

        # сгруппировать колонки по таблицам
        by_table = {}
        for r in col_rows:
            m = r._mapping
            key = (m["table_schema"], m["table_name"])
            by_table.setdefault(key, []).append(
                Column(
                    name=m["column_name"],
                    data_type=str(m["data_type"]).upper(),
                    is_nullable=(m["is_nullable"] == "YES"),
                    default=m["column_default"],
                )
            )

        tables: List[Table] = []
        for r in tbl_rows:
            m = r._mapping
            key = (m["table_schema"], m["table_name"])
            tables.append(
                Table(
                    schema=m["table_schema"],
                    name=m["table_name"],
                    columns=by_table.get(key, []),
                )
            )
        return tables
