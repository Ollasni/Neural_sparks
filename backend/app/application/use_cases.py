from typing import List
from sqlalchemy.engine import Engine
from app.domain.models import SchemaOverview, Table
from app.infrastructure.repositories.schema_repository import SchemaRepository

class GetSchemaOverviewUC:
    def __init__(self, engine: Engine, schema: str = "public"):
        self.repo = SchemaRepository(engine, target_schema=schema)

    def execute(self) -> SchemaOverview:
        tables: List[Table] = self.repo.list_tables() or []  # ← защита от None
        columns_total = sum(len(t.columns or []) for t in tables)
        return SchemaOverview(
            db_type="POSTGRESQL",
            tables_count=len(tables),
            columns_count=columns_total,
            tables=tables,
        )
