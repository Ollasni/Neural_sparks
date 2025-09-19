from dataclasses import dataclass
from typing import Optional, List

@dataclass(frozen=True)
class Column:
    name: str
    data_type: str
    is_nullable: bool
    default: Optional[str]

@dataclass(frozen=True)
class Table:
    schema: str
    name: str
    columns: List[Column]

@dataclass(frozen=True)
class SchemaOverview:
    db_type: str
    tables_count: int
    columns_count: int
    tables: List[Table]
