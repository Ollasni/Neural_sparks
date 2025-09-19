from dataclasses import dataclass
from typing import Optional
from sqlalchemy import create_engine, text   # ← добавили text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

@dataclass
class PostgresConfig:
    host: str
    port: int
    user: str
    password: str
    database: str
    sslmode: Optional[str] = None  # "require" | "disable" | None

def make_postgres_engine(cfg: PostgresConfig) -> Engine:
    # postgresql+psycopg2://user:password@host:port/dbname?sslmode=...
    ssl_q = f"?sslmode={cfg.sslmode}" if cfg.sslmode else ""
    url = f"postgresql+psycopg2://{cfg.user}:{cfg.password}@{cfg.host}:{cfg.port}/{cfg.database}{ssl_q}"
    try:
        engine = create_engine(url, pool_pre_ping=True, pool_recycle=1800)
        # пробное соединение (SQLAlchemy 2.0 требует text())
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine
    except SQLAlchemyError as e:
        raise RuntimeError(f"PostgreSQL connection failed: {e}") from e
