from dataclasses import dataclass
from typing import Optional, Dict, Any
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
import logging

logger = logging.getLogger(__name__)

@dataclass
class PostgresConfig:
    host: str
    port: int
    user: str
    password: str
    database: str
    sslmode: Optional[str] = None  # "require" | "disable" | None
    sslcert: Optional[str] = None
    sslkey: Optional[str] = None
    sslrootcert: Optional[str] = None

def make_postgres_engine(cfg: PostgresConfig) -> Engine:
    """Создает движок SQLAlchemy для PostgreSQL с поддержкой SSL"""
    # Строим URL с SSL параметрами
    url = _build_database_url(cfg)
    
    try:
        # Настройки пула соединений
        engine_kwargs = {
            'pool_pre_ping': True,
            'pool_recycle': 1800,
            'pool_size': 20,
            'max_overflow': 30,
            'pool_timeout': 30
        }
        
        # Если используется SSL, добавляем дополнительные настройки
        if cfg.sslmode and cfg.sslmode != "disable":
            engine_kwargs['connect_args'] = {
                'sslmode': cfg.sslmode,
                'sslcert': cfg.sslcert,
                'sslkey': cfg.sslkey,
                'sslrootcert': cfg.sslrootcert
            }
        
        engine = create_engine(url, **engine_kwargs)
        
        # Пробное соединение для проверки
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        logger.info(f"Successfully connected to PostgreSQL at {cfg.host}:{cfg.port}/{cfg.database}")
        return engine
        
    except SQLAlchemyError as e:
        logger.error(f"PostgreSQL connection failed: {e}")
        raise RuntimeError(f"PostgreSQL connection failed: {e}") from e

def make_postgres_engine_from_config(config: Dict[str, Any]) -> Engine:
    """Создает движок PostgreSQL из конфигурации"""
    cfg = PostgresConfig(
        host=config['host'],
        port=config['port'],
        user=config['user'],
        password=config['password'],
        database=config['database'],
        sslmode=config.get('sslmode'),
        sslcert=config.get('sslcert'),
        sslkey=config.get('sslkey'),
        sslrootcert=config.get('sslrootcert')
    )
    return make_postgres_engine(cfg)

def _build_database_url(cfg: PostgresConfig) -> str:
    """Строит URL базы данных с SSL параметрами"""
    # Базовый URL
    if cfg.password:
        url = f"postgresql+psycopg2://{cfg.user}:{cfg.password}@{cfg.host}:{cfg.port}/{cfg.database}"
    else:
        url = f"postgresql+psycopg2://{cfg.user}@{cfg.host}:{cfg.port}/{cfg.database}"
    
    # Добавляем SSL параметры
    ssl_params = []
    if cfg.sslmode and cfg.sslmode != "disable":
        ssl_params.append(f"sslmode={cfg.sslmode}")
    if cfg.sslcert:
        ssl_params.append(f"sslcert={cfg.sslcert}")
    if cfg.sslkey:
        ssl_params.append(f"sslkey={cfg.sslkey}")
    if cfg.sslrootcert:
        ssl_params.append(f"sslrootcert={cfg.sslrootcert}")
    
    if ssl_params:
        url += "?" + "&".join(ssl_params)
    
    return url
