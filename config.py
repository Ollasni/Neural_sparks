"""
Централизованная система конфигурации для BI-GPT Agent
Обеспечивает валидацию параметров и управление настройками
"""

import os
import logging
from typing import List, Optional, Dict, Any
from pathlib import Path
from dataclasses import dataclass, field
from enum import Enum

from pydantic import BaseSettings, Field, validator, SecretStr
from pydantic.env_settings import SettingsSourceCallable


class Environment(str, Enum):
    """Типы окружений"""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"


class LogLevel(str, Enum):
    """Уровни логирования"""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class ModelProvider(str, Enum):
    """Провайдеры моделей"""
    OPENAI = "openai"
    LOCAL = "local"


@dataclass
class DatabaseLimits:
    """Лимиты для базы данных"""
    max_execution_time: int = 30
    max_result_rows: int = 10000
    default_limit: int = 1000
    pool_size: int = 20
    max_overflow: int = 30
    pool_timeout: int = 30
    pool_recycle: int = 3600


@dataclass
class SecurityLimits:
    """Лимиты безопасности"""
    max_joins: int = 5
    max_subqueries: int = 3
    session_timeout: int = 3600
    max_requests_per_minute: int = 60
    allowed_functions: List[str] = field(default_factory=lambda: [
        'COUNT', 'SUM', 'AVG', 'MAX', 'MIN', 'DATE', 'CURRENT_DATE', 'CURRENT_TIMESTAMP'
    ])
    blocked_keywords: List[str] = field(default_factory=lambda: [
        'DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE', 'TRUNCATE', 
        'EXEC', 'EXECUTE', 'sp_', 'xp_'
    ])


@dataclass
class PerformanceSettings:
    """Настройки производительности"""
    enable_cache: bool = True
    cache_ttl: int = 3600
    cache_max_size: int = 1000
    model_timeout: int = 30
    max_retries: int = 3
    retry_delay: int = 1


class Settings(BaseSettings):
    """Основные настройки приложения"""
    
    # =============================================================================
    # Application Settings
    # =============================================================================
    app_name: str = Field("BI-GPT Agent", env="APP_NAME")
    app_version: str = Field("1.0.0", env="APP_VERSION")
    environment: Environment = Field(Environment.DEVELOPMENT, env="APP_ENVIRONMENT")
    debug_mode: bool = Field(False, env="DEBUG_MODE")
    
    # =============================================================================
    # API Configuration
    # =============================================================================
    
    # OpenAI settings
    openai_api_key: Optional[SecretStr] = Field(None, env="OPENAI_API_KEY")
    openai_model: str = Field("gpt-4", env="OPENAI_MODEL")
    openai_max_tokens: int = Field(500, env="OPENAI_MAX_TOKENS")
    openai_temperature: float = Field(0.0, env="OPENAI_TEMPERATURE")
    
    # Local model settings
    local_api_key: SecretStr = Field(default=None, env="LOCAL_API_KEY")
    local_base_url: str = Field(default=None, env="LOCAL_BASE_URL")
    local_model_name: str = Field("llama4scout", env="LOCAL_MODEL_NAME")
    
    # Model provider selection
    model_provider: ModelProvider = Field(ModelProvider.LOCAL, env="MODEL_PROVIDER")
    
    # =============================================================================
    # Database Configuration
    # =============================================================================
    database_url: str = Field("sqlite:///./bi_demo.db", env="DATABASE_URL")
    
    # =============================================================================
    # Security Configuration
    # =============================================================================
    enable_pii_detection: bool = Field(True, env="ENABLE_PII_DETECTION")
    
    # =============================================================================
    # Logging Configuration
    # =============================================================================
    log_level: LogLevel = Field(LogLevel.INFO, env="LOG_LEVEL")
    log_file: Optional[str] = Field("logs/bi_gpt_agent.log", env="LOG_FILE")
    log_max_size: int = Field(10485760, env="LOG_MAX_SIZE")  # 10MB
    log_backup_count: int = Field(5, env="LOG_BACKUP_COUNT")
    enable_structured_logging: bool = Field(True, env="ENABLE_STRUCTURED_LOGGING")
    
    # =============================================================================
    # Monitoring Configuration
    # =============================================================================
    health_check_interval: int = Field(60, env="HEALTH_CHECK_INTERVAL")
    enable_metrics: bool = Field(True, env="ENABLE_METRICS")
    metrics_port: int = Field(9090, env="METRICS_PORT")
    sentry_dsn: Optional[str] = Field(None, env="SENTRY_DSN")
    prometheus_enabled: bool = Field(False, env="PROMETHEUS_ENABLED")
    
    # =============================================================================
    # UI Configuration
    # =============================================================================
    streamlit_host: str = Field("0.0.0.0", env="STREAMLIT_HOST")
    streamlit_port: int = Field(8501, env="STREAMLIT_PORT")
    streamlit_theme: str = Field("light", env="STREAMLIT_THEME")
    
    # Feature flags
    enable_advanced_visualizations: bool = Field(True, env="ENABLE_ADVANCED_VISUALIZATIONS")
    enable_query_history: bool = Field(True, env="ENABLE_QUERY_HISTORY")
    enable_export_features: bool = Field(True, env="ENABLE_EXPORT_FEATURES")
    
    # =============================================================================
    # Validators
    # =============================================================================
    
    @validator('openai_temperature')
    def validate_temperature(cls, v):
        if not 0.0 <= v <= 2.0:
            raise ValueError('Temperature must be between 0.0 and 2.0')
        return v
    
    @validator('openai_max_tokens')
    def validate_max_tokens(cls, v):
        if not 1 <= v <= 4000:
            raise ValueError('Max tokens must be between 1 and 4000')
        return v
    
    @validator('log_level')
    def validate_log_level(cls, v):
        if isinstance(v, str):
            v = v.upper()
            if v not in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
                raise ValueError('Invalid log level')
        return v
    
    @validator('database_url')
    def validate_database_url(cls, v):
        if not v:
            raise ValueError('Database URL cannot be empty')
        return v
    
    @validator('local_base_url')
    def validate_base_url(cls, v):
        if not v.startswith(('http://', 'https://')):
            raise ValueError('Base URL must start with http:// or https://')
        return v
    
    # =============================================================================
    # Properties
    # =============================================================================
    
    @property
    def database_limits(self) -> DatabaseLimits:
        """Возвращает лимиты для базы данных"""
        return DatabaseLimits(
            max_execution_time=int(os.getenv('MAX_QUERY_EXECUTION_TIME', 30)),
            max_result_rows=int(os.getenv('MAX_RESULT_ROWS', 10000)),
            default_limit=int(os.getenv('DEFAULT_RESULT_LIMIT', 1000)),
            pool_size=int(os.getenv('DATABASE_POOL_SIZE', 20)),
            max_overflow=int(os.getenv('DATABASE_MAX_OVERFLOW', 30)),
            pool_timeout=int(os.getenv('DATABASE_POOL_TIMEOUT', 30)),
            pool_recycle=int(os.getenv('DATABASE_POOL_RECYCLE', 3600))
        )
    
    @property
    def security_limits(self) -> SecurityLimits:
        """Возвращает лимиты безопасности"""
        allowed_functions = os.getenv('ALLOWED_SQL_FUNCTIONS', '').split(',')
        if not allowed_functions or allowed_functions == ['']:
            allowed_functions = ['COUNT', 'SUM', 'AVG', 'MAX', 'MIN', 'DATE', 'CURRENT_DATE', 'CURRENT_TIMESTAMP']
        
        blocked_keywords = os.getenv('BLOCKED_KEYWORDS', '').split(',')
        if not blocked_keywords or blocked_keywords == ['']:
            blocked_keywords = ['DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE', 'TRUNCATE', 'EXEC', 'EXECUTE', 'sp_', 'xp_']
        
        return SecurityLimits(
            max_joins=int(os.getenv('MAX_JOINS_ALLOWED', 5)),
            max_subqueries=int(os.getenv('MAX_SUBQUERIES_ALLOWED', 3)),
            session_timeout=int(os.getenv('SESSION_TIMEOUT', 3600)),
            max_requests_per_minute=int(os.getenv('MAX_REQUESTS_PER_MINUTE', 60)),
            allowed_functions=[f.strip().upper() for f in allowed_functions],
            blocked_keywords=[k.strip().upper() for k in blocked_keywords]
        )
    
    @property
    def performance(self) -> PerformanceSettings:
        """Возвращает настройки производительности"""
        return PerformanceSettings(
            enable_cache=os.getenv('ENABLE_QUERY_CACHE', 'true').lower() == 'true',
            cache_ttl=int(os.getenv('CACHE_TTL_SECONDS', 3600)),
            cache_max_size=int(os.getenv('CACHE_MAX_SIZE', 1000)),
            model_timeout=int(os.getenv('MODEL_TIMEOUT_SECONDS', 30)),
            max_retries=int(os.getenv('MAX_RETRIES', 3)),
            retry_delay=int(os.getenv('RETRY_DELAY_SECONDS', 1))
        )
    
    @property
    def is_production(self) -> bool:
        """Проверяет, является ли окружение продакшном"""
        return self.environment == Environment.PRODUCTION
    
    @property
    def is_development(self) -> bool:
        """Проверяет, является ли окружение разработкой"""
        return self.environment == Environment.DEVELOPMENT
    
    def get_api_key(self) -> str:
        """Возвращает API ключ в зависимости от провайдера"""
        if self.model_provider == ModelProvider.OPENAI:
            if not self.openai_api_key:
                # Пытаемся получить из переменных окружения
                import os
                api_key = os.getenv("OPENAI_API_KEY")
                if not api_key:
                    raise ValueError("OpenAI API key is required. Set OPENAI_API_KEY environment variable")
                return api_key
            return self.openai_api_key.get_secret_value()
        else:
            if not self.local_api_key:
                # Пытаемся получить из переменных окружения
                import os
                api_key = os.getenv("LOCAL_API_KEY")
                if not api_key:
                    raise ValueError("Local model API key is required. Set LOCAL_API_KEY environment variable")
                return api_key
            return self.local_api_key.get_secret_value()
    
    def get_model_config(self) -> Dict[str, Any]:
        """Возвращает конфигурацию модели"""
        if self.model_provider == ModelProvider.OPENAI:
            return {
                'api_key': self.get_api_key(),
                'model': self.openai_model,
                'max_tokens': self.openai_max_tokens,
                'temperature': self.openai_temperature,
                'base_url': None
            }
        else:
            # Получаем base_url из переменных окружения если не задан
            import os
            base_url = self.local_base_url or os.getenv("LOCAL_BASE_URL")
            if not base_url:
                raise ValueError("Local model base URL is required. Set LOCAL_BASE_URL environment variable")
                
            return {
                'api_key': self.get_api_key(),
                'model': self.local_model_name,
                'max_tokens': self.openai_max_tokens,  # Используем те же лимиты
                'temperature': self.openai_temperature,
                'base_url': base_url
            }
    
    # =============================================================================
    # Configuration
    # =============================================================================
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        validate_assignment = True


class ConfigManager:
    """Менеджер конфигурации с дополнительными возможностями"""
    
    def __init__(self):
        self._settings: Optional[Settings] = None
        self._config_file_path = Path(".env")
    
    @property
    def settings(self) -> Settings:
        """Возвращает настройки (singleton)"""
        if self._settings is None:
            self._settings = Settings()
        return self._settings
    
    def reload_config(self):
        """Перезагружает конфигурацию"""
        self._settings = None
        return self.settings
    
    def validate_config(self) -> List[str]:
        """Валидирует конфигурацию и возвращает список ошибок"""
        errors = []
        
        try:
            settings = self.settings
            
            # Проверка обязательных параметров для продакшена
            if settings.is_production:
                if not settings.openai_api_key and settings.model_provider == ModelProvider.OPENAI:
                    errors.append("OpenAI API key is required in production")
                
                if settings.debug_mode:
                    errors.append("Debug mode should be disabled in production")
                
                if settings.log_level == LogLevel.DEBUG:
                    errors.append("Debug logging should be disabled in production")
            
            # Проверка доступности файла лога
            if settings.log_file:
                log_dir = Path(settings.log_file).parent
                if not log_dir.exists():
                    try:
                        log_dir.mkdir(parents=True, exist_ok=True)
                    except Exception as e:
                        errors.append(f"Cannot create log directory: {e}")
            
            # Проверка сетевых настроек
            if settings.model_provider == ModelProvider.LOCAL:
                if not settings.local_base_url:
                    errors.append("Local base URL is required when using local provider")
            
        except Exception as e:
            errors.append(f"Configuration validation error: {e}")
        
        return errors
    
    def create_example_env(self, force: bool = False):
        """Создает пример .env файла"""
        example_path = Path(".env.example")
        
        if example_path.exists() and not force:
            return
        
        example_content = """# BI-GPT Agent Configuration
# Copy this file to .env and fill in your values

# Application
APP_ENVIRONMENT=development
DEBUG_MODE=false
LOG_LEVEL=INFO

# OpenAI (optional)
OPENAI_API_KEY=your_openai_api_key_here
OPENAI_MODEL=gpt-4

# Local Model (default)
LOCAL_API_KEY=your_local_api_key_here
LOCAL_BASE_URL=https://your_model_server.com/v1
MODEL_PROVIDER=local

# Database
DATABASE_URL=sqlite:///./bi_demo.db

# Security
ENABLE_PII_DETECTION=true
MAX_JOINS_ALLOWED=5

# Performance
ENABLE_QUERY_CACHE=true
CACHE_TTL_SECONDS=3600

# Monitoring
ENABLE_METRICS=true
HEALTH_CHECK_INTERVAL=60
"""
        
        with open(example_path, 'w', encoding='utf-8') as f:
            f.write(example_content)
    
    def get_config_summary(self) -> Dict[str, Any]:
        """Возвращает сводку конфигурации для отладки"""
        settings = self.settings
        
        return {
            'app': {
                'name': settings.app_name,
                'version': settings.app_version,
                'environment': settings.environment,
                'debug': settings.debug_mode
            },
            'model': {
                'provider': settings.model_provider,
                'model_name': settings.openai_model if settings.model_provider == ModelProvider.OPENAI else settings.local_model_name,
                'has_api_key': bool(settings.get_api_key())
            },
            'database': {
                'url': settings.database_url,
                'limits': settings.database_limits.__dict__
            },
            'security': {
                'pii_detection': settings.enable_pii_detection,
                'limits': settings.security_limits.__dict__
            },
            'logging': {
                'level': settings.log_level,
                'file': settings.log_file,
                'structured': settings.enable_structured_logging
            }
        }


# Глобальный экземпляр менеджера конфигурации
config_manager = ConfigManager()

# Удобные алиасы
def get_settings() -> Settings:
    """Возвращает текущие настройки"""
    return config_manager.settings

def reload_config() -> Settings:
    """Перезагружает и возвращает настройки"""
    return config_manager.reload_config()

def validate_config() -> List[str]:
    """Валидирует конфигурацию"""
    return config_manager.validate_config()
