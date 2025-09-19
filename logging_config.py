"""
Система логирования уровня production для BI-GPT Agent
Поддерживает структурированное логирование, ротацию логов и различные форматы
"""

import os
import sys
import json
import logging
import logging.handlers
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, Union
try:
    from pythonjsonlogger import jsonlogger
except ImportError:
    # Fallback для случая если pythonjsonlogger не установлен
    import json
    
    class JsonFormatter(logging.Formatter):
        def format(self, record):
            log_obj = {
                'timestamp': self.formatTime(record),
                'level': record.levelname,
                'message': record.getMessage(),
                'module': record.module,
                'function': record.funcName,
                'line': record.lineno
            }
            if record.exc_info:
                log_obj['exception'] = self.formatException(record.exc_info)
            return json.dumps(log_obj, ensure_ascii=False)
    
    # Создаем псевдоним для совместимости
    class jsonlogger:
        JsonFormatter = JsonFormatter

from config import get_settings, LogLevel
from exceptions import BIGPTException, ErrorContext


class ContextFilter(logging.Filter):
    """Фильтр для добавления контекстной информации к логам"""
    
    def __init__(self):
        super().__init__()
        self.default_context = {
            'service': 'bi-gpt-agent',
            'version': '1.0.0',
            'environment': 'development'
        }
    
    def filter(self, record):
        """Добавляет контекстную информацию к записи лога"""
        settings = get_settings()
        
        # Добавляем базовую информацию
        record.service = settings.app_name
        record.version = settings.app_version
        record.environment = settings.environment.value
        record.timestamp = datetime.utcnow().isoformat()
        
        # Добавляем информацию о запросе если есть
        if hasattr(record, 'request_id'):
            record.request_id = getattr(record, 'request_id', None)
        if hasattr(record, 'user_id'):
            record.user_id = getattr(record, 'user_id', None)
        if hasattr(record, 'session_id'):
            record.session_id = getattr(record, 'session_id', None)
        
        return True


class StructuredFormatter(jsonlogger.JsonFormatter):
    """Форматтер для структурированного логирования в JSON"""
    
    def __init__(self):
        super().__init__(
            fmt='%(timestamp)s %(name)s %(levelname)s %(message)s'
        )
    
    def add_fields(self, log_record, record, message_dict):
        """Добавляет дополнительные поля к записи лога"""
        super().add_fields(log_record, record, message_dict)
        
        # Добавляем timestamp если его нет
        if 'timestamp' not in log_record:
            log_record['timestamp'] = datetime.utcnow().isoformat()
        
        # Добавляем уровень лога
        log_record['level'] = record.levelname
        
        # Добавляем информацию о модуле
        log_record['module'] = record.module
        log_record['function'] = record.funcName
        log_record['line'] = record.lineno
        
        # Добавляем контекстную информацию
        for attr in ['service', 'version', 'environment', 'request_id', 'user_id', 'session_id']:
            if hasattr(record, attr):
                log_record[attr] = getattr(record, attr)
        
        # Обрабатываем исключения
        if record.exc_info:
            log_record['exception'] = {
                'type': record.exc_info[0].__name__,
                'message': str(record.exc_info[1]),
                'traceback': self.formatException(record.exc_info)
            }


class HumanReadableFormatter(logging.Formatter):
    """Форматтер для человекочитаемых логов"""
    
    def __init__(self):
        super().__init__(
            fmt='%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    
    def format(self, record):
        """Форматирует запись лога"""
        # Добавляем цвета для консольного вывода
        if hasattr(record, 'user_id') and record.user_id:
            record.message = f"[User:{record.user_id}] {record.getMessage()}"
        
        if hasattr(record, 'request_id') and record.request_id:
            record.message = f"[Req:{record.request_id[:8]}] {record.getMessage()}"
        
        return super().format(record)


class LoggerManager:
    """Менеджер логгеров для централизованного управления"""
    
    def __init__(self):
        self._loggers: Dict[str, logging.Logger] = {}
        self._initialized = False
        self.settings = get_settings()
    
    def setup_logging(self):
        """Настраивает систему логирования"""
        if self._initialized:
            return
        
        # Создаем директорию для логов
        if self.settings.log_file:
            log_dir = Path(self.settings.log_file).parent
            log_dir.mkdir(parents=True, exist_ok=True)
        
        # Настраиваем корневой логгер
        root_logger = logging.getLogger()
        # Обрабатываем log_level как строку или enum
        if hasattr(self.settings.log_level, 'value'):
            log_level = self.settings.log_level.value
        else:
            log_level = str(self.settings.log_level)
        root_logger.setLevel(log_level)
        
        # Очищаем существующие хендлеры
        root_logger.handlers.clear()
        
        # Добавляем хендлеры
        self._setup_console_handler(root_logger)
        
        if self.settings.log_file:
            self._setup_file_handler(root_logger)
        
        # Настраиваем логгеры сторонних библиотек
        self._setup_third_party_loggers()
        
        self._initialized = True
    
    def _setup_console_handler(self, logger: logging.Logger):
        """Настраивает консольный хендлер"""
        console_handler = logging.StreamHandler(sys.stdout)
        
        if self.settings.enable_structured_logging and self.settings.is_production:
            formatter = StructuredFormatter()
        else:
            formatter = HumanReadableFormatter()
        
        console_handler.setFormatter(formatter)
        console_handler.addFilter(ContextFilter())
        
        # В разработке показываем все логи, в продакшене только WARNING+
        if self.settings.is_development:
            console_handler.setLevel(logging.DEBUG)
        else:
            console_handler.setLevel(logging.WARNING)
        
        logger.addHandler(console_handler)
    
    def _setup_file_handler(self, logger: logging.Logger):
        """Настраивает файловый хендлер с ротацией"""
        file_handler = logging.handlers.RotatingFileHandler(
            filename=self.settings.log_file,
            maxBytes=self.settings.log_max_size,
            backupCount=self.settings.log_backup_count,
            encoding='utf-8'
        )
        
        # Файловые логи всегда структурированные
        formatter = StructuredFormatter()
        file_handler.setFormatter(formatter)
        file_handler.addFilter(ContextFilter())
        # Обрабатываем log_level как строку или enum
        if hasattr(self.settings.log_level, 'value'):
            log_level = self.settings.log_level.value
        else:
            log_level = str(self.settings.log_level)
        file_handler.setLevel(log_level)
        
        logger.addHandler(file_handler)
    
    def _setup_third_party_loggers(self):
        """Настраивает логгеры сторонних библиотек"""
        # Уменьшаем уровень логирования для шумных библиотек
        noisy_loggers = [
            'urllib3.connectionpool',
            'openai',
            'httpx',
            'streamlit',
            'sqlalchemy.engine',
            'asyncio'
        ]
        
        for logger_name in noisy_loggers:
            logger = logging.getLogger(logger_name)
            if self.settings.is_production:
                logger.setLevel(logging.WARNING)
            else:
                logger.setLevel(logging.INFO)
    
    def get_logger(self, name: str) -> logging.Logger:
        """Возвращает логгер с заданным именем"""
        if not self._initialized:
            self.setup_logging()
        
        if name not in self._loggers:
            self._loggers[name] = logging.getLogger(name)
        
        return self._loggers[name]
    
    def log_exception(
        self,
        logger: logging.Logger,
        exception: Union[Exception, BIGPTException],
        context: Optional[ErrorContext] = None,
        extra_data: Optional[Dict[str, Any]] = None
    ):
        """Логирует исключение с полной информацией"""
        extra = extra_data or {}
        
        if isinstance(exception, BIGPTException):
            extra.update({
                'error_code': exception.error_code,
                'error_category': exception.category.value,
                'error_severity': exception.severity.value,
                'user_message': exception.user_message,
                'recovery_suggestions': exception.recovery_suggestions
            })
            
            if exception.context:
                extra.update({
                    'request_id': exception.context.request_id,
                    'user_id': exception.context.user_id,
                    'session_id': exception.context.session_id,
                    'query': exception.context.query,
                    'sql_query': exception.context.sql_query
                })
            
            # Определяем уровень логирования по серьезности
            if exception.severity.value == 'critical':
                log_level = logging.CRITICAL
            elif exception.severity.value == 'high':
                log_level = logging.ERROR
            elif exception.severity.value == 'medium':
                log_level = logging.WARNING
            else:
                log_level = logging.INFO
        else:
            log_level = logging.ERROR
            extra.update({
                'exception_type': type(exception).__name__
            })
        
        if context:
            extra.update(context.to_dict())
        
        logger.log(
            log_level,
            f"Exception occurred: {str(exception)}",
            exc_info=exception,
            extra=extra
        )
    
    def log_performance(
        self,
        logger: logging.Logger,
        operation: str,
        duration: float,
        success: bool = True,
        extra_data: Optional[Dict[str, Any]] = None
    ):
        """Логирует метрики производительности"""
        extra = extra_data or {}
        extra.update({
            'operation': operation,
            'duration_seconds': duration,
            'success': success,
            'performance_metric': True
        })
        
        if success:
            logger.info(f"Operation '{operation}' completed in {duration:.3f}s", extra=extra)
        else:
            logger.warning(f"Operation '{operation}' failed after {duration:.3f}s", extra=extra)
    
    def log_security_event(
        self,
        logger: logging.Logger,
        event_type: str,
        details: Dict[str, Any],
        severity: str = 'medium'
    ):
        """Логирует события безопасности"""
        extra = {
            'security_event': True,
            'event_type': event_type,
            'severity': severity,
            **details
        }
        
        if severity in ['high', 'critical']:
            log_level = logging.ERROR
        else:
            log_level = logging.WARNING
        
        logger.log(
            log_level,
            f"Security event: {event_type}",
            extra=extra
        )
    
    def log_user_action(
        self,
        logger: logging.Logger,
        action: str,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        """Логирует действия пользователей"""
        extra = {
            'user_action': True,
            'action': action,
            'user_id': user_id,
            'session_id': session_id
        }
        
        if details:
            extra.update(details)
        
        logger.info(f"User action: {action}", extra=extra)


# Глобальный экземпляр менеджера логгеров
logger_manager = LoggerManager()

# Удобные функции для получения логгеров
def get_logger(name: str) -> logging.Logger:
    """Возвращает настроенный логгер"""
    return logger_manager.get_logger(name)

def setup_logging():
    """Настраивает систему логирования"""
    logger_manager.setup_logging()

def log_exception(
    exception: Union[Exception, BIGPTException],
    logger_name: str = 'bi_gpt_agent',
    context: Optional[ErrorContext] = None,
    extra_data: Optional[Dict[str, Any]] = None
):
    """Логирует исключение"""
    logger = get_logger(logger_name)
    logger_manager.log_exception(logger, exception, context, extra_data)

def log_performance(
    operation: str,
    duration: float,
    success: bool = True,
    logger_name: str = 'performance',
    extra_data: Optional[Dict[str, Any]] = None
):
    """Логирует метрики производительности"""
    logger = get_logger(logger_name)
    logger_manager.log_performance(logger, operation, duration, success, extra_data)

def log_security_event(
    event_type: str,
    details: Dict[str, Any],
    severity: str = 'medium',
    logger_name: str = 'security'
):
    """Логирует события безопасности"""
    logger = get_logger(logger_name)
    logger_manager.log_security_event(logger, event_type, details, severity)

def log_user_action(
    action: str,
    user_id: Optional[str] = None,
    session_id: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
    logger_name: str = 'user_actions'
):
    """Логирует действия пользователей"""
    logger = get_logger(logger_name)
    logger_manager.log_user_action(logger, action, user_id, session_id, details)
