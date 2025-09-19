"""
Централизованная система исключений для BI-GPT Agent
Обеспечивает структурированную обработку ошибок с контекстом
"""

import sys
import traceback
from typing import Optional, Dict, Any, List
from enum import Enum
from dataclasses import dataclass, field
from datetime import datetime


class ErrorSeverity(str, Enum):
    """Уровни серьезности ошибок"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ErrorCategory(str, Enum):
    """Категории ошибок"""
    VALIDATION = "validation"
    SECURITY = "security"
    DATABASE = "database"
    MODEL = "model"
    CONFIGURATION = "configuration"
    NETWORK = "network"
    PERFORMANCE = "performance"
    USER_INPUT = "user_input"
    SYSTEM = "system"


@dataclass
class ErrorContext:
    """Контекст ошибки для отладки"""
    timestamp: datetime = field(default_factory=datetime.now)
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    query: Optional[str] = None
    sql_query: Optional[str] = None
    request_id: Optional[str] = None
    component: Optional[str] = None
    function_name: Optional[str] = None
    file_name: Optional[str] = None
    line_number: Optional[int] = None
    stack_trace: Optional[str] = None
    additional_data: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Преобразует контекст в словарь для логирования"""
        return {
            'timestamp': self.timestamp.isoformat(),
            'user_id': self.user_id,
            'session_id': self.session_id,
            'query': self.query,
            'sql_query': self.sql_query,
            'request_id': self.request_id,
            'component': self.component,
            'function_name': self.function_name,
            'file_name': self.file_name,
            'line_number': self.line_number,
            'stack_trace': self.stack_trace,
            **self.additional_data
        }


class BIGPTException(Exception):
    """Базовый класс для всех исключений BI-GPT Agent"""
    
    def __init__(
        self,
        message: str,
        category: ErrorCategory = ErrorCategory.SYSTEM,
        severity: ErrorSeverity = ErrorSeverity.MEDIUM,
        error_code: Optional[str] = None,
        context: Optional[ErrorContext] = None,
        original_exception: Optional[Exception] = None,
        user_message: Optional[str] = None,
        recovery_suggestions: Optional[List[str]] = None
    ):
        super().__init__(message)
        self.message = message
        self.category = category
        self.severity = severity
        self.error_code = error_code or self._generate_error_code()
        self.context = context or ErrorContext()
        self.original_exception = original_exception
        self.user_message = user_message or self._generate_user_message()
        self.recovery_suggestions = recovery_suggestions or []
        
        # Автоматическое заполнение стека вызовов
        if not self.context.stack_trace:
            self.context.stack_trace = ''.join(traceback.format_tb(sys.exc_info()[2]))
            
        # Заполнение информации о вызывающей функции
        if not self.context.function_name:
            frame = sys._getframe(1)
            self.context.function_name = frame.f_code.co_name
            self.context.file_name = frame.f_code.co_filename
            self.context.line_number = frame.f_lineno
    
    def _generate_error_code(self) -> str:
        """Генерирует код ошибки на основе категории"""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"{self.category.value.upper()}_{timestamp}"
    
    def _generate_user_message(self) -> str:
        """Генерирует пользовательское сообщение об ошибке"""
        category_messages = {
            ErrorCategory.VALIDATION: "Данные не прошли проверку",
            ErrorCategory.SECURITY: "Обнаружена угроза безопасности",
            ErrorCategory.DATABASE: "Ошибка при работе с базой данных",
            ErrorCategory.MODEL: "Ошибка в работе ИИ-модели",
            ErrorCategory.CONFIGURATION: "Ошибка конфигурации системы",
            ErrorCategory.NETWORK: "Проблема с сетевым подключением",
            ErrorCategory.PERFORMANCE: "Превышено время выполнения",
            ErrorCategory.USER_INPUT: "Некорректный запрос пользователя",
            ErrorCategory.SYSTEM: "Внутренняя ошибка системы"
        }
        return category_messages.get(self.category, "Произошла неизвестная ошибка")
    
    def to_dict(self) -> Dict[str, Any]:
        """Преобразует исключение в словарь для логирования"""
        return {
            'error_code': self.error_code,
            'message': self.message,
            'user_message': self.user_message,
            'category': self.category.value,
            'severity': self.severity.value,
            'recovery_suggestions': self.recovery_suggestions,
            'original_exception': str(self.original_exception) if self.original_exception else None,
            'context': self.context.to_dict()
        }


# =============================================================================
# Специализированные исключения
# =============================================================================

class ValidationError(BIGPTException):
    """Ошибки валидации данных"""
    
    def __init__(self, message: str, field: Optional[str] = None, value: Any = None, **kwargs):
        self.field = field
        self.value = value
        
        # Извлекаем контекст из kwargs или создаем новый
        context = kwargs.pop('context', ErrorContext())
        context.additional_data.update({
            'validation_field': field,
            'validation_value': str(value) if value is not None else None
        })
        
        recovery_suggestions_default = [
            "Проверьте правильность введенных данных",
            "Убедитесь, что все обязательные поля заполнены"
        ]
        
        super().__init__(
            message=message,
            category=ErrorCategory.VALIDATION,
            severity=ErrorSeverity.LOW,
            context=context,
            recovery_suggestions=kwargs.pop('recovery_suggestions', recovery_suggestions_default),
            **kwargs
        )


class SecurityError(BIGPTException):
    """Ошибки безопасности"""
    
    def __init__(self, message: str, threat_type: Optional[str] = None, **kwargs):
        self.threat_type = threat_type
        
        # Извлекаем контекст из kwargs или создаем новый
        context = kwargs.pop('context', ErrorContext())
        context.additional_data['threat_type'] = threat_type
        
        recovery_suggestions_default = [
            "Измените запрос, избегая потенциально опасных команд",
            "Обратитесь к администратору если считаете, что запрос безопасен"
        ]
        
        super().__init__(
            message=message,
            category=ErrorCategory.SECURITY,
            severity=ErrorSeverity.HIGH,
            context=context,
            user_message="Запрос заблокирован из соображений безопасности",
            recovery_suggestions=kwargs.pop('recovery_suggestions', recovery_suggestions_default),
            **kwargs
        )


class SQLValidationError(ValidationError):
    """Ошибки валидации SQL"""
    
    def __init__(self, message: str, sql_query: Optional[str] = None, **kwargs):
        self.sql_query = sql_query
        
        # Извлекаем контекст из kwargs или создаем новый
        context = kwargs.pop('context', ErrorContext())
        context.sql_query = sql_query
        
        recovery_suggestions_default = [
            "Упростите запрос",
            "Проверьте правильность синтаксиса",
            "Избегайте сложных конструкций"
        ]
        
        super().__init__(
            message=message,
            field="sql_query",
            value=sql_query,
            user_message="SQL запрос не прошел проверку",
            recovery_suggestions=kwargs.pop('recovery_suggestions', recovery_suggestions_default),
            **kwargs
        )


class ModelError(BIGPTException):
    """Ошибки работы с ИИ-моделью"""
    
    def __init__(self, message: str, model_name: Optional[str] = None, **kwargs):
        self.model_name = model_name
        
        context = kwargs.get('context', ErrorContext())
        context.additional_data['model_name'] = model_name
        
        super().__init__(
            message=message,
            category=ErrorCategory.MODEL,
            severity=ErrorSeverity.HIGH,
            context=context,
            user_message="Не удалось обработать запрос с помощью ИИ",
            recovery_suggestions=[
                "Попробуйте переформулировать запрос",
                "Используйте более простые термины",
                "Повторите попытку через несколько секунд"
            ],
            **kwargs
        )


class DatabaseError(BIGPTException):
    """Ошибки работы с базой данных"""
    
    def __init__(self, message: str, query: Optional[str] = None, **kwargs):
        self.query = query
        
        context = kwargs.get('context', ErrorContext())
        context.sql_query = query
        
        super().__init__(
            message=message,
            category=ErrorCategory.DATABASE,
            severity=ErrorSeverity.HIGH,
            context=context,
            user_message="Ошибка при выполнении запроса к базе данных",
            recovery_suggestions=[
                "Проверьте подключение к базе данных",
                "Убедитесь в корректности запроса",
                "Попробуйте упростить запрос"
            ],
            **kwargs
        )


class ConfigurationError(BIGPTException):
    """Ошибки конфигурации"""
    
    def __init__(self, message: str, config_key: Optional[str] = None, **kwargs):
        self.config_key = config_key
        
        context = kwargs.get('context', ErrorContext())
        context.additional_data['config_key'] = config_key
        
        super().__init__(
            message=message,
            category=ErrorCategory.CONFIGURATION,
            severity=ErrorSeverity.CRITICAL,
            context=context,
            user_message="Ошибка конфигурации системы",
            recovery_suggestions=[
                "Проверьте настройки в .env файле",
                "Убедитесь в корректности API ключей",
                "Обратитесь к администратору"
            ],
            **kwargs
        )


class PerformanceError(BIGPTException):
    """Ошибки производительности"""
    
    def __init__(self, message: str, timeout: Optional[float] = None, **kwargs):
        self.timeout = timeout
        
        context = kwargs.get('context', ErrorContext())
        context.additional_data['timeout'] = timeout
        
        super().__init__(
            message=message,
            category=ErrorCategory.PERFORMANCE,
            severity=ErrorSeverity.MEDIUM,
            context=context,
            user_message="Запрос выполняется слишком долго",
            recovery_suggestions=[
                "Упростите запрос",
                "Уменьшите объем данных",
                "Попробуйте позже"
            ],
            **kwargs
        )


class NetworkError(BIGPTException):
    """Ошибки сети"""
    
    def __init__(self, message: str, endpoint: Optional[str] = None, **kwargs):
        self.endpoint = endpoint
        
        context = kwargs.get('context', ErrorContext())
        context.additional_data['endpoint'] = endpoint
        
        super().__init__(
            message=message,
            category=ErrorCategory.NETWORK,
            severity=ErrorSeverity.HIGH,
            context=context,
            user_message="Проблема с сетевым подключением",
            recovery_suggestions=[
                "Проверьте интернет-соединение",
                "Убедитесь в доступности сервиса",
                "Повторите попытку"
            ],
            **kwargs
        )


# =============================================================================
# Утилиты для обработки ошибок
# =============================================================================

def create_error_context(
    user_id: Optional[str] = None,
    session_id: Optional[str] = None,
    query: Optional[str] = None,
    request_id: Optional[str] = None,
    **additional_data
) -> ErrorContext:
    """Создает контекст ошибки с заполненными данными"""
    context = ErrorContext(
        user_id=user_id,
        session_id=session_id,
        query=query,
        request_id=request_id
    )
    context.additional_data.update(additional_data)
    return context


def handle_exception(
    exc: Exception,
    context: Optional[ErrorContext] = None,
    user_message: Optional[str] = None,
    recovery_suggestions: Optional[List[str]] = None
) -> BIGPTException:
    """Преобразует обычное исключение в BIGPTException"""
    
    if isinstance(exc, BIGPTException):
        return exc
    
    # Определяем категорию на основе типа исключения
    category = ErrorCategory.SYSTEM
    severity = ErrorSeverity.MEDIUM
    
    if isinstance(exc, (ValueError, TypeError)):
        category = ErrorCategory.VALIDATION
        severity = ErrorSeverity.LOW
    elif isinstance(exc, PermissionError):
        category = ErrorCategory.SECURITY
        severity = ErrorSeverity.HIGH
    elif isinstance(exc, (ConnectionError, TimeoutError)):
        category = ErrorCategory.NETWORK
        severity = ErrorSeverity.HIGH
    elif isinstance(exc, (OSError, IOError)):
        category = ErrorCategory.SYSTEM
        severity = ErrorSeverity.HIGH
    
    return BIGPTException(
        message=str(exc),
        category=category,
        severity=severity,
        context=context,
        original_exception=exc,
        user_message=user_message,
        recovery_suggestions=recovery_suggestions
    )
