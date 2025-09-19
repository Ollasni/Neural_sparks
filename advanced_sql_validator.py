"""
Продвинутая система валидации SQL запросов для BI-GPT Agent
Включает AST парсинг, анализ сложности и расширенные проверки безопасности
"""

import re
import ast
import time
from typing import List, Dict, Tuple, Optional, Set, Any
from dataclasses import dataclass, field
from enum import Enum
import sqlparse
from sqlparse import sql, tokens as T

from config import get_settings
from exceptions import SQLValidationError, SecurityError, PerformanceError, create_error_context
from logging_config import get_logger, log_security_event


class RiskLevel(str, Enum):
    """Уровни риска SQL запроса"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ValidationResult(str, Enum):
    """Результаты валидации"""
    ALLOWED = "allowed"
    BLOCKED = "blocked"
    WARNING = "warning"


@dataclass
class SQLAnalysis:
    """Результат анализа SQL запроса"""
    query: str
    is_valid: bool = True
    risk_level: RiskLevel = RiskLevel.LOW
    validation_result: ValidationResult = ValidationResult.ALLOWED
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    # Метрики сложности
    complexity_score: int = 0
    join_count: int = 0
    subquery_count: int = 0
    function_count: int = 0
    condition_count: int = 0
    
    # Структурный анализ
    tables_accessed: Set[str] = field(default_factory=set)
    columns_accessed: Set[str] = field(default_factory=set)
    functions_used: Set[str] = field(default_factory=set)
    keywords_used: Set[str] = field(default_factory=set)
    
    # Потенциальные угрозы
    security_issues: List[Dict[str, str]] = field(default_factory=list)
    performance_issues: List[Dict[str, str]] = field(default_factory=list)
    
    # Дополнительная информация
    estimated_execution_time: Optional[float] = None
    estimated_memory_usage: Optional[int] = None
    recommendations: List[str] = field(default_factory=list)


class AdvancedSQLValidator:
    """Продвинутый валидатор SQL запросов"""
    
    def __init__(self):
        self.settings = get_settings()
        self.security_limits = self.settings.security_limits
        self.logger = get_logger('sql_validator')
        
        # Расширенные списки опасных паттернов
        self.dangerous_keywords = {
            'critical': [
                'DROP', 'TRUNCATE', 'ALTER', 'CREATE',
                'EXEC', 'EXECUTE', 'SHUTDOWN', 'KILL', 'GRANT', 'REVOKE'
            ],
            'high': [
                'UNION', 'LOAD_FILE', 'INTO OUTFILE', 'INTO DUMPFILE',
                'BENCHMARK', 'SLEEP', 'WAITFOR', 'DBCC'
            ],
            'medium': [
                'INFORMATION_SCHEMA', 'SHOW TABLES', 'SHOW DATABASES',
                'DESCRIBE', 'EXPLAIN', 'SYSTEM'
            ],
            'allowed': [
                'SELECT', 'INSERT', 'UPDATE', 'DELETE'
            ]
        }
        
        # Системные функции и процедуры
        self.system_functions = {
            'file_operations': ['LOAD_FILE', 'INTO OUTFILE', 'INTO DUMPFILE'],
            'system_info': ['VERSION', 'USER', 'DATABASE', 'CONNECTION_ID'],
            'delay_functions': ['SLEEP', 'BENCHMARK', 'WAITFOR'],
            'stored_procedures': ['sp_', 'xp_', 'fn_', 'sys.']
        }
        
        # Паттерны SQL инъекций
        self.injection_patterns = [
            r"(\b(OR|AND)\b\s+\d+\s*=\s*\d+)",  # 1=1, 1=0
            r"(\b(OR|AND)\b\s+['\"][^'\"]*['\"]?\s*=\s*['\"][^'\"]*['\"]?)",
            r"(UNION\s+SELECT)",
            r"(;\s*(DROP|DELETE|INSERT|UPDATE))",
            r"(/\*.*?\*/)",  # SQL комментарии
            r"(--\s*.*$)",   # Однострочные комментарии
            r"(\bhex\s*\()",  # Hex encoding
            r"(\bchar\s*\()",  # Char encoding
            r"(\bconcat\s*\()",  # String concatenation for bypass
        ]
        
        # Допустимые SQL функции
        self.allowed_functions = set(self.security_limits.allowed_functions)
        
        # Схема базы данных (для валидации таблиц и колонок)
        self.known_tables = {
            'customers', 'orders', 'products', 'sales', 'inventory'
        }
        
        self.known_columns = {
            'customers': {'id', 'name', 'email', 'registration_date', 'segment'},
            'orders': {'id', 'customer_id', 'order_date', 'amount', 'status'},
            'products': {'id', 'name', 'category', 'price', 'cost'},
            'sales': {'id', 'order_id', 'product_id', 'quantity', 'revenue', 'costs'},
            'inventory': {'id', 'product_id', 'current_stock', 'warehouse'}
        }
    
    def validate_sql(self, query: str, context: Optional[Dict[str, Any]] = None) -> SQLAnalysis:
        """Основной метод валидации SQL запроса"""
        start_time = time.time()
        analysis = SQLAnalysis(query=query)
        
        try:
            # Предварительная очистка
            cleaned_query = self._clean_query(query)
            analysis.query = cleaned_query
            
            # Парсинг SQL
            parsed = self._parse_sql(cleaned_query)
            if not parsed:
                analysis.is_valid = False
                analysis.errors.append("Не удалось распарсить PostgreSQL SQL запрос")
                analysis.risk_level = RiskLevel.HIGH
                analysis.validation_result = ValidationResult.BLOCKED
                return analysis
            
            # Основные проверки
            self._check_basic_security(analysis, parsed)
            self._check_sql_injection(analysis)
            self._analyze_complexity(analysis, parsed)
            self._check_schema_compliance(analysis, parsed)
            self._check_performance_risks(analysis)
            self._check_order_by_syntax(analysis)
            self._analyze_functions(analysis, parsed)
            
            # Определение финального результата
            self._determine_final_result(analysis)
            
            # Генерация рекомендаций
            self._generate_recommendations(analysis)
            
        except Exception as e:
            self.logger.error(f"Error during SQL validation: {e}")
            analysis.is_valid = False
            analysis.errors.append(f"Ошибка валидации: {str(e)}")
            analysis.risk_level = RiskLevel.HIGH
            analysis.validation_result = ValidationResult.BLOCKED
        
        finally:
            analysis.estimated_execution_time = time.time() - start_time
        
        # Логирование результатов
        self._log_validation_result(analysis, context)
        
        return analysis
    
    def _clean_query(self, query: str) -> str:
        """Очищает PostgreSQL SQL запрос от лишних символов"""
        # Удаляем лишние пробелы и переносы строк
        query = re.sub(r'\s+', ' ', query.strip())
        
        # Удаляем комментарии (но сохраняем их для анализа)
        query = re.sub(r'--.*$', '', query, flags=re.MULTILINE)
        query = re.sub(r'/\*.*?\*/', '', query, flags=re.DOTALL)
        
        # Удаляем нежелательные префиксы
        query = self._remove_unwanted_prefixes(query)
        
        return query.strip()
    
    def _remove_unwanted_prefixes(self, query: str) -> str:
        """Удаляет нежелательные префиксы из SQL запроса"""
        unwanted_prefixes = [
            'EXPLAIN QUERY PLAN ',
            'WITH RECURSIVE ',
            'EXPLAIN ',
            'DESCRIBE ',
            'DESC ',
            'SHOW ',
            'WITH ',
        ]
        
        for prefix in unwanted_prefixes:
            if query.upper().startswith(prefix.upper()):
                query = query[len(prefix):].strip()
                logger.warning(f"Обнаружен нежелательный префикс '{prefix}' в SQL запросе")
                break
        
        # Специальная обработка для SELECT TOP (SQL Server синтаксис)
        if re.match(r'^SELECT\s+TOP\s+\d+\s+', query, re.IGNORECASE):
            match = re.match(r'^SELECT\s+TOP\s+(\d+)\s+(.*)', query, re.IGNORECASE | re.DOTALL)
            if match:
                limit_num = match.group(1)
                rest_query = match.group(2)
                query = f"SELECT {rest_query} LIMIT {limit_num}"
                logger.warning(f"Заменен SELECT TOP {limit_num} на SELECT ... LIMIT {limit_num}")
        
        return query
    
    def _parse_sql(self, query: str) -> Optional[sqlparse.sql.Statement]:
        """Парсит PostgreSQL SQL запрос"""
        try:
            parsed = sqlparse.parse(query)
            if parsed:
                return parsed[0]
        except Exception as e:
            self.logger.warning(f"SQL parsing error: {e}")
        return None
    
    def _check_basic_security(self, analysis: SQLAnalysis, parsed):
        """Проверяет базовую безопасность SQL"""
        query_upper = analysis.query.upper()
        
        # Проверяем, что запрос начинается с разрешенной команды
        allowed_commands = self.dangerous_keywords['allowed']
        query_starts_with_allowed = any(query_upper.strip().startswith(cmd) for cmd in allowed_commands)
        
        if not query_starts_with_allowed:
            analysis.errors.append(f"Разрешены только команды: {', '.join(allowed_commands)}")
            analysis.risk_level = RiskLevel.CRITICAL
            return
        
        # Проверка на критичные команды
        for keyword in self.dangerous_keywords['critical']:
            if keyword in query_upper:
                analysis.errors.append(f"Обнаружена запрещенная команда: {keyword}")
                analysis.security_issues.append({
                    'type': 'dangerous_keyword',
                    'keyword': keyword,
                    'severity': 'critical'
                })
                analysis.risk_level = RiskLevel.CRITICAL
        
        # Проверка на команды высокого риска
        for keyword in self.dangerous_keywords['high']:
            if keyword in query_upper:
                analysis.warnings.append(f"Обнаружена команда высокого риска: {keyword}")
                analysis.security_issues.append({
                    'type': 'high_risk_keyword',
                    'keyword': keyword,
                    'severity': 'high'
                })
                if analysis.risk_level == RiskLevel.LOW:
                    analysis.risk_level = RiskLevel.HIGH
        
        # Проверка на команды среднего риска
        for keyword in self.dangerous_keywords['medium']:
            if keyword in query_upper:
                analysis.warnings.append(f"Обнаружена команда среднего риска: {keyword}")
                analysis.security_issues.append({
                    'type': 'medium_risk_keyword',
                    'keyword': keyword,
                    'severity': 'medium'
                })
                if analysis.risk_level == RiskLevel.LOW:
                    analysis.risk_level = RiskLevel.MEDIUM
        
        # Дополнительные проверки для модифицирующих операций
        self._check_modifying_operations(analysis, query_upper)
    
    def _check_modifying_operations(self, analysis: SQLAnalysis, query_upper: str):
        """Проверяет модифицирующие операции (INSERT, UPDATE, DELETE)"""
        
        # Проверка для DELETE операций
        if query_upper.strip().startswith('DELETE'):
            analysis.warnings.append("Выполняется DELETE операция - будьте осторожны")
            analysis.risk_level = max(analysis.risk_level, RiskLevel.HIGH)
            
            # Проверяем наличие WHERE клаузулы для безопасности
            if 'WHERE' not in query_upper:
                analysis.errors.append("DELETE без WHERE клаузулы запрещен")
                analysis.risk_level = RiskLevel.CRITICAL
                analysis.validation_result = ValidationResult.BLOCKED
            
            # Проверяем на массовое удаление
            if 'TRUNCATE' in query_upper or 'DROP' in query_upper:
                analysis.errors.append("Обнаружены опасные операции удаления")
                analysis.risk_level = RiskLevel.CRITICAL
                analysis.validation_result = ValidationResult.BLOCKED
        
        # Проверка для UPDATE операций
        elif query_upper.strip().startswith('UPDATE'):
            analysis.warnings.append("Выполняется UPDATE операция - будьте осторожны")
            analysis.risk_level = max(analysis.risk_level, RiskLevel.HIGH)
            
            # Проверяем наличие WHERE клаузулы для безопасности
            if 'WHERE' not in query_upper:
                analysis.errors.append("UPDATE без WHERE клаузулы запрещен")
                analysis.risk_level = RiskLevel.CRITICAL
                analysis.validation_result = ValidationResult.BLOCKED
        
        # Проверка для INSERT операций
        elif query_upper.strip().startswith('INSERT'):
            analysis.warnings.append("Выполняется INSERT операция")
            analysis.risk_level = max(analysis.risk_level, RiskLevel.MEDIUM)
            
            # Проверяем на массовую вставку
            if 'SELECT' in query_upper and 'INSERT' in query_upper:
                analysis.warnings.append("Выполняется INSERT с подзапросом - проверьте данные")
    
    def _check_sql_injection(self, analysis: SQLAnalysis):
        """Проверяет на SQL инъекции"""
        query_lower = analysis.query.lower()
        
        for pattern in self.injection_patterns:
            matches = re.findall(pattern, query_lower, re.IGNORECASE | re.MULTILINE)
            if matches:
                analysis.errors.append(f"Обнаружен паттерн SQL инъекции: {pattern}")
                analysis.security_issues.append({
                    'type': 'sql_injection',
                    'pattern': pattern,
                    'matches': str(matches),
                    'severity': 'critical'
                })
                analysis.risk_level = RiskLevel.CRITICAL
    
    def _analyze_complexity(self, analysis: SQLAnalysis, parsed):
        """Анализирует сложность запроса"""
        query_upper = analysis.query.upper()
        
        # Подсчет JOIN'ов
        join_patterns = ['JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN', 'CROSS JOIN']
        for pattern in join_patterns:
            analysis.join_count += len(re.findall(pattern, query_upper))
        
        # Подсчет подзапросов
        analysis.subquery_count = query_upper.count('(SELECT')
        
        # Подсчет условий
        analysis.condition_count = query_upper.count('WHERE') + query_upper.count('HAVING')
        
        # Вычисление общей сложности
        analysis.complexity_score = (
            analysis.join_count * 2 +
            analysis.subquery_count * 3 +
            analysis.condition_count * 1
        )
        
        # Проверка лимитов
        if analysis.join_count > self.security_limits.max_joins:
            analysis.errors.append(f"Слишком много JOIN'ов: {analysis.join_count} (максимум: {self.security_limits.max_joins})")
            analysis.risk_level = RiskLevel.HIGH
        
        if analysis.subquery_count > self.security_limits.max_subqueries:
            analysis.errors.append(f"Слишком много подзапросов: {analysis.subquery_count} (максимум: {self.security_limits.max_subqueries})")
            analysis.risk_level = RiskLevel.HIGH
        
        if analysis.complexity_score > 20:
            analysis.warnings.append(f"Высокая сложность запроса: {analysis.complexity_score}")
            analysis.performance_issues.append({
                'type': 'high_complexity',
                'score': analysis.complexity_score,
                'description': 'Запрос может выполняться медленно'
            })
    
    def _check_schema_compliance(self, analysis: SQLAnalysis, parsed):
        """Проверяет соответствие схеме базы данных"""
        query_words = re.findall(r'\b\w+\b', analysis.query.lower())
        
        # Ищем упоминания таблиц
        for word in query_words:
            if word in self.known_tables:
                analysis.tables_accessed.add(word)
        
        # Проверка на обращение к неизвестным таблицам
        potential_tables = set()
        from_match = re.search(r'FROM\s+(\w+)', analysis.query, re.IGNORECASE)
        if from_match:
            potential_tables.add(from_match.group(1).lower())
        
        join_matches = re.findall(r'JOIN\s+(\w+)', analysis.query, re.IGNORECASE)
        for match in join_matches:
            potential_tables.add(match.lower())
        
        unknown_tables = potential_tables - self.known_tables
        if unknown_tables:
            analysis.warnings.append(f"Обращение к неизвестным таблицам: {', '.join(unknown_tables)}")
    
    def _check_performance_risks(self, analysis: SQLAnalysis):
        """Проверяет риски производительности"""
        query_upper = analysis.query.upper()
        
        # Проверка на отсутствие LIMIT
        if 'LIMIT' not in query_upper:
            analysis.warnings.append("Отсутствует LIMIT - может вернуть много данных")
            analysis.performance_issues.append({
                'type': 'no_limit',
                'description': 'Запрос может вернуть слишком много записей'
            })
        
        # Проверка на потенциально медленные операции
        slow_operations = ['ORDER BY', 'GROUP BY', 'DISTINCT', 'LIKE']
        for operation in slow_operations:
            if operation in query_upper:
                analysis.performance_issues.append({
                    'type': 'slow_operation',
                    'operation': operation,
                    'description': f'Операция {operation} может замедлить выполнение'
                })
        
        # Проверка на использование функций в WHERE
        if re.search(r'WHERE.*\w+\s*\(', query_upper):
            analysis.performance_issues.append({
                'type': 'function_in_where',
                'description': 'Использование функций в WHERE может замедлить запрос'
            })
    
    def _check_order_by_syntax(self, analysis: SQLAnalysis):
        """Проверяет синтаксис ORDER BY клаузулы"""
        query_upper = analysis.query.upper()
        
        if 'ORDER BY' in query_upper:
            # Ищем ORDER BY клаузулу
            order_by_match = re.search(r'ORDER\s+BY\s+([^LIMIT]+?)(?=\s+LIMIT|\s*$)', analysis.query, re.IGNORECASE | re.DOTALL)
            
            if order_by_match:
                order_by_part = order_by_match.group(1).strip()
                
                # Проверяем на неполные ссылки типа "T1." без указания колонки
                incomplete_refs = re.findall(r'\b\w+\.\s*(?=\s*[,LIMIT]|\s*$)', order_by_part)
                
                if incomplete_refs:
                    analysis.errors.append(f"Неполные ссылки в ORDER BY: {', '.join(incomplete_refs)}")
                    analysis.risk_level = RiskLevel.HIGH
                    analysis.validation_result = ValidationResult.BLOCKED
    
    def _analyze_functions(self, analysis: SQLAnalysis, parsed):
        """Анализирует используемые SQL функции"""
        # Поиск функций в запросе
        function_pattern = r'\b(\w+)\s*\('
        functions = re.findall(function_pattern, analysis.query, re.IGNORECASE)
        
        for func in functions:
            func_upper = func.upper()
            analysis.functions_used.add(func_upper)
            
            # Проверка на разрешенные функции
            if func_upper not in self.allowed_functions:
                analysis.warnings.append(f"Использование неразрешенной функции: {func}")
                analysis.security_issues.append({
                    'type': 'unauthorized_function',
                    'function': func,
                    'severity': 'medium'
                })
            
            # Проверка на системные функции
            for category, system_funcs in self.system_functions.items():
                if any(func_upper.startswith(sf) for sf in system_funcs):
                    analysis.errors.append(f"Использование системной функции: {func}")
                    analysis.security_issues.append({
                        'type': 'system_function',
                        'function': func,
                        'category': category,
                        'severity': 'high'
                    })
                    analysis.risk_level = RiskLevel.HIGH
    
    def _determine_final_result(self, analysis: SQLAnalysis):
        """Определяет финальный результат валидации"""
        # Теперь не блокируем запросы, только предупреждаем
        if analysis.errors:
            # Критические ошибки все еще блокируем
            critical_errors = [e for e in analysis.errors if any(keyword in e.upper() for keyword in ['DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE'])]
            if critical_errors:
                analysis.is_valid = False
                analysis.validation_result = ValidationResult.BLOCKED
            else:
                analysis.is_valid = True
                analysis.validation_result = ValidationResult.WARNING
        elif analysis.risk_level in [RiskLevel.HIGH, RiskLevel.CRITICAL]:
            analysis.is_valid = True
            analysis.validation_result = ValidationResult.WARNING
        elif analysis.warnings:
            analysis.validation_result = ValidationResult.WARNING
        else:
            analysis.validation_result = ValidationResult.ALLOWED
    
    def _generate_recommendations(self, analysis: SQLAnalysis):
        """Генерирует рекомендации по улучшению запроса"""
        if not analysis.query.upper().strip().endswith('LIMIT'):
            analysis.recommendations.append("Добавьте LIMIT для ограничения количества результатов")
        
        if analysis.join_count > 3:
            analysis.recommendations.append("Рассмотрите возможность упрощения запроса с меньшим количеством JOIN'ов")
        
        if analysis.subquery_count > 1:
            analysis.recommendations.append("Попробуйте переписать запрос без подзапросов")
        
        if any('slow_operation' in issue.get('type', '') for issue in analysis.performance_issues):
            analysis.recommendations.append("Рассмотрите возможность оптимизации операций сортировки и группировки")
        
        if analysis.security_issues:
            analysis.recommendations.append("Упростите запрос, избегая потенциально опасных конструкций")
    
    def _log_validation_result(self, analysis: SQLAnalysis, context: Optional[Dict[str, Any]]):
        """Логирует результат валидации"""
        log_data = {
            'query_length': len(analysis.query),
            'complexity_score': analysis.complexity_score,
            'risk_level': analysis.risk_level.value,
            'validation_result': analysis.validation_result.value,
            'errors_count': len(analysis.errors),
            'warnings_count': len(analysis.warnings),
            'security_issues_count': len(analysis.security_issues),
            'performance_issues_count': len(analysis.performance_issues)
        }
        
        if context:
            log_data.update(context)
        
        if analysis.validation_result == ValidationResult.BLOCKED:
            log_security_event(
                'sql_validation_blocked',
                log_data,
                'high' if analysis.risk_level == RiskLevel.CRITICAL else 'medium'
            )
        elif analysis.warnings:
            self.logger.warning(f"SQL validation warning", extra=log_data)
        else:
            self.logger.info(f"SQL validation successful", extra=log_data)
    
    def get_validation_summary(self, analysis: SQLAnalysis) -> str:
        """Возвращает краткое описание результатов валидации"""
        if analysis.validation_result == ValidationResult.BLOCKED:
            return f"Запрос заблокирован: {'; '.join(analysis.errors[:3])}"
        elif analysis.validation_result == ValidationResult.WARNING:
            return f"Запрос выполнен с предупреждениями: {'; '.join(analysis.warnings[:3])}"
        else:
            return "Запрос прошел все проверки безопасности"
    
    def get_risk_color(self, risk_level: RiskLevel) -> str:
        """Возвращает цвет для отображения уровня риска"""
        colors = {
            RiskLevel.LOW: "#28a745",      # Зеленый
            RiskLevel.MEDIUM: "#ffc107",   # Желтый
            RiskLevel.HIGH: "#fd7e14",     # Оранжевый
            RiskLevel.CRITICAL: "#dc3545"  # Красный
        }
        return colors.get(risk_level, "#6c757d")
    
    def get_risk_icon(self, risk_level: RiskLevel) -> str:
        """Возвращает иконку для уровня риска"""
        icons = {
            RiskLevel.LOW: "✅",
            RiskLevel.MEDIUM: "⚠️",
            RiskLevel.HIGH: "🔶",
            RiskLevel.CRITICAL: "🚨"
        }
        return icons.get(risk_level, "❓")


# Глобальный экземпляр валидатора
sql_validator = AdvancedSQLValidator()

def validate_sql_query(
    query: str,
    context: Optional[Dict[str, Any]] = None
) -> SQLAnalysis:
    """Валидирует PostgreSQL SQL запрос с расширенными проверками"""
    return sql_validator.validate_sql(query, context)

def is_sql_safe(query: str) -> Tuple[bool, List[str]]:
    """Быстрая проверка безопасности PostgreSQL SQL запроса"""
    analysis = validate_sql_query(query)
    return analysis.validation_result != ValidationResult.BLOCKED, analysis.errors
