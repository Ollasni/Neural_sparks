"""
Guardrails Module for BI-GPT Agent
Защита от галлюцинаций, PII, небезопасных запросов и валидация SQL
Обеспечение безопасности и корректности генерируемых запросов
"""

import re
import logging
import hashlib
from typing import Dict, List, Any, Optional, Tuple, Set
from dataclasses import dataclass, field
from enum import Enum
import json

logger = logging.getLogger(__name__)


class RiskLevel(Enum):
    """Уровни риска"""
    LOW = "low"
    MEDIUM = "medium"  
    HIGH = "high"
    CRITICAL = "critical"


class ViolationType(Enum):
    """Типы нарушений"""
    PII_DETECTED = "pii_detected"
    DANGEROUS_OPERATION = "dangerous_operation"
    SCHEMA_VIOLATION = "schema_violation"
    HALLUCINATION = "hallucination"
    PERFORMANCE_RISK = "performance_risk"
    DATA_LEAK = "data_leak"
    INJECTION_RISK = "injection_risk"


@dataclass
class GuardrailViolation:
    """Нарушение правил безопасности"""
    violation_type: ViolationType
    risk_level: RiskLevel
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    suggestion: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.violation_type.value,
            "risk_level": self.risk_level.value,
            "message": self.message,
            "details": self.details,
            "suggestion": self.suggestion
        }


@dataclass
class ValidationResult:
    """Результат валидации"""
    is_safe: bool
    violations: List[GuardrailViolation] = field(default_factory=list)
    confidence_score: float = 1.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def has_critical_violations(self) -> bool:
        return any(v.risk_level == RiskLevel.CRITICAL for v in self.violations)
    
    @property
    def max_risk_level(self) -> RiskLevel:
        if not self.violations:
            return RiskLevel.LOW
        
        risk_hierarchy = {
            RiskLevel.LOW: 0,
            RiskLevel.MEDIUM: 1,
            RiskLevel.HIGH: 2,
            RiskLevel.CRITICAL: 3
        }
        
        max_risk = max(self.violations, key=lambda v: risk_hierarchy[v.risk_level])
        return max_risk.risk_level


class PIIDetector:
    """Детектор персональных данных"""
    
    def __init__(self):
        # Паттерны для различных типов PII
        self.pii_patterns = {
            'email': [
                r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
                r'\bemail\b', r'\bпочта\b', r'\bmailbox\b'
            ],
            'phone': [
                r'\b\+?1?[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b',
                r'\b\d{3}-\d{2}-\d{2}\b',
                r'\bphone\b', r'\bтелефон\b', r'\bmobile\b'
            ],
            'credit_card': [
                r'\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b',
                r'\bcard\b', r'\bкарта\b', r'\bcredit\b'
            ],
            'ssn': [
                r'\b\d{3}-\d{2}-\d{4}\b',
                r'\bssn\b', r'\bинн\b', r'\bsnils\b'
            ],
            'passport': [
                r'\b[A-Z]{2}\d{7}\b',
                r'\bpassport\b', r'\bпаспорт\b'
            ],
            'name': [
                r'\b(имя|фамилия|name|firstname|lastname)\b'
            ],
            'address': [
                r'\b(адрес|address|street|улица)\b'
            ],
            'ip_address': [
                r'\b(?:\d{1,3}\.){3}\d{1,3}\b',
                r'\bip\b'
            ]
        }
        
        # Колонки, которые часто содержат PII
        self.pii_column_patterns = [
            r'.*email.*', r'.*mail.*', r'.*почта.*',
            r'.*phone.*', r'.*tel.*', r'.*телефон.*',
            r'.*name.*', r'.*имя.*', r'.*фамилия.*',
            r'.*address.*', r'.*адрес.*',
            r'.*passport.*', r'.*паспорт.*',
            r'.*card.*', r'.*карта.*',
            r'.*ssn.*', r'.*инн.*', r'.*snils.*'
        ]
    
    def detect_pii_in_text(self, text: str) -> List[Dict[str, Any]]:
        """Обнаруживает PII в тексте"""
        detected_pii = []
        text_lower = text.lower()
        
        for pii_type, patterns in self.pii_patterns.items():
            for pattern in patterns:
                matches = re.finditer(pattern, text_lower, re.IGNORECASE)
                for match in matches:
                    detected_pii.append({
                        'type': pii_type,
                        'match': match.group(),
                        'start': match.start(),
                        'end': match.end(),
                        'confidence': 0.8 if len(match.group()) > 3 else 0.6
                    })
        
        return detected_pii
    
    def detect_pii_columns(self, column_names: List[str]) -> List[str]:
        """Обнаруживает колонки, которые могут содержать PII"""
        pii_columns = []
        
        for column in column_names:
            column_lower = column.lower()
            
            for pattern in self.pii_column_patterns:
                if re.match(pattern, column_lower):
                    pii_columns.append(column)
                    break
        
        return pii_columns


class SQLSafetyValidator:
    """Валидатор безопасности SQL"""
    
    def __init__(self):
        # Опасные SQL команды
        self.dangerous_commands = [
            'DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE',
            'TRUNCATE', 'REPLACE', 'MERGE', 'UPSERT'
        ]
        
        # Системные функции и процедуры
        self.dangerous_functions = [
            'EXEC', 'EXECUTE', 'sp_', 'xp_', 'LOAD_FILE', 'INTO OUTFILE',
            'LOAD DATA', 'BULK INSERT', 'OPENROWSET'
        ]
        
        # Паттерны SQL инъекций
        self.injection_patterns = [
            r"'[\s]*[;]+",  # Завершение команды
            r"--[\s]*\w+",  # SQL комментарии
            r"/\*.*\*/",    # Блочные комментарии
            r"\bunion\s+select\b",  # UNION инъекции
            r"\bor\s+1\s*=\s*1\b",  # Простые условия
            r"\band\s+1\s*=\s*1\b",
            r"\bsleep\s*\(",  # Время-основанные атаки
            r"\bwaitfor\s+delay\b",
            r"\bbenchmark\s*\(",
            r"\bconcat\s*\(\s*char\s*\(",  # Обход фильтров
        ]
    
    def validate_sql_safety(self, sql: str) -> List[GuardrailViolation]:
        """Валидирует безопасность SQL запроса"""
        violations = []
        sql_upper = sql.upper()
        sql_lower = sql.lower()
        
        # Проверка на опасные команды
        for command in self.dangerous_commands:
            if f' {command} ' in f' {sql_upper} ' or sql_upper.startswith(f'{command} '):
                violations.append(GuardrailViolation(
                    violation_type=ViolationType.DANGEROUS_OPERATION,
                    risk_level=RiskLevel.CRITICAL,
                    message=f"Dangerous SQL command detected: {command}",
                    details={"command": command},
                    suggestion="Only SELECT queries are allowed"
                ))
        
        # Проверка на опасные функции
        for func in self.dangerous_functions:
            if func.upper() in sql_upper:
                violations.append(GuardrailViolation(
                    violation_type=ViolationType.DANGEROUS_OPERATION,
                    risk_level=RiskLevel.HIGH,
                    message=f"Dangerous function detected: {func}",
                    details={"function": func},
                    suggestion="Avoid using system functions"
                ))
        
        # Проверка на SQL инъекции
        for pattern in self.injection_patterns:
            if re.search(pattern, sql_lower, re.IGNORECASE):
                violations.append(GuardrailViolation(
                    violation_type=ViolationType.INJECTION_RISK,
                    risk_level=RiskLevel.HIGH,
                    message="Potential SQL injection pattern detected",
                    details={"pattern": pattern},
                    suggestion="Review query for malicious patterns"
                ))
        
        # Проверка на SELECT *
        if re.search(r'select\s+\*', sql_lower):
            violations.append(GuardrailViolation(
                violation_type=ViolationType.DATA_LEAK,
                risk_level=RiskLevel.MEDIUM,
                message="SELECT * is not allowed",
                details={"reason": "potential_data_exposure"},
                suggestion="Specify exact columns to select"
            ))
        
        return violations


class SchemaValidator:
    """Валидатор соответствия схеме"""
    
    def __init__(self, schema_data: Dict[str, Any]):
        self.schema_data = schema_data
        self.valid_tables = set(schema_data.get("tables", {}).keys())
        self.valid_columns = {}
        
        # Индексируем колонки по таблицам
        for table_name, table_info in schema_data.get("tables", {}).items():
            self.valid_columns[table_name] = set()
            for column in table_info.get("columns", []):
                self.valid_columns[table_name].add(column.get("name", ""))
    
    def validate_schema_compliance(self, sql: str, referenced_tables: List[str] = None, 
                                 referenced_columns: List[str] = None) -> List[GuardrailViolation]:
        """Валидирует соответствие SQL схеме БД"""
        violations = []
        
        # Извлекаем таблицы и колонки из SQL если не переданы
        if referenced_tables is None:
            referenced_tables = self._extract_tables_from_sql(sql)
        
        if referenced_columns is None:
            referenced_columns = self._extract_columns_from_sql(sql)
        
        # Проверяем существование таблиц
        for table in referenced_tables:
            if table not in self.valid_tables:
                violations.append(GuardrailViolation(
                    violation_type=ViolationType.HALLUCINATION,
                    risk_level=RiskLevel.HIGH,
                    message=f"Table '{table}' does not exist in schema",
                    details={"table": table, "available_tables": list(self.valid_tables)},
                    suggestion=f"Use one of available tables: {', '.join(sorted(self.valid_tables))}"
                ))
        
        # Проверяем существование колонок
        for column_ref in referenced_columns:
            if '.' in column_ref:
                table, column = column_ref.rsplit('.', 1)
                if table in self.valid_columns:
                    if column not in self.valid_columns[table]:
                        violations.append(GuardrailViolation(
                            violation_type=ViolationType.HALLUCINATION,
                            risk_level=RiskLevel.HIGH,
                            message=f"Column '{column}' does not exist in table '{table}'",
                            details={
                                "table": table, 
                                "column": column,
                                "available_columns": list(self.valid_columns[table])
                            },
                            suggestion=f"Use one of available columns: {', '.join(sorted(self.valid_columns[table]))}"
                        ))
        
        return violations
    
    def _extract_tables_from_sql(self, sql: str) -> List[str]:
        """Извлекает имена таблиц из SQL"""
        tables = []
        
        # Простое извлечение FROM и JOIN
        from_pattern = r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_.]*)'
        join_pattern = r'\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_.]*)'
        
        for pattern in [from_pattern, join_pattern]:
            matches = re.finditer(pattern, sql, re.IGNORECASE)
            for match in matches:
                table_name = match.group(1)
                if table_name not in tables:
                    tables.append(table_name)
        
        return tables
    
    def _extract_columns_from_sql(self, sql: str) -> List[str]:
        """Извлекает ссылки на колонки из SQL"""
        columns = []
        
        # Ищем паттерны table.column
        column_pattern = r'\b([a-zA-Z_][a-zA-Z0-9_.]*\.[a-zA-Z_][a-zA-Z0-9_]*)\b'
        matches = re.finditer(column_pattern, sql)
        
        for match in matches:
            column_ref = match.group(1)
            if column_ref not in columns:
                columns.append(column_ref)
        
        return columns


class PerformanceGuard:
    """Страж производительности"""
    
    def __init__(self):
        self.max_joins = 5
        self.max_columns = 50
        self.max_filters = 20
        self.max_order_by = 5
        self.dangerous_patterns = [
            r'\bLIKE\s+["\']%.*%["\']',  # LIKE с процентами с обеих сторон
            r'\bNOT\s+EXISTS',  # NOT EXISTS может быть медленным
            r'\bIN\s*\([^)]{100,}\)',  # Большие IN списки
        ]
    
    def validate_performance(self, sql: str, complexity_score: int = 0) -> List[GuardrailViolation]:
        """Валидирует производительность запроса"""
        violations = []
        
        # Подсчитываем количество JOIN'ов
        join_count = len(re.findall(r'\bJOIN\b', sql, re.IGNORECASE))
        if join_count > self.max_joins:
            violations.append(GuardrailViolation(
                violation_type=ViolationType.PERFORMANCE_RISK,
                risk_level=RiskLevel.HIGH,
                message=f"Too many JOINs: {join_count} (max: {self.max_joins})",
                details={"join_count": join_count, "max_joins": self.max_joins},
                suggestion="Consider simplifying the query or creating views"
            ))
        
        # Проверяем опасные паттерны
        for pattern in self.dangerous_patterns:
            if re.search(pattern, sql, re.IGNORECASE):
                violations.append(GuardrailViolation(
                    violation_type=ViolationType.PERFORMANCE_RISK,
                    risk_level=RiskLevel.MEDIUM,
                    message="Performance-risky pattern detected",
                    details={"pattern": pattern},
                    suggestion="Review pattern for performance impact"
                ))
        
        # Оценка по сложности
        if complexity_score > 30:
            violations.append(GuardrailViolation(
                violation_type=ViolationType.PERFORMANCE_RISK,
                risk_level=RiskLevel.HIGH,
                message=f"Query complexity too high: {complexity_score}",
                details={"complexity_score": complexity_score},
                suggestion="Break down into simpler queries"
            ))
        
        return violations


class Guardrails:
    """Основной класс системы защиты"""
    
    def __init__(self, schema_file: str = "schema.json"):
        # Загружаем схему
        try:
            with open(schema_file, 'r', encoding='utf-8') as f:
                self.schema_data = json.load(f)
        except Exception as e:
            logger.warning(f"Could not load schema: {e}")
            self.schema_data = {"tables": {}, "fks": [], "pii_columns": []}
        
        # Инициализируем компоненты
        self.pii_detector = PIIDetector()
        self.sql_validator = SQLSafetyValidator()
        self.schema_validator = SchemaValidator(self.schema_data)
        self.performance_guard = PerformanceGuard()
        
        # Известные PII колонки из схемы
        self.known_pii_columns = set(self.schema_data.get("pii_columns", []))
    
    def validate_query(self, user_query: str) -> ValidationResult:
        """Валидирует пользовательский запрос"""
        violations = []
        
        # Проверка на PII в запросе
        pii_found = self.pii_detector.detect_pii_in_text(user_query)
        for pii in pii_found:
            violations.append(GuardrailViolation(
                violation_type=ViolationType.PII_DETECTED,
                risk_level=RiskLevel.CRITICAL,
                message=f"Personal data detected in query: {pii['type']}",
                details=pii,
                suggestion="Remove personal information from query"
            ))
        
        # Дополнительные проверки запроса
        query_lower = user_query.lower()
        
        # Проверяем на потенциально опасные намерения
        if any(word in query_lower for word in ['delete', 'drop', 'truncate', 'удали', 'сотри']):
            violations.append(GuardrailViolation(
                violation_type=ViolationType.DANGEROUS_OPERATION,
                risk_level=RiskLevel.HIGH,
                message="Query contains potentially dangerous operations",
                details={"query": user_query},
                suggestion="Use only data retrieval queries"
            ))
        
        is_safe = not any(v.risk_level == RiskLevel.CRITICAL for v in violations)
        
        return ValidationResult(
            is_safe=is_safe,
            violations=violations,
            confidence_score=self._calculate_confidence(violations),
            metadata={"user_query": user_query}
        )
    
    def validate_sql(self, sql: str, complexity_score: int = 0, 
                    referenced_tables: List[str] = None,
                    referenced_columns: List[str] = None) -> ValidationResult:
        """Валидирует сгенерированный SQL"""
        violations = []
        
        # Проверки безопасности
        safety_violations = self.sql_validator.validate_sql_safety(sql)
        violations.extend(safety_violations)
        
        # Проверки схемы
        schema_violations = self.schema_validator.validate_schema_compliance(
            sql, referenced_tables, referenced_columns
        )
        violations.extend(schema_violations)
        
        # Проверки производительности
        performance_violations = self.performance_guard.validate_performance(sql, complexity_score)
        violations.extend(performance_violations)
        
        # Проверка на доступ к PII колонкам
        pii_violations = self._check_pii_column_access(sql)
        violations.extend(pii_violations)
        
        is_safe = not any(v.risk_level == RiskLevel.CRITICAL for v in violations)
        
        return ValidationResult(
            is_safe=is_safe,
            violations=violations,
            confidence_score=self._calculate_confidence(violations),
            metadata={"sql": sql, "complexity_score": complexity_score}
        )
    
    def _check_pii_column_access(self, sql: str) -> List[GuardrailViolation]:
        """Проверяет доступ к PII колонкам"""
        violations = []
        
        # Извлекаем колонки из SQL
        referenced_columns = self.schema_validator._extract_columns_from_sql(sql)
        
        for column_ref in referenced_columns:
            if column_ref in self.known_pii_columns:
                violations.append(GuardrailViolation(
                    violation_type=ViolationType.PII_DETECTED,
                    risk_level=RiskLevel.HIGH,
                    message=f"Access to PII column detected: {column_ref}",
                    details={"column": column_ref},
                    suggestion="Avoid selecting PII columns directly"
                ))
        
        return violations
    
    def _calculate_confidence(self, violations: List[GuardrailViolation]) -> float:
        """Вычисляет общую уверенность в безопасности"""
        if not violations:
            return 1.0
        
        # Штрафы за нарушения
        penalties = {
            RiskLevel.LOW: 0.05,
            RiskLevel.MEDIUM: 0.15,
            RiskLevel.HIGH: 0.3,
            RiskLevel.CRITICAL: 0.7
        }
        
        total_penalty = sum(penalties.get(v.risk_level, 0.1) for v in violations)
        confidence = max(0.0, 1.0 - total_penalty)
        
        return confidence
    
    def get_security_report(self, user_query: str, sql: str, 
                          complexity_score: int = 0) -> Dict[str, Any]:
        """Создает полный отчет по безопасности"""
        query_validation = self.validate_query(user_query)
        sql_validation = self.validate_sql(sql, complexity_score)
        
        # Объединяем нарушения
        all_violations = query_validation.violations + sql_validation.violations
        
        return {
            "overall_safe": query_validation.is_safe and sql_validation.is_safe,
            "query_validation": {
                "is_safe": query_validation.is_safe,
                "confidence": query_validation.confidence_score,
                "violations": [v.to_dict() for v in query_validation.violations]
            },
            "sql_validation": {
                "is_safe": sql_validation.is_safe,
                "confidence": sql_validation.confidence_score,
                "violations": [v.to_dict() for v in sql_validation.violations]
            },
            "risk_assessment": {
                "max_risk_level": max(
                    query_validation.max_risk_level.value,
                    sql_validation.max_risk_level.value
                ) if all_violations else "low",
                "total_violations": len(all_violations),
                "critical_violations": sum(1 for v in all_violations if v.risk_level == RiskLevel.CRITICAL),
                "high_violations": sum(1 for v in all_violations if v.risk_level == RiskLevel.HIGH)
            },
            "recommendations": self._generate_recommendations(all_violations)
        }
    
    def _generate_recommendations(self, violations: List[GuardrailViolation]) -> List[str]:
        """Генерирует рекомендации на основе нарушений"""
        recommendations = []
        
        # Группируем по типам нарушений
        violation_types = {}
        for violation in violations:
            vtype = violation.violation_type
            if vtype not in violation_types:
                violation_types[vtype] = []
            violation_types[vtype].append(violation)
        
        # Генерируем рекомендации
        if ViolationType.PII_DETECTED in violation_types:
            recommendations.append("Remove or mask personal identifiable information")
        
        if ViolationType.DANGEROUS_OPERATION in violation_types:
            recommendations.append("Use only SELECT queries for data retrieval")
        
        if ViolationType.HALLUCINATION in violation_types:
            recommendations.append("Verify table and column names against schema")
        
        if ViolationType.PERFORMANCE_RISK in violation_types:
            recommendations.append("Optimize query for better performance")
        
        if ViolationType.INJECTION_RISK in violation_types:
            recommendations.append("Review query for potential security vulnerabilities")
        
        return recommendations


def main():
    """Функция для тестирования Guardrails"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Guardrails Test')
    parser.add_argument('--query', type=str, help='User query to validate')
    parser.add_argument('--sql', type=str, help='SQL to validate') 
    parser.add_argument('--schema', type=str, default='schema.json', help='Schema file')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    
    # Создаем Guardrails
    guardrails = Guardrails(args.schema)
    
    if args.query and args.sql:
        # Полный отчет
        report = guardrails.get_security_report(args.query, args.sql)
        
        print("🛡️ Security Report:")
        print(f"Overall Safe: {'✅' if report['overall_safe'] else '❌'}")
        print(f"Max Risk Level: {report['risk_assessment']['max_risk_level']}")
        print(f"Total Violations: {report['risk_assessment']['total_violations']}")
        
        if report['query_validation']['violations']:
            print("\n🔍 Query Violations:")
            for violation in report['query_validation']['violations']:
                print(f"  - {violation['type']}: {violation['message']}")
        
        if report['sql_validation']['violations']:
            print("\n💾 SQL Violations:")
            for violation in report['sql_validation']['violations']:
                print(f"  - {violation['type']}: {violation['message']}")
        
        if report['recommendations']:
            print("\n💡 Recommendations:")
            for rec in report['recommendations']:
                print(f"  - {rec}")
    
    elif args.query:
        # Валидация только запроса
        result = guardrails.validate_query(args.query)
        print(f"🔍 Query Validation: {'✅ Safe' if result.is_safe else '❌ Unsafe'}")
        print(f"Confidence: {result.confidence_score:.3f}")
        
        for violation in result.violations:
            print(f"  ⚠️ {violation.violation_type.value}: {violation.message}")
    
    elif args.sql:
        # Валидация только SQL
        result = guardrails.validate_sql(args.sql)
        print(f"💾 SQL Validation: {'✅ Safe' if result.is_safe else '❌ Unsafe'}")
        print(f"Confidence: {result.confidence_score:.3f}")
        
        for violation in result.violations:
            print(f"  ⚠️ {violation.violation_type.value}: {violation.message}")
    
    else:
        print("Please provide --query or --sql to validate")


if __name__ == "__main__":
    main()

