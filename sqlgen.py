"""
SQL Generator Module for BI-GPT Agent
Детерминированная генерация SQL из структурированного плана запроса
Поддержка PostgreSQL синтаксиса
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

from planner import QueryPlan, ColumnReference, AggregationSpec, FilterCondition, JoinSpec, SortSpec
from planner import AggregationType, FilterOperator, JoinType, SortDirection

logger = logging.getLogger(__name__)


class SQLDialect(Enum):
    """SQL диалекты"""
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"


@dataclass
class SQLGenerationOptions:
    """Опции генерации SQL"""
    dialect: SQLDialect = SQLDialect.POSTGRESQL
    use_table_aliases: bool = True
    max_limit: int = 1000
    default_limit: int = 100
    quote_identifiers: bool = False
    optimize_joins: bool = True
    include_comments: bool = False


class SQLGenerator:
    """Генератор SQL запросов из планов"""
    
    def __init__(self, options: SQLGenerationOptions = None):
        self.options = options or SQLGenerationOptions()
        
        # Диалект-специфичные функции
        self.dialect_functions = {
            SQLDialect.POSTGRESQL: {
                'date_current': 'CURRENT_DATE',
                'datetime_current': 'CURRENT_TIMESTAMP',
                'date_add': lambda date_expr, interval: f"{date_expr} + INTERVAL '{interval}'",
                'date_sub': lambda date_expr, interval: f"{date_expr} - INTERVAL '{interval}'",
                'limit': lambda limit: f"LIMIT {limit}",
                'quote_char': '"'
            },
            SQLDialect.MYSQL: {
                'date_current': 'CURDATE()',
                'datetime_current': 'NOW()',
                'date_add': lambda date_expr, interval: f"DATE_ADD({date_expr}, INTERVAL {interval})",
                'date_sub': lambda date_expr, interval: f"DATE_SUB({date_expr}, INTERVAL {interval})",
                'limit': lambda limit: f"LIMIT {limit}",
                'quote_char': '`'
            }
        }
        
        # Таблица алиасов
        self.table_aliases: Dict[str, str] = {}
        self.alias_counter = 0
    
    def generate_sql(self, plan: QueryPlan) -> str:
        """Генерирует SQL из плана запроса"""
        logger.debug(f"Generating SQL for plan with {plan.complexity_score} complexity")
        
        # Сбрасываем алиасы для нового запроса
        self.table_aliases = {}
        self.alias_counter = 0
        
        try:
            # Валидируем план
            self._validate_plan(plan)
            
            # Генерируем части запроса
            select_clause = self._generate_select_clause(plan)
            from_clause = self._generate_from_clause(plan)
            join_clauses = self._generate_join_clauses(plan)
            where_clause = self._generate_where_clause(plan)
            group_by_clause = self._generate_group_by_clause(plan)
            having_clause = self._generate_having_clause(plan)
            order_by_clause = self._generate_order_by_clause(plan)
            limit_clause = self._generate_limit_clause(plan)
            
            # Собираем итоговый запрос
            sql_parts = []
            
            if self.options.include_comments and plan.intent:
                sql_parts.append(f"-- Intent: {plan.intent}")
            
            sql_parts.append(select_clause)
            sql_parts.append(from_clause)
            
            if join_clauses:
                sql_parts.extend(join_clauses)
            
            if where_clause:
                sql_parts.append(where_clause)
            
            if group_by_clause:
                sql_parts.append(group_by_clause)
            
            if having_clause:
                sql_parts.append(having_clause)
            
            if order_by_clause:
                sql_parts.append(order_by_clause)
            
            if limit_clause:
                sql_parts.append(limit_clause)
            
            sql = '\n'.join(sql_parts)
            
            logger.debug(f"Generated SQL: {sql}")
            return sql
            
        except Exception as e:
            logger.error(f"Error generating SQL: {e}")
            raise
    
    def _validate_plan(self, plan: QueryPlan):
        """Валидирует план запроса"""
        if not plan.from_table and not plan.select_columns and not plan.aggregations:
            raise ValueError("Plan must have at least from_table or select columns/aggregations")
        
        # Проверяем совместимость колонок и таблиц
        all_tables = plan.get_all_tables()
        all_columns = plan.get_all_columns()
        
        for column in all_columns:
            if column.table not in all_tables:
                logger.warning(f"Column {column.full_name} references table not in query: {column.table}")
    
    def _generate_select_clause(self, plan: QueryPlan) -> str:
        """Генерирует SELECT часть"""
        select_items = []
        
        # Обычные колонки
        for column in plan.select_columns:
            column_expr = self._format_column_reference(column)
            if column.alias:
                column_expr += f" AS {self._quote_identifier(column.alias)}"
            select_items.append(column_expr)
        
        # Агрегации
        for agg in plan.aggregations:
            agg_expr = self._format_aggregation(agg)
            select_items.append(agg_expr)
        
        if not select_items:
            # Если нет колонок, выбираем все из главной таблицы
            if plan.from_table:
                table_alias = self._get_table_alias(plan.from_table)
                select_items.append(f"{table_alias}.*")
            else:
                select_items.append("1 as dummy")
        
        return f"SELECT {', '.join(select_items)}"
    
    def _generate_from_clause(self, plan: QueryPlan) -> str:
        """Генерирует FROM часть"""
        if not plan.from_table:
            return ""
        
        table_alias = self._get_table_alias(plan.from_table)
        from_clause = f"FROM {self._quote_identifier(plan.from_table)}"
        
        if self.options.use_table_aliases and table_alias != plan.from_table:
            from_clause += f" AS {table_alias}"
        
        return from_clause
    
    def _generate_join_clauses(self, plan: QueryPlan) -> List[str]:
        """Генерирует JOIN части"""
        join_clauses = []
        
        for join in plan.joins:
            join_clause = self._format_join(join)
            join_clauses.append(join_clause)
        
        return join_clauses
    
    def _generate_where_clause(self, plan: QueryPlan) -> Optional[str]:
        """Генерирует WHERE часть"""
        if not plan.filters:
            return None
        
        conditions = []
        for filter_cond in plan.filters:
            condition_expr = self._format_filter_condition(filter_cond)
            conditions.append(condition_expr)
        
        if conditions:
            # Объединяем условия с учетом логических операторов
            where_expr = self._combine_conditions(conditions, plan.filters)
            return f"WHERE {where_expr}"
        
        return None
    
    def _generate_group_by_clause(self, plan: QueryPlan) -> Optional[str]:
        """Генерирует GROUP BY часть"""
        if not plan.group_by:
            return None
        
        group_columns = []
        for column in plan.group_by:
            group_columns.append(self._format_column_reference(column))
        
        return f"GROUP BY {', '.join(group_columns)}"
    
    def _generate_having_clause(self, plan: QueryPlan) -> Optional[str]:
        """Генерирует HAVING часть"""
        if not plan.having:
            return None
        
        conditions = []
        for filter_cond in plan.having:
            condition_expr = self._format_filter_condition(filter_cond)
            conditions.append(condition_expr)
        
        if conditions:
            having_expr = self._combine_conditions(conditions, plan.having)
            return f"HAVING {having_expr}"
        
        return None
    
    def _generate_order_by_clause(self, plan: QueryPlan) -> Optional[str]:
        """Генерирует ORDER BY часть"""
        if not plan.order_by:
            return None
        
        order_items = []
        for sort_spec in plan.order_by:
            column_expr = self._format_column_reference(sort_spec.column)
            direction = sort_spec.direction.value
            order_items.append(f"{column_expr} {direction}")
        
        return f"ORDER BY {', '.join(order_items)}"
    
    def _generate_limit_clause(self, plan: QueryPlan) -> Optional[str]:
        """Генерирует LIMIT часть"""
        limit = plan.limit or self.options.default_limit
        
        # Применяем максимальный лимит
        if limit > self.options.max_limit:
            limit = self.options.max_limit
            logger.warning(f"Limit reduced to {self.options.max_limit}")
        
        funcs = self.dialect_functions[self.options.dialect]
        return funcs['limit'](limit)
    
    def _format_column_reference(self, column: ColumnReference) -> str:
        """Форматирует ссылку на колонку"""
        table_alias = self._get_table_alias(column.table)
        column_name = self._quote_identifier(column.column)
        
        return f"{table_alias}.{column_name}"
    
    def _format_aggregation(self, agg: AggregationSpec) -> str:
        """Форматирует агрегацию"""
        column_expr = self._format_column_reference(agg.column)
        
        if agg.distinct:
            column_expr = f"DISTINCT {column_expr}"
        
        func_name = agg.function.value
        agg_expr = f"{func_name}({column_expr})"
        
        if agg.alias:
            agg_expr += f" AS {self._quote_identifier(agg.alias)}"
        
        return agg_expr
    
    def _format_join(self, join: JoinSpec) -> str:
        """Форматирует JOIN"""
        join_type = join.join_type.value
        right_table = self._quote_identifier(join.right_table)
        right_alias = self._get_table_alias(join.right_table)
        
        join_clause = f"{join_type} JOIN {right_table}"
        
        if self.options.use_table_aliases and right_alias != join.right_table:
            join_clause += f" AS {right_alias}"
        
        # Условие соединения
        left_alias = self._get_table_alias(join.left_table)
        condition = f"{left_alias}.{self._quote_identifier(join.left_column)} = {right_alias}.{self._quote_identifier(join.right_column)}"
        
        join_clause += f" ON {condition}"
        
        return join_clause
    
    def _format_filter_condition(self, filter_cond: FilterCondition) -> str:
        """Форматирует условие фильтрации"""
        column_expr = self._format_column_reference(filter_cond.column)
        operator = filter_cond.operator.value
        value = filter_cond.value
        
        if operator in [FilterOperator.IS_NULL.value, FilterOperator.IS_NOT_NULL.value]:
            return f"{column_expr} {operator}"
        
        if operator == FilterOperator.IN.value or operator == FilterOperator.NOT_IN.value:
            if isinstance(value, list):
                formatted_values = [self._format_value(v) for v in value]
                value_list = f"({', '.join(formatted_values)})"
                return f"{column_expr} {operator} {value_list}"
        
        if operator == FilterOperator.BETWEEN.value:
            if isinstance(value, list) and len(value) == 2:
                val1 = self._format_value(value[0])
                val2 = self._format_value(value[1])
                return f"{column_expr} BETWEEN {val1} AND {val2}"
        
        if operator in [FilterOperator.LIKE.value, FilterOperator.NOT_LIKE.value]:
            formatted_value = self._format_value(value, force_string=True)
            return f"{column_expr} {operator} {formatted_value}"
        
        # Обычные операторы сравнения
        formatted_value = self._format_value(value)
        return f"{column_expr} {operator} {formatted_value}"
    
    def _combine_conditions(self, conditions: List[str], filter_specs: List[FilterCondition]) -> str:
        """Объединяет условия с логическими операторами"""
        if len(conditions) == 1:
            return conditions[0]
        
        combined = conditions[0]
        
        for i in range(1, len(conditions)):
            logical_op = "AND"  # по умолчанию
            
            if i < len(filter_specs):
                logical_op = filter_specs[i].logical_operator or "AND"
            
            combined += f" {logical_op} {conditions[i]}"
        
        return combined
    
    def _format_value(self, value: Any, force_string: bool = False) -> str:
        """Форматирует значение для SQL"""
        if value is None:
            return "NULL"
        
        if isinstance(value, str):
            # Обрабатываем специальные SQL выражения
            if value.startswith('[DATE:') and value.endswith(']'):
                # Извлекаем SQL выражение даты
                date_expr = value[6:-1]  # Убираем [DATE: и ]
                return self._format_date_expression(date_expr)
            
            # Обычная строка
            escaped_value = value.replace("'", "''")
            return f"'{escaped_value}'"
        
        if isinstance(value, (int, float)):
            if force_string:
                return f"'{value}'"
            return str(value)
        
        if isinstance(value, bool):
            if self.options.dialect == SQLDialect.POSTGRESQL:
                return str(value).upper()
            else:
                return "1" if value else "0"
        
        # Для других типов приводим к строке
        escaped_value = str(value).replace("'", "''")
        return f"'{escaped_value}'"
    
    def _format_date_expression(self, date_expr: str) -> str:
        """Форматирует выражение даты для текущего диалекта"""
        funcs = self.dialect_functions[self.options.dialect]
        
        # Заменяем стандартные выражения на диалект-специфичные
        if date_expr == 'CURRENT_DATE':
            return funcs['date_current']
        
        if date_expr == 'CURRENT_TIMESTAMP':
            return funcs['datetime_current']
        
        # Обрабатываем INTERVAL выражения
        if 'INTERVAL' in date_expr:
            if 'CURRENT_DATE - INTERVAL' in date_expr:
                # Извлекаем интервал
                import re
                match = re.search(r'CURRENT_DATE - INTERVAL (.+)', date_expr)
                if match:
                    interval = match.group(1).strip("'\"")
                    return funcs['date_sub'](funcs['date_current'], interval)
            
            if 'CURRENT_DATE + INTERVAL' in date_expr:
                match = re.search(r'CURRENT_DATE \+ INTERVAL (.+)', date_expr)
                if match:
                    interval = match.group(1).strip("'\"")
                    return funcs['date_add'](funcs['date_current'], interval)
        
        # Если не можем преобразовать, возвращаем как есть
        return date_expr
    
    def _get_table_alias(self, table_name: str) -> str:
        """Получает или создает алиас для таблицы"""
        if not self.options.use_table_aliases:
            return self._quote_identifier(table_name)
        
        if table_name not in self.table_aliases:
            # Создаем простой алиас
            if '.' in table_name:
                # Для схема.таблица берем первую букву схемы + первую букву таблицы
                schema, table = table_name.split('.', 1)
                alias = f"{schema[0]}{table[0]}".lower()
            else:
                # Для простого имени берем первую букву
                alias = table_name[0].lower()
            
            # Обеспечиваем уникальность
            base_alias = alias
            counter = 1
            while alias in self.table_aliases.values():
                alias = f"{base_alias}{counter}"
                counter += 1
            
            self.table_aliases[table_name] = alias
        
        return self.table_aliases[table_name]
    
    def _quote_identifier(self, identifier: str) -> str:
        """Квотирует идентификатор если необходимо"""
        if not self.options.quote_identifiers:
            return identifier
        
        quote_char = self.dialect_functions[self.options.dialect]['quote_char']
        return f"{quote_char}{identifier}{quote_char}"
    
    def get_generated_sql_info(self, plan: QueryPlan) -> Dict[str, Any]:
        """Возвращает информацию о сгенерированном SQL"""
        sql = self.generate_sql(plan)
        
        return {
            "sql": sql,
            "dialect": self.options.dialect.value,
            "complexity_score": plan.complexity_score,
            "table_count": len(plan.get_all_tables()),
            "column_count": len(plan.get_all_columns()),
            "join_count": len(plan.joins),
            "filter_count": len(plan.filters),
            "aggregation_count": len(plan.aggregations),
            "estimated_performance": self._estimate_performance(plan),
            "table_aliases": self.table_aliases.copy()
        }
    
    def _estimate_performance(self, plan: QueryPlan) -> str:
        """Оценивает производительность запроса"""
        score = plan.complexity_score
        
        if score <= 5:
            return "fast"
        elif score <= 15:
            return "medium"
        elif score <= 25:
            return "slow"
        else:
            return "very_slow"


def main():
    """Функция для тестирования SQL генератора"""
    import argparse
    from nl_normalizer import NLNormalizer
    from retriever import SchemaRetriever
    from planner import QueryPlanner
    
    parser = argparse.ArgumentParser(description='SQL Generator Test')
    parser.add_argument('--query', type=str, required=True, help='Query to generate SQL for')
    parser.add_argument('--schema', type=str, default='schema.json', help='Schema file')
    parser.add_argument('--dialect', type=str, default='postgresql', 
                       choices=['postgresql', 'mysql'], help='SQL dialect')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    
    # Создаем компоненты pipeline
    normalizer = NLNormalizer()
    retriever = SchemaRetriever(args.schema)
    planner = QueryPlanner(retriever)
    
    # Настройки генератора
    options = SQLGenerationOptions(
        dialect=SQLDialect(args.dialect),
        include_comments=True,
        use_table_aliases=True
    )
    generator = SQLGenerator(options)
    
    # Обрабатываем запрос
    print(f"🔤 Original query: {args.query}")
    
    # Нормализуем
    normalized_query = normalizer.normalize(args.query)
    print(f"✨ Normalized: {normalized_query.normalized}")
    
    # Планируем
    plan = planner.create_plan(normalized_query)
    print(f"📋 Plan complexity: {plan.complexity_score}")
    
    # Генерируем SQL
    sql_info = generator.get_generated_sql_info(plan)
    
    print(f"\n🔍 Generated SQL ({sql_info['dialect']}):")
    print(sql_info['sql'])
    
    print(f"\n📊 SQL Statistics:")
    print(f"  Tables: {sql_info['table_count']}")
    print(f"  Columns: {sql_info['column_count']}")
    print(f"  Joins: {sql_info['join_count']}")
    print(f"  Filters: {sql_info['filter_count']}")
    print(f"  Aggregations: {sql_info['aggregation_count']}")
    print(f"  Performance: {sql_info['estimated_performance']}")
    
    if sql_info['table_aliases']:
        print(f"\n🏷️ Table aliases:")
        for table, alias in sql_info['table_aliases'].items():
            print(f"  {table} → {alias}")


if __name__ == "__main__":
    main()

