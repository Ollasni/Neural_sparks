"""
Query Planner Module for BI-GPT Agent
Преобразование нормализованных NL запросов в структурированный план запроса
Валидация с помощью Pydantic и семантический анализ
"""

import json
import logging
from typing import Dict, List, Any, Optional, Union, Literal
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime

from pydantic import BaseModel, Field, validator, root_validator
from pydantic.types import StrictStr, StrictInt, StrictFloat

from nl_normalizer import NormalizedQuery, Language
from retriever import SchemaRetriever, SearchResult

logger = logging.getLogger(__name__)


class AggregationType(str, Enum):
    """Типы агрегаций"""
    COUNT = "COUNT"
    SUM = "SUM"
    AVG = "AVG"
    MIN = "MIN"
    MAX = "MAX"
    COUNT_DISTINCT = "COUNT_DISTINCT"


class FilterOperator(str, Enum):
    """Операторы фильтрации"""
    EQUALS = "="
    NOT_EQUALS = "!="
    GREATER_THAN = ">"
    GREATER_THAN_OR_EQUAL = ">="
    LESS_THAN = "<"
    LESS_THAN_OR_EQUAL = "<="
    IN = "IN"
    NOT_IN = "NOT IN"
    LIKE = "LIKE"
    NOT_LIKE = "NOT LIKE"
    IS_NULL = "IS NULL"
    IS_NOT_NULL = "IS NOT NULL"
    BETWEEN = "BETWEEN"


class JoinType(str, Enum):
    """Типы соединений"""
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"


class SortDirection(str, Enum):
    """Направления сортировки"""
    ASC = "ASC"
    DESC = "DESC"


class ColumnReference(BaseModel):
    """Ссылка на колонку"""
    table: StrictStr = Field(..., description="Имя таблицы")
    column: StrictStr = Field(..., description="Имя колонки")
    alias: Optional[StrictStr] = Field(None, description="Псевдоним колонки")
    
    @property
    def full_name(self) -> str:
        """Полное имя колонки"""
        return f"{self.table}.{self.column}"
    
    def __str__(self) -> str:
        return self.full_name


class AggregationSpec(BaseModel):
    """Спецификация агрегации"""
    function: AggregationType = Field(..., description="Функция агрегации")
    column: ColumnReference = Field(..., description="Колонка для агрегации")
    alias: Optional[StrictStr] = Field(None, description="Псевдоним результата")
    distinct: bool = Field(False, description="Использовать DISTINCT")
    
    @validator('alias', always=True)
    def generate_alias_if_none(cls, v, values):
        if v is None and 'function' in values and 'column' in values:
            func = values['function'].lower()
            col = values['column'].column
            return f"{func}_{col}"
        return v


class FilterCondition(BaseModel):
    """Условие фильтрации"""
    column: ColumnReference = Field(..., description="Колонка для фильтрации")
    operator: FilterOperator = Field(..., description="Оператор сравнения")
    value: Union[StrictStr, StrictInt, StrictFloat, List[Any]] = Field(..., description="Значение для сравнения")
    logical_operator: Optional[Literal["AND", "OR"]] = Field("AND", description="Логический оператор с предыдущим условием")
    
    @validator('value')
    def validate_value_for_operator(cls, v, values):
        if 'operator' in values:
            op = values['operator']
            if op in [FilterOperator.IN, FilterOperator.NOT_IN]:
                if not isinstance(v, list):
                    raise ValueError(f"Operator {op} requires list value")
            elif op == FilterOperator.BETWEEN:
                if not isinstance(v, list) or len(v) != 2:
                    raise ValueError(f"Operator {op} requires list of 2 values")
            elif op in [FilterOperator.IS_NULL, FilterOperator.IS_NOT_NULL]:
                # Для NULL проверок значение не нужно
                return None
        return v


class JoinSpec(BaseModel):
    """Спецификация соединения"""
    left_table: StrictStr = Field(..., description="Левая таблица")
    right_table: StrictStr = Field(..., description="Правая таблица")
    left_column: StrictStr = Field(..., description="Колонка левой таблицы")
    right_column: StrictStr = Field(..., description="Колонка правой таблицы")
    join_type: JoinType = Field(JoinType.INNER, description="Тип соединения")
    
    @property
    def join_condition(self) -> str:
        """Условие соединения"""
        return f"{self.left_table}.{self.left_column} = {self.right_table}.{self.right_column}"


class SortSpec(BaseModel):
    """Спецификация сортировки"""
    column: ColumnReference = Field(..., description="Колонка для сортировки")
    direction: SortDirection = Field(SortDirection.ASC, description="Направление сортировки")


class QueryPlan(BaseModel):
    """Структурированный план запроса"""
    
    # Базовые компоненты
    select_columns: List[ColumnReference] = Field(default_factory=list, description="Колонки для выборки")
    aggregations: List[AggregationSpec] = Field(default_factory=list, description="Агрегации")
    from_table: Optional[StrictStr] = Field(None, description="Основная таблица")
    joins: List[JoinSpec] = Field(default_factory=list, description="Соединения")
    filters: List[FilterCondition] = Field(default_factory=list, description="Условия фильтрации")
    group_by: List[ColumnReference] = Field(default_factory=list, description="Группировка")
    having: List[FilterCondition] = Field(default_factory=list, description="Условия HAVING")
    order_by: List[SortSpec] = Field(default_factory=list, description="Сортировка")
    limit: Optional[StrictInt] = Field(None, description="Ограничение количества строк")
    
    # Метаданные
    intent: Optional[StrictStr] = Field(None, description="Намерение пользователя")
    confidence: StrictFloat = Field(1.0, description="Уверенность в плане", ge=0, le=1)
    complexity_score: StrictInt = Field(0, description="Сложность запроса", ge=0)
    estimated_performance: Optional[StrictStr] = Field(None, description="Оценка производительности")
    
    # Контекст
    original_query: Optional[StrictStr] = Field(None, description="Оригинальный запрос")
    normalized_query: Optional[StrictStr] = Field(None, description="Нормализованный запрос")
    language: Optional[Language] = Field(None, description="Язык запроса")
    
    @root_validator
    def validate_query_consistency(cls, values):
        """Валидация консистентности плана"""
        errors = []
        
        # Проверяем наличие основной таблицы если есть колонки
        select_columns = values.get('select_columns', [])
        aggregations = values.get('aggregations', [])
        from_table = values.get('from_table')
        
        if (select_columns or aggregations) and not from_table:
            errors.append("from_table is required when select_columns or aggregations are specified")
        
        # Проверяем группировку при агрегации
        group_by = values.get('group_by', [])
        if aggregations and not group_by:
            # Если есть агрегации, но нет группировки, проверяем что нет обычных колонок
            if select_columns:
                errors.append("GROUP BY is required when mixing aggregations with regular columns")
        
        # Проверяем соединения
        joins = values.get('joins', [])
        if joins:
            # Все таблицы в соединениях должны быть связаны
            all_tables = {from_table} if from_table else set()
            for join in joins:
                all_tables.add(join.left_table)
                all_tables.add(join.right_table)
            
            # Проверяем что все колонки ссылаются на существующие таблицы
            for col in select_columns + [agg.column for agg in aggregations]:
                if col.table not in all_tables:
                    errors.append(f"Column {col.full_name} references unknown table {col.table}")
        
        if errors:
            raise ValueError(f"Query plan validation failed: {'; '.join(errors)}")
        
        return values
    
    @validator('complexity_score', always=True)
    def calculate_complexity(cls, v, values):
        """Автоматически вычисляет сложность запроса"""
        score = 0
        
        # Базовая сложность
        score += len(values.get('select_columns', []))
        score += len(values.get('aggregations', [])) * 2
        score += len(values.get('joins', [])) * 3
        score += len(values.get('filters', []))
        score += len(values.get('group_by', []))
        score += len(values.get('having', [])) * 2
        score += len(values.get('order_by', []))
        
        if values.get('limit'):
            score += 1
        
        return score
    
    def get_all_tables(self) -> List[str]:
        """Возвращает все таблицы в запросе"""
        tables = set()
        
        if self.from_table:
            tables.add(self.from_table)
        
        for join in self.joins:
            tables.add(join.left_table)
            tables.add(join.right_table)
        
        return list(tables)
    
    def get_all_columns(self) -> List[ColumnReference]:
        """Возвращает все колонки в запросе"""
        columns = []
        columns.extend(self.select_columns)
        columns.extend([agg.column for agg in self.aggregations])
        columns.extend([f.column for f in self.filters])
        columns.extend(self.group_by)
        columns.extend([h.column for h in self.having])
        columns.extend([o.column for o in self.order_by])
        
        return columns


class QueryPlanner:
    """Планировщик запросов"""
    
    def __init__(self, schema_retriever: SchemaRetriever):
        self.schema_retriever = schema_retriever
        
        # Маппинг намерений к SQL операциям
        self.intent_mappings = {
            'select': self._plan_select_query,
            'count': self._plan_count_query,
            'aggregate': self._plan_aggregate_query,
            'top': self._plan_top_query,
            'filter': self._plan_filter_query,
            'trend': self._plan_trend_query,
            'compare': self._plan_compare_query
        }
    
    def create_plan(self, normalized_query: NormalizedQuery) -> QueryPlan:
        """Создает план запроса на основе нормализованного запроса"""
        logger.info(f"Creating query plan for: {normalized_query.normalized}")
        
        # Начинаем с базового плана
        plan = QueryPlan(
            original_query=normalized_query.original,
            normalized_query=normalized_query.normalized,
            language=normalized_query.detected_language,
            intent=normalized_query.intent,
            confidence=normalized_query.confidence
        )
        
        # Анализируем бизнес-термины для определения таблиц
        relevant_tables = self._identify_tables(normalized_query)
        relevant_columns = self._identify_columns(normalized_query, relevant_tables)
        
        if not relevant_tables:
            logger.warning("No relevant tables found")
            return plan
        
        # Устанавливаем основную таблицу
        plan.from_table = relevant_tables[0]
        
        # Планируем соединения если нужно несколько таблиц
        if len(relevant_tables) > 1:
            plan.joins = self._plan_joins(relevant_tables)
        
        # Применяем планировщик в зависимости от намерения
        if normalized_query.intent in self.intent_mappings:
            planner_func = self.intent_mappings[normalized_query.intent]
            plan = planner_func(plan, normalized_query, relevant_columns)
        else:
            # Базовое планирование
            plan = self._plan_default_query(plan, normalized_query, relevant_columns)
        
        # Добавляем фильтры по датам
        plan = self._add_date_filters(plan, normalized_query)
        
        # Добавляем лимиты по числам
        plan = self._add_number_limits(plan, normalized_query)
        
        logger.info(f"Created plan with complexity score: {plan.complexity_score}")
        return plan
    
    def _identify_tables(self, normalized_query: NormalizedQuery) -> List[str]:
        """Определяет релевантные таблицы"""
        # Ищем таблицы через ретривер
        search_results = self.schema_retriever.search(
            normalized_query.normalized, 
            search_type="tables", 
            limit=5
        )
        
        tables = []
        for result in search_results:
            if result.result_type == "table":
                table_name = result.metadata.get('name', '')
                if table_name and table_name not in tables:
                    tables.append(table_name)
        
        # Также ищем по бизнес-терминам
        for term in normalized_query.business_terms:
            term_search = self.schema_retriever.search(term, search_type="tables", limit=3)
            for result in term_search:
                if result.result_type == "table":
                    table_name = result.metadata.get('name', '')
                    if table_name and table_name not in tables:
                        tables.append(table_name)
        
        return tables[:3]  # Ограничиваем максимум 3 таблицами
    
    def _identify_columns(self, normalized_query: NormalizedQuery, tables: List[str]) -> List[ColumnReference]:
        """Определяет релевантные колонки"""
        columns = []
        
        # Ищем колонки через семантический поиск
        search_results = self.schema_retriever.search(
            normalized_query.normalized,
            search_type="columns",
            limit=10
        )
        
        for result in search_results:
            if result.result_type == "column":
                table_name = result.metadata.get('table', '')
                column_name = result.metadata.get('name', '')
                
                # Проверяем что колонка из релевантной таблицы
                if any(table in table_name for table in tables):
                    col_ref = ColumnReference(
                        table=table_name,
                        column=column_name
                    )
                    if col_ref not in columns:
                        columns.append(col_ref)
        
        return columns
    
    def _plan_joins(self, tables: List[str]) -> List[JoinSpec]:
        """Планирует соединения между таблицами"""
        joins = []
        
        # Получаем предложения по соединениям от ретривера
        suggested_joins = self.schema_retriever.suggest_joins(tables)
        
        for join_info in suggested_joins:
            # Парсим имена таблиц и колонок
            from_parts = join_info['join_condition'].split(' = ')
            if len(from_parts) == 2:
                left_part = from_parts[0].strip()
                right_part = from_parts[1].strip()
                
                if '.' in left_part and '.' in right_part:
                    left_table, left_col = left_part.rsplit('.', 1)
                    right_table, right_col = right_part.rsplit('.', 1)
                    
                    join_spec = JoinSpec(
                        left_table=left_table,
                        right_table=right_table,
                        left_column=left_col,
                        right_column=right_col,
                        join_type=JoinType.INNER
                    )
                    joins.append(join_spec)
        
        return joins
    
    def _plan_select_query(self, plan: QueryPlan, normalized_query: NormalizedQuery, columns: List[ColumnReference]) -> QueryPlan:
        """Планирует обычный SELECT запрос"""
        # Добавляем все релевантные колонки
        plan.select_columns = columns[:10]  # Ограничиваем количество
        return plan
    
    def _plan_count_query(self, plan: QueryPlan, normalized_query: NormalizedQuery, columns: List[ColumnReference]) -> QueryPlan:
        """Планирует COUNT запрос"""
        # Для count запросов создаем агрегацию
        if plan.from_table:
            # Ищем подходящую колонку для подсчета (обычно ID)
            count_column = None
            for col in columns:
                if 'id' in col.column.lower() or col.column.lower().endswith('_id'):
                    count_column = col
                    break
            
            if not count_column and columns:
                count_column = columns[0]
            
            if count_column:
                plan.aggregations.append(AggregationSpec(
                    function=AggregationType.COUNT,
                    column=count_column,
                    alias="count"
                ))
        
        return plan
    
    def _plan_aggregate_query(self, plan: QueryPlan, normalized_query: NormalizedQuery, columns: List[ColumnReference]) -> QueryPlan:
        """Планирует агрегационный запрос"""
        # Определяем тип агрегации по запросу
        text = normalized_query.normalized.lower()
        
        for col in columns:
            col_tags = []
            # Получаем информацию о колонке из схемы
            for table_name, table_info in self.schema_retriever.table_index.items():
                if col.table in table_name:
                    for column_info in self.schema_retriever.column_index.values():
                        if column_info['table'] == table_name and column_info['name'] == col.column:
                            col_tags = column_info.get('tags', [])
                            break
            
            # Определяем агрегацию по контексту
            if 'сумма' in text or 'sum' in text or 'итого' in text:
                if 'money' in col_tags or 'measure' in col_tags:
                    plan.aggregations.append(AggregationSpec(
                        function=AggregationType.SUM,
                        column=col,
                        alias=f"sum_{col.column}"
                    ))
            elif 'среднее' in text or 'средний' in text or 'avg' in text:
                if 'money' in col_tags or 'measure' in col_tags:
                    plan.aggregations.append(AggregationSpec(
                        function=AggregationType.AVG,
                        column=col,
                        alias=f"avg_{col.column}"
                    ))
            elif 'максимум' in text or 'max' in text:
                plan.aggregations.append(AggregationSpec(
                    function=AggregationType.MAX,
                    column=col,
                    alias=f"max_{col.column}"
                ))
            elif 'минимум' in text or 'min' in text:
                plan.aggregations.append(AggregationSpec(
                    function=AggregationType.MIN,
                    column=col,
                    alias=f"min_{col.column}"
                ))
        
        return plan
    
    def _plan_top_query(self, plan: QueryPlan, normalized_query: NormalizedQuery, columns: List[ColumnReference]) -> QueryPlan:
        """Планирует TOP запрос"""
        # Добавляем колонки
        plan.select_columns = columns[:5]
        
        # Определяем лимит из чисел
        for number_info in normalized_query.extracted_numbers:
            if number_info['value'] <= 100:  # Разумный лимит
                plan.limit = int(number_info['value'])
                break
        
        # Добавляем сортировку по релевантной колонке
        if columns:
            # Ищем колонку для сортировки (обычно с тегом money/measure)
            sort_column = None
            for col in columns:
                col_info = self.schema_retriever.column_index.get(col.full_name, {})
                col_tags = col_info.get('tags', [])
                if 'money' in col_tags or 'measure' in col_tags:
                    sort_column = col
                    break
            
            if not sort_column:
                sort_column = columns[0]
            
            plan.order_by.append(SortSpec(
                column=sort_column,
                direction=SortDirection.DESC
            ))
        
        return plan
    
    def _plan_filter_query(self, plan: QueryPlan, normalized_query: NormalizedQuery, columns: List[ColumnReference]) -> QueryPlan:
        """Планирует запрос с фильтрацией"""
        plan.select_columns = columns
        
        # Добавляем фильтры на основе чисел и контекста
        text = normalized_query.normalized.lower()
        
        for col in columns:
            # Определяем фильтры по контексту
            if 'больше' in text or 'свыше' in text:
                for number_info in normalized_query.extracted_numbers:
                    plan.filters.append(FilterCondition(
                        column=col,
                        operator=FilterOperator.GREATER_THAN,
                        value=number_info['value']
                    ))
                    break
            elif 'меньше' in text or 'менее' in text:
                for number_info in normalized_query.extracted_numbers:
                    plan.filters.append(FilterCondition(
                        column=col,
                        operator=FilterOperator.LESS_THAN,
                        value=number_info['value']
                    ))
                    break
        
        return plan
    
    def _plan_trend_query(self, plan: QueryPlan, normalized_query: NormalizedQuery, columns: List[ColumnReference]) -> QueryPlan:
        """Планирует запрос для анализа трендов"""
        # Для трендов нужна группировка по времени
        date_columns = []
        measure_columns = []
        
        for col in columns:
            col_info = self.schema_retriever.column_index.get(col.full_name, {})
            col_tags = col_info.get('tags', [])
            
            if 'date' in col_tags or 'time' in col_tags:
                date_columns.append(col)
            elif 'money' in col_tags or 'measure' in col_tags:
                measure_columns.append(col)
        
        if date_columns and measure_columns:
            # Группируем по дате
            plan.group_by = date_columns[:1]
            plan.select_columns = date_columns[:1]
            
            # Агрегируем меры
            for measure_col in measure_columns[:2]:
                plan.aggregations.append(AggregationSpec(
                    function=AggregationType.SUM,
                    column=measure_col,
                    alias=f"sum_{measure_col.column}"
                ))
            
            # Сортируем по дате
            plan.order_by.append(SortSpec(
                column=date_columns[0],
                direction=SortDirection.ASC
            ))
        
        return plan
    
    def _plan_compare_query(self, plan: QueryPlan, normalized_query: NormalizedQuery, columns: List[ColumnReference]) -> QueryPlan:
        """Планирует запрос для сравнения"""
        # Для сравнения нужна группировка
        category_columns = []
        measure_columns = []
        
        for col in columns:
            col_info = self.schema_retriever.column_index.get(col.full_name, {})
            col_tags = col_info.get('tags', [])
            
            if 'category' in col_tags or 'status' in col_tags:
                category_columns.append(col)
            elif 'money' in col_tags or 'measure' in col_tags:
                measure_columns.append(col)
        
        if category_columns and measure_columns:
            plan.group_by = category_columns[:2]
            plan.select_columns = category_columns[:2]
            
            for measure_col in measure_columns[:2]:
                plan.aggregations.append(AggregationSpec(
                    function=AggregationType.SUM,
                    column=measure_col,
                    alias=f"sum_{measure_col.column}"
                ))
        
        return plan
    
    def _plan_default_query(self, plan: QueryPlan, normalized_query: NormalizedQuery, columns: List[ColumnReference]) -> QueryPlan:
        """Планирует запрос по умолчанию"""
        plan.select_columns = columns[:8]  # Ограничиваем количество колонок
        return plan
    
    def _add_date_filters(self, plan: QueryPlan, normalized_query: NormalizedQuery) -> QueryPlan:
        """Добавляет фильтры по датам"""
        if not normalized_query.extracted_dates:
            return plan
        
        # Ищем колонки с датами
        date_columns = []
        for col in plan.get_all_columns():
            col_info = self.schema_retriever.column_index.get(col.full_name, {})
            col_tags = col_info.get('tags', [])
            if 'date' in col_tags or 'time' in col_tags:
                date_columns.append(col)
        
        if not date_columns:
            return plan
        
        # Добавляем фильтр по первой найденной дате
        date_column = date_columns[0]
        for date_info in normalized_query.extracted_dates:
            if date_info['type'] == 'relative_date':
                plan.filters.append(FilterCondition(
                    column=date_column,
                    operator=FilterOperator.GREATER_THAN_OR_EQUAL,
                    value=date_info['sql_expression']
                ))
            elif date_info['type'] == 'absolute_date':
                plan.filters.append(FilterCondition(
                    column=date_column,
                    operator=FilterOperator.EQUALS,
                    value=date_info['sql_expression']
                ))
        
        return plan
    
    def _add_number_limits(self, plan: QueryPlan, normalized_query: NormalizedQuery) -> QueryPlan:
        """Добавляет лимиты по числам"""
        if not normalized_query.extracted_numbers:
            return plan
        
        # Если в запросе есть "топ", "лучшие" и число, добавляем LIMIT
        text = normalized_query.normalized.lower()
        if any(word in text for word in ['топ', 'лучшие', 'top', 'best', 'первые']):
            for number_info in normalized_query.extracted_numbers:
                if number_info['value'] <= 100:  # Разумный лимит
                    plan.limit = int(number_info['value'])
                    break
        
        return plan


def main():
    """Функция для тестирования планировщика"""
    import argparse
    from nl_normalizer import NLNormalizer
    
    parser = argparse.ArgumentParser(description='Query Planner Test')
    parser.add_argument('--query', type=str, required=True, help='Query to plan')
    parser.add_argument('--schema', type=str, default='schema.json', help='Schema file')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    
    # Создаем компоненты
    normalizer = NLNormalizer()
    retriever = SchemaRetriever(args.schema)
    planner = QueryPlanner(retriever)
    
    # Нормализуем запрос
    normalized_query = normalizer.normalize(args.query)
    
    # Создаем план
    plan = planner.create_plan(normalized_query)
    
    print(f"🔤 Original query: {plan.original_query}")
    print(f"✨ Normalized query: {plan.normalized_query}")
    print(f"🎯 Intent: {plan.intent}")
    print(f"📊 Complexity score: {plan.complexity_score}")
    print(f"🔢 Confidence: {plan.confidence:.3f}")
    print()
    
    print("📋 Query Plan:")
    print(f"  FROM: {plan.from_table}")
    
    if plan.select_columns:
        print(f"  SELECT:")
        for col in plan.select_columns:
            print(f"    - {col.full_name}")
    
    if plan.aggregations:
        print(f"  AGGREGATIONS:")
        for agg in plan.aggregations:
            print(f"    - {agg.function}({agg.column.full_name}) AS {agg.alias}")
    
    if plan.joins:
        print(f"  JOINS:")
        for join in plan.joins:
            print(f"    - {join.join_type} JOIN {join.right_table} ON {join.join_condition}")
    
    if plan.filters:
        print(f"  WHERE:")
        for filter_cond in plan.filters:
            print(f"    - {filter_cond.column.full_name} {filter_cond.operator} {filter_cond.value}")
    
    if plan.group_by:
        print(f"  GROUP BY:")
        for col in plan.group_by:
            print(f"    - {col.full_name}")
    
    if plan.order_by:
        print(f"  ORDER BY:")
        for sort_spec in plan.order_by:
            print(f"    - {sort_spec.column.full_name} {sort_spec.direction}")
    
    if plan.limit:
        print(f"  LIMIT: {plan.limit}")
    
    # Выводим JSON для отладки
    if args.verbose:
        print("\n📄 JSON Plan:")
        print(json.dumps(plan.dict(), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
