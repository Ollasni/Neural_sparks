"""
Explanation Generator Module for BI-GPT Agent
Генерация понятных бизнес-объяснений для SQL запросов и результатов
Поддержка многоязычности и контекста бизнес-логики
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import pandas as pd
import json

from planner import QueryPlan, ColumnReference, AggregationSpec, FilterCondition
from planner import AggregationType, FilterOperator
from nl_normalizer import Language, NormalizedQuery

logger = logging.getLogger(__name__)


class ExplanationType(Enum):
    """Типы объяснений"""
    QUERY_INTENT = "query_intent"
    SQL_BREAKDOWN = "sql_breakdown"
    RESULTS_SUMMARY = "results_summary"
    BUSINESS_INSIGHTS = "business_insights"
    DATA_QUALITY = "data_quality"


@dataclass
class ExplanationSection:
    """Секция объяснения"""
    title: str
    content: str
    section_type: ExplanationType
    confidence: float = 1.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Explanation:
    """Полное объяснение"""
    sections: List[ExplanationSection] = field(default_factory=list)
    language: Language = Language.RUSSIAN
    overall_confidence: float = 1.0
    generated_at: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "sections": [
                {
                    "title": section.title,
                    "content": section.content,
                    "type": section.section_type.value,
                    "confidence": section.confidence,
                    "metadata": section.metadata
                }
                for section in self.sections
            ],
            "language": self.language.value,
            "overall_confidence": self.overall_confidence,
            "generated_at": self.generated_at
        }


class BusinessTermsTranslator:
    """Переводчик технических терминов в бизнес-понятия"""
    
    def __init__(self):
        self.translations = {
            Language.RUSSIAN: {
                # Агрегации
                'COUNT': 'количество',
                'SUM': 'сумма',
                'AVG': 'среднее значение',
                'MAX': 'максимальное значение',
                'MIN': 'минимальное значение',
                'COUNT_DISTINCT': 'уникальное количество',
                
                # Операторы
                '=': 'равно',
                '!=': 'не равно',
                '>': 'больше',
                '>=': 'больше или равно',
                '<': 'меньше',
                '<=': 'меньше или равно',
                'LIKE': 'содержит',
                'NOT LIKE': 'не содержит',
                'IN': 'входит в список',
                'NOT IN': 'не входит в список',
                'IS NULL': 'не указано',
                'IS NOT NULL': 'указано',
                'BETWEEN': 'между',
                
                # Соединения
                'INNER JOIN': 'соединение',
                'LEFT JOIN': 'левое соединение',
                'RIGHT JOIN': 'правое соединение',
                'FULL JOIN': 'полное соединение',
                
                # Сортировка
                'ASC': 'по возрастанию',
                'DESC': 'по убыванию',
                
                # Таблицы (примеры)
                'customers': 'клиенты',
                'orders': 'заказы',
                'products': 'товары',
                'sales': 'продажи',
                'inventory': 'склад',
                
                # Колонки (примеры)
                'id': 'идентификатор',
                'name': 'название',
                'email': 'электронная почта',
                'amount': 'сумма',
                'quantity': 'количество',
                'price': 'цена',
                'date': 'дата',
                'status': 'статус'
            },
            
            Language.ENGLISH: {
                # Агрегации
                'COUNT': 'count',
                'SUM': 'total',
                'AVG': 'average',
                'MAX': 'maximum',
                'MIN': 'minimum',
                'COUNT_DISTINCT': 'unique count',
                
                # Операторы
                '=': 'equals',
                '!=': 'not equals',
                '>': 'greater than',
                '>=': 'greater than or equal',
                '<': 'less than',
                '<=': 'less than or equal',
                'LIKE': 'contains',
                'NOT LIKE': 'does not contain',
                'IN': 'is in',
                'NOT IN': 'is not in',
                'IS NULL': 'is empty',
                'IS NOT NULL': 'is not empty',
                'BETWEEN': 'between',
                
                # Соединения
                'INNER JOIN': 'joined with',
                'LEFT JOIN': 'left joined with',
                'RIGHT JOIN': 'right joined with',
                'FULL JOIN': 'fully joined with',
                
                # Сортировка
                'ASC': 'ascending',
                'DESC': 'descending'
            }
        }
    
    def translate_term(self, term: str, language: Language) -> str:
        """Переводит технический термин в бизнес-понятие"""
        if language not in self.translations:
            return term
        
        return self.translations[language].get(term.upper(), term)
    
    def translate_table_name(self, table_name: str, language: Language) -> str:
        """Переводит имя таблицы"""
        # Убираем схему если есть
        clean_name = table_name.split('.')[-1] if '.' in table_name else table_name
        return self.translate_term(clean_name, language)
    
    def translate_column_name(self, column_name: str, language: Language) -> str:
        """Переводит имя колонки"""
        return self.translate_term(column_name, language)


class QueryIntentExplainer:
    """Объяснитель намерений запроса"""
    
    def __init__(self, translator: BusinessTermsTranslator):
        self.translator = translator
        
        self.intent_templates = {
            Language.RUSSIAN: {
                'select': "Пользователь хочет получить данные из таблицы {tables}",
                'count': "Пользователь хочет подсчитать количество записей в {tables}",
                'aggregate': "Пользователь хочет получить агрегированные данные: {aggregations}",
                'filter': "Пользователь хочет отфильтровать данные по условиям: {filters}",
                'top': "Пользователь хочет получить топ-{limit} записей по {criteria}",
                'trend': "Пользователь анализирует тренды и динамику изменений",
                'compare': "Пользователь сравнивает данные по различным категориям"
            },
            Language.ENGLISH: {
                'select': "User wants to retrieve data from {tables}",
                'count': "User wants to count records in {tables}",
                'aggregate': "User wants aggregated data: {aggregations}",
                'filter': "User wants to filter data by: {filters}",
                'top': "User wants top {limit} records by {criteria}",
                'trend': "User is analyzing trends and changes over time",
                'compare': "User is comparing data across different categories"
            }
        }
    
    def explain_intent(self, normalized_query: NormalizedQuery, plan: QueryPlan) -> ExplanationSection:
        """Объясняет намерение пользователя"""
        language = normalized_query.detected_language
        intent = normalized_query.intent or 'select'
        
        # Собираем контекст
        tables = [self.translator.translate_table_name(t, language) for t in plan.get_all_tables()]
        
        aggregations = []
        for agg in plan.aggregations:
            func_name = self.translator.translate_term(agg.function.value, language)
            col_name = self.translator.translate_column_name(agg.column.column, language)
            aggregations.append(f"{func_name} {col_name}")
        
        filters = []
        for filter_cond in plan.filters:
            col_name = self.translator.translate_column_name(filter_cond.column.column, language)
            op_name = self.translator.translate_term(filter_cond.operator.value, language)
            filters.append(f"{col_name} {op_name} {filter_cond.value}")
        
        # Формируем объяснение
        template = self.intent_templates[language].get(intent, 
            self.intent_templates[language]['select'])
        
        explanation = template.format(
            tables=', '.join(tables) if tables else 'неизвестные таблицы',
            aggregations=', '.join(aggregations) if aggregations else 'данные',
            filters='; '.join(filters) if filters else 'все записи',
            limit=plan.limit or 'все',
            criteria='заданным критериям' if language == Language.RUSSIAN else 'specified criteria'
        )
        
        return ExplanationSection(
            title="Намерение запроса" if language == Language.RUSSIAN else "Query Intent",
            content=explanation,
            section_type=ExplanationType.QUERY_INTENT,
            confidence=normalized_query.confidence,
            metadata={"intent": intent, "original_query": normalized_query.original}
        )


class SQLBreakdownExplainer:
    """Объяснитель структуры SQL запроса"""
    
    def __init__(self, translator: BusinessTermsTranslator):
        self.translator = translator
    
    def explain_sql_structure(self, plan: QueryPlan, language: Language) -> ExplanationSection:
        """Объясняет структуру SQL запроса"""
        parts = []
        
        # SELECT часть
        if plan.select_columns:
            columns = [self.translator.translate_column_name(col.column, language) 
                      for col in plan.select_columns]
            if language == Language.RUSSIAN:
                parts.append(f"Выбираются колонки: {', '.join(columns)}")
            else:
                parts.append(f"Selected columns: {', '.join(columns)}")
        
        # Агрегации
        if plan.aggregations:
            for agg in plan.aggregations:
                func = self.translator.translate_term(agg.function.value, language)
                col = self.translator.translate_column_name(agg.column.column, language)
                if language == Language.RUSSIAN:
                    parts.append(f"Вычисляется {func} для {col}")
                else:
                    parts.append(f"Computing {func} for {col}")
        
        # FROM часть
        if plan.from_table:
            table = self.translator.translate_table_name(plan.from_table, language)
            if language == Language.RUSSIAN:
                parts.append(f"Данные берутся из таблицы: {table}")
            else:
                parts.append(f"Data is taken from table: {table}")
        
        # JOIN'ы
        if plan.joins:
            for join in plan.joins:
                left_table = self.translator.translate_table_name(join.left_table, language)
                right_table = self.translator.translate_table_name(join.right_table, language)
                join_type = self.translator.translate_term(join.join_type.value, language)
                
                if language == Language.RUSSIAN:
                    parts.append(f"Выполняется {join_type} с таблицей {right_table}")
                else:
                    parts.append(f"Performing {join_type} with table {right_table}")
        
        # WHERE условия
        if plan.filters:
            if language == Language.RUSSIAN:
                parts.append(f"Применяются фильтры:")
            else:
                parts.append(f"Applying filters:")
            
            for filter_cond in plan.filters:
                col = self.translator.translate_column_name(filter_cond.column.column, language)
                op = self.translator.translate_term(filter_cond.operator.value, language)
                parts.append(f"  • {col} {op} {filter_cond.value}")
        
        # GROUP BY
        if plan.group_by:
            group_cols = [self.translator.translate_column_name(col.column, language) 
                         for col in plan.group_by]
            if language == Language.RUSSIAN:
                parts.append(f"Группировка по: {', '.join(group_cols)}")
            else:
                parts.append(f"Grouped by: {', '.join(group_cols)}")
        
        # ORDER BY
        if plan.order_by:
            order_items = []
            for sort_spec in plan.order_by:
                col = self.translator.translate_column_name(sort_spec.column.column, language)
                direction = self.translator.translate_term(sort_spec.direction.value, language)
                order_items.append(f"{col} {direction}")
            
            if language == Language.RUSSIAN:
                parts.append(f"Сортировка: {', '.join(order_items)}")
            else:
                parts.append(f"Sorted by: {', '.join(order_items)}")
        
        # LIMIT
        if plan.limit:
            if language == Language.RUSSIAN:
                parts.append(f"Ограничение результата: {plan.limit} записей")
            else:
                parts.append(f"Limited to: {plan.limit} records")
        
        content = '\n'.join(parts) if parts else (
            "Простой запрос данных" if language == Language.RUSSIAN else "Simple data query"
        )
        
        return ExplanationSection(
            title="Структура запроса" if language == Language.RUSSIAN else "Query Structure",
            content=content,
            section_type=ExplanationType.SQL_BREAKDOWN,
            confidence=0.9,
            metadata={"complexity_score": plan.complexity_score}
        )


class ResultsSummaryExplainer:
    """Объяснитель результатов запроса"""
    
    def __init__(self, translator: BusinessTermsTranslator):
        self.translator = translator
    
    def explain_results(self, results_df: pd.DataFrame, plan: QueryPlan, 
                       language: Language) -> ExplanationSection:
        """Объясняет результаты запроса"""
        if results_df.empty:
            content = ("Запрос не вернул результатов. Возможно, данные отсутствуют или "
                      "условия фильтрации слишком строгие." if language == Language.RUSSIAN else
                      "Query returned no results. Data may be missing or filter conditions too strict.")
            
            return ExplanationSection(
                title="Результаты" if language == Language.RUSSIAN else "Results",
                content=content,
                section_type=ExplanationType.RESULTS_SUMMARY,
                confidence=1.0,
                metadata={"row_count": 0, "column_count": 0}
            )
        
        row_count = len(results_df)
        col_count = len(results_df.columns)
        
        parts = []
        
        # Общая статистика
        if language == Language.RUSSIAN:
            parts.append(f"Найдено {row_count} записей с {col_count} полями")
        else:
            parts.append(f"Found {row_count} records with {col_count} fields")
        
        # Анализ числовых колонок
        numeric_cols = results_df.select_dtypes(include=['number']).columns
        if len(numeric_cols) > 0:
            for col in numeric_cols[:3]:  # Первые 3 числовые колонки
                col_translated = self.translator.translate_column_name(col, language)
                mean_val = results_df[col].mean()
                total_val = results_df[col].sum()
                
                if language == Language.RUSSIAN:
                    parts.append(f"По полю '{col_translated}': среднее = {mean_val:.2f}, сумма = {total_val:.2f}")
                else:
                    parts.append(f"For '{col_translated}': average = {mean_val:.2f}, total = {total_val:.2f}")
        
        # Анализ категориальных данных
        categorical_cols = results_df.select_dtypes(include=['object']).columns
        if len(categorical_cols) > 0:
            for col in categorical_cols[:2]:  # Первые 2 категориальные колонки
                unique_count = results_df[col].nunique()
                col_translated = self.translator.translate_column_name(col, language)
                
                if language == Language.RUSSIAN:
                    parts.append(f"В поле '{col_translated}' найдено {unique_count} уникальных значений")
                else:
                    parts.append(f"Field '{col_translated}' has {unique_count} unique values")
        
        content = '\n'.join(parts)
        
        return ExplanationSection(
            title="Анализ результатов" if language == Language.RUSSIAN else "Results Analysis",
            content=content,
            section_type=ExplanationType.RESULTS_SUMMARY,
            confidence=0.8,
            metadata={
                "row_count": row_count,
                "column_count": col_count,
                "numeric_columns": len(numeric_cols),
                "categorical_columns": len(categorical_cols)
            }
        )


class BusinessInsightsGenerator:
    """Генератор бизнес-инсайтов"""
    
    def __init__(self, translator: BusinessTermsTranslator):
        self.translator = translator
    
    def generate_insights(self, results_df: pd.DataFrame, plan: QueryPlan, 
                         normalized_query: NormalizedQuery) -> ExplanationSection:
        """Генерирует бизнес-инсайты"""
        language = normalized_query.detected_language
        insights = []
        
        if results_df.empty:
            return ExplanationSection(
                title="Бизнес-инсайты" if language == Language.RUSSIAN else "Business Insights",
                content="Нет данных для анализа" if language == Language.RUSSIAN else "No data for analysis",
                section_type=ExplanationType.BUSINESS_INSIGHTS,
                confidence=0.0
            )
        
        # Анализ на основе намерения
        intent = normalized_query.intent
        
        if intent == 'count':
            count = len(results_df)
            if language == Language.RUSSIAN:
                if count == 0:
                    insights.append("Данные отсутствуют - возможно, стоит проверить источники данных")
                elif count < 10:
                    insights.append("Небольшое количество записей - может потребоваться расширение критериев поиска")
                elif count > 1000:
                    insights.append("Большой объем данных - рассмотрите возможность дополнительной фильтрации")
                else:
                    insights.append("Умеренное количество записей для анализа")
            else:
                if count == 0:
                    insights.append("No data found - consider checking data sources")
                elif count < 10:
                    insights.append("Low record count - may need broader search criteria")
                elif count > 1000:
                    insights.append("Large dataset - consider additional filtering")
                else:
                    insights.append("Moderate amount of data for analysis")
        
        elif intent == 'aggregate':
            # Анализ агрегированных данных
            numeric_cols = results_df.select_dtypes(include=['number']).columns
            
            for col in numeric_cols[:2]:
                values = results_df[col].dropna()
                if len(values) > 0:
                    std_dev = values.std()
                    mean_val = values.mean()
                    cv = std_dev / mean_val if mean_val != 0 else 0
                    
                    col_translated = self.translator.translate_column_name(col, language)
                    
                    if language == Language.RUSSIAN:
                        if cv > 1:
                            insights.append(f"Высокая вариативность в {col_translated} - разброс значений значительный")
                        elif cv < 0.2:
                            insights.append(f"Низкая вариативность в {col_translated} - значения довольно стабильны")
                        else:
                            insights.append(f"Умеренная вариативность в {col_translated}")
                    else:
                        if cv > 1:
                            insights.append(f"High variability in {col_translated} - significant value spread")
                        elif cv < 0.2:
                            insights.append(f"Low variability in {col_translated} - values are quite stable")
                        else:
                            insights.append(f"Moderate variability in {col_translated}")
        
        elif intent == 'top':
            # Анализ топ-результатов
            if len(results_df) >= 3:
                if language == Language.RUSSIAN:
                    insights.append("В топе представлены лидирующие позиции - стоит проанализировать их характеристики")
                else:
                    insights.append("Top results show leading positions - consider analyzing their characteristics")
        
        # Общие инсайты на основе данных
        if len(results_df.columns) > 5:
            if language == Language.RUSSIAN:
                insights.append("Многомерный анализ - рассмотрите возможность визуализации ключевых показателей")
            else:
                insights.append("Multi-dimensional analysis - consider visualizing key metrics")
        
        content = '\n'.join(insights) if insights else (
            "Дополнительный анализ данных может выявить интересные закономерности" 
            if language == Language.RUSSIAN else
            "Additional data analysis may reveal interesting patterns"
        )
        
        return ExplanationSection(
            title="Бизнес-инсайты" if language == Language.RUSSIAN else "Business Insights",
            content=content,
            section_type=ExplanationType.BUSINESS_INSIGHTS,
            confidence=0.6,
            metadata={"insights_count": len(insights)}
        )


class ExplanationGenerator:
    """Основной генератор объяснений"""
    
    def __init__(self, schema_file: str = "schema.json"):
        self.translator = BusinessTermsTranslator()
        self.intent_explainer = QueryIntentExplainer(self.translator)
        self.sql_explainer = SQLBreakdownExplainer(self.translator)
        self.results_explainer = ResultsSummaryExplainer(self.translator)
        self.insights_generator = BusinessInsightsGenerator(self.translator)
        
        # Загружаем схему для контекста
        try:
            with open(schema_file, 'r', encoding='utf-8') as f:
                self.schema_data = json.load(f)
        except Exception as e:
            logger.warning(f"Could not load schema: {e}")
            self.schema_data = {}
    
    def generate_full_explanation(self, normalized_query: NormalizedQuery, 
                                plan: QueryPlan, results_df: pd.DataFrame,
                                sql: str = "") -> Explanation:
        """Генерирует полное объяснение"""
        from datetime import datetime
        
        language = normalized_query.detected_language
        sections = []
        
        # 1. Объяснение намерения
        intent_section = self.intent_explainer.explain_intent(normalized_query, plan)
        sections.append(intent_section)
        
        # 2. Разбор SQL структуры
        sql_section = self.sql_explainer.explain_sql_structure(plan, language)
        sections.append(sql_section)
        
        # 3. Анализ результатов
        results_section = self.results_explainer.explain_results(results_df, plan, language)
        sections.append(results_section)
        
        # 4. Бизнес-инсайты
        insights_section = self.insights_generator.generate_insights(results_df, plan, normalized_query)
        sections.append(insights_section)
        
        # Вычисляем общую уверенность
        confidences = [section.confidence for section in sections]
        overall_confidence = sum(confidences) / len(confidences) if confidences else 0.0
        
        return Explanation(
            sections=sections,
            language=language,
            overall_confidence=overall_confidence,
            generated_at=datetime.now().isoformat()
        )
    
    def generate_quick_explanation(self, user_query: str, results_df: pd.DataFrame,
                                 language: Language = Language.RUSSIAN) -> str:
        """Генерирует краткое объяснение"""
        if results_df.empty:
            return ("Запрос не вернул результатов" if language == Language.RUSSIAN 
                   else "Query returned no results")
        
        row_count = len(results_df)
        col_count = len(results_df.columns)
        
        # Краткая статистика
        if language == Language.RUSSIAN:
            explanation = f"Найдено {row_count} записей с {col_count} полями. "
        else:
            explanation = f"Found {row_count} records with {col_count} fields. "
        
        # Добавляем информацию о числовых данных
        numeric_cols = results_df.select_dtypes(include=['number']).columns
        if len(numeric_cols) > 0:
            col = numeric_cols[0]
            col_translated = self.translator.translate_column_name(col, language)
            mean_val = results_df[col].mean()
            
            if language == Language.RUSSIAN:
                explanation += f"Среднее значение {col_translated}: {mean_val:.2f}"
            else:
                explanation += f"Average {col_translated}: {mean_val:.2f}"
        
        return explanation
    
    def explain_error(self, error_message: str, context: Dict[str, Any],
                     language: Language = Language.RUSSIAN) -> str:
        """Объясняет ошибку в понятных терминах"""
        error_lower = error_message.lower()
        
        if language == Language.RUSSIAN:
            if 'table' in error_lower and 'not found' in error_lower:
                return "Указанная таблица не найдена в базе данных. Проверьте название таблицы."
            elif 'column' in error_lower and 'not found' in error_lower:
                return "Указанная колонка не существует. Проверьте название поля."
            elif 'syntax error' in error_lower:
                return "Ошибка в структуре запроса. Проверьте правильность формулировки."
            elif 'permission' in error_lower or 'access' in error_lower:
                return "Недостаточно прав для выполнения запроса."
            else:
                return f"Произошла ошибка при выполнении запроса: {error_message}"
        else:
            if 'table' in error_lower and 'not found' in error_lower:
                return "Specified table not found in database. Check table name."
            elif 'column' in error_lower and 'not found' in error_lower:
                return "Specified column does not exist. Check field name."
            elif 'syntax error' in error_lower:
                return "Query syntax error. Check query formulation."
            elif 'permission' in error_lower or 'access' in error_lower:
                return "Insufficient permissions to execute query."
            else:
                return f"Error occurred while executing query: {error_message}"


def main():
    """Функция для тестирования генератора объяснений"""
    import argparse
    from nl_normalizer import NLNormalizer
    from retriever import SchemaRetriever
    from planner import QueryPlanner
    
    parser = argparse.ArgumentParser(description='Explanation Generator Test')
    parser.add_argument('--query', type=str, required=True, help='Query to explain')
    parser.add_argument('--schema', type=str, default='schema.json', help='Schema file')
    parser.add_argument('--language', type=str, default='russian', 
                       choices=['russian', 'english'], help='Explanation language')
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
    explainer = ExplanationGenerator(args.schema)
    
    # Обрабатываем запрос
    print(f"🔤 Analyzing query: {args.query}")
    
    # Нормализуем
    normalized_query = normalizer.normalize(args.query)
    
    # Планируем
    plan = planner.create_plan(normalized_query)
    
    # Создаем пустой DataFrame для демонстрации
    import pandas as pd
    demo_results = pd.DataFrame({
        'name': ['Product A', 'Product B', 'Product C'],
        'amount': [1000, 1500, 800],
        'quantity': [10, 15, 8]
    })
    
    # Генерируем объяснение
    language = Language.RUSSIAN if args.language == 'russian' else Language.ENGLISH
    normalized_query.detected_language = language  # Переопределяем для демо
    
    explanation = explainer.generate_full_explanation(
        normalized_query, plan, demo_results
    )
    
    print(f"\n📋 Full Explanation ({language.value}):")
    print(f"Overall Confidence: {explanation.overall_confidence:.3f}")
    print(f"Generated At: {explanation.generated_at}")
    
    for i, section in enumerate(explanation.sections, 1):
        print(f"\n{i}. {section.title}")
        print(f"   {section.content}")
        print(f"   Confidence: {section.confidence:.3f}")
    
    # Также демонстрируем краткое объяснение
    quick_explanation = explainer.generate_quick_explanation(
        args.query, demo_results, language
    )
    print(f"\n💬 Quick Explanation:")
    print(f"   {quick_explanation}")


if __name__ == "__main__":
    main()
