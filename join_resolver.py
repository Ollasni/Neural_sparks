"""
Join Resolver Module for BI-GPT Agent
Автоматическое определение и построение JOIN'ов между таблицами на основе FK-графа
Оптимизация путей соединений для минимизации сложности запросов
"""

import json
import logging
from typing import Dict, List, Any, Optional, Tuple, Set
from dataclasses import dataclass, field
from collections import defaultdict, deque
import networkx as nx

logger = logging.getLogger(__name__)

# Fallback если networkx недоступен
try:
    import networkx as nx
    HAS_NETWORKX = True
except ImportError:
    HAS_NETWORKX = False
    logger.warning("networkx not available. Using simple graph implementation.")


@dataclass 
class JoinPath:
    """Путь соединения между таблицами"""
    from_table: str
    to_table: str
    joins: List[Dict[str, Any]] = field(default_factory=list)
    cost: int = 0
    confidence: float = 1.0
    
    def __post_init__(self):
        self.cost = len(self.joins)


@dataclass
class ForeignKeyRelation:
    """Связь внешнего ключа"""
    from_table: str
    from_column: str
    to_table: str  
    to_column: str
    constraint_name: str
    cardinality: str = "many_to_one"  # one_to_one, one_to_many, many_to_one, many_to_many
    
    @property
    def from_full(self) -> str:
        return f"{self.from_table}.{self.from_column}"
    
    @property  
    def to_full(self) -> str:
        return f"{self.to_table}.{self.to_column}"


class SimpleGraph:
    """Простая реализация графа если networkx недоступен"""
    
    def __init__(self):
        self.nodes: Set[str] = set()
        self.edges: Dict[str, List[Tuple[str, Dict[str, Any]]]] = defaultdict(list)
    
    def add_node(self, node: str):
        """Добавляет узел"""
        self.nodes.add(node)
    
    def add_edge(self, from_node: str, to_node: str, **attrs):
        """Добавляет ребро"""
        self.nodes.add(from_node)
        self.nodes.add(to_node)
        self.edges[from_node].append((to_node, attrs))
        # Добавляем обратное ребро для ненаправленного графа
        self.edges[to_node].append((from_node, attrs))
    
    def shortest_path(self, source: str, target: str) -> Optional[List[str]]:
        """Находит кратчайший путь между узлами (BFS)"""
        if source == target:
            return [source]
        
        if source not in self.nodes or target not in self.nodes:
            return None
        
        visited = set()
        queue = deque([(source, [source])])
        
        while queue:
            node, path = queue.popleft()
            
            if node in visited:
                continue
            
            visited.add(node)
            
            for neighbor, _ in self.edges[node]:
                if neighbor == target:
                    return path + [neighbor]
                
                if neighbor not in visited:
                    queue.append((neighbor, path + [neighbor]))
        
        return None
    
    def get_edge_data(self, from_node: str, to_node: str) -> Optional[Dict[str, Any]]:
        """Получает данные ребра"""
        for neighbor, attrs in self.edges.get(from_node, []):
            if neighbor == to_node:
                return attrs
        return None


class JoinResolver:
    """Резолвер соединений таблиц"""
    
    def __init__(self, schema_file: str = "schema.json"):
        self.schema_file = schema_file
        self.schema_data: Dict[str, Any] = {}
        self.fk_relations: List[ForeignKeyRelation] = []
        
        # Граф связей между таблицами
        self.graph = nx.Graph() if HAS_NETWORKX else SimpleGraph()
        
        # Кэш путей для оптимизации
        self.path_cache: Dict[Tuple[str, str], Optional[JoinPath]] = {}
        
        self._load_schema()
        self._build_relationship_graph()
    
    def _load_schema(self):
        """Загружает схему из JSON файла"""
        try:
            with open(self.schema_file, 'r', encoding='utf-8') as f:
                self.schema_data = json.load(f)
            logger.info(f"Schema loaded from {self.schema_file}")
        except FileNotFoundError:
            logger.warning(f"Schema file {self.schema_file} not found")
            self.schema_data = {"tables": {}, "fks": []}
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing schema file: {e}")
            self.schema_data = {"tables": {}, "fks": []}
    
    def _build_relationship_graph(self):
        """Строит граф связей между таблицами"""
        # Парсим внешние ключи
        for fk_data in self.schema_data.get("fks", []):
            try:
                from_parts = fk_data["from"].split(".")
                to_parts = fk_data["to"].split(".")
                
                if len(from_parts) >= 2 and len(to_parts) >= 2:
                    from_table = ".".join(from_parts[:-1])
                    from_column = from_parts[-1]
                    to_table = ".".join(to_parts[:-1]) 
                    to_column = to_parts[-1]
                    
                    fk_relation = ForeignKeyRelation(
                        from_table=from_table,
                        from_column=from_column,
                        to_table=to_table,
                        to_column=to_column,
                        constraint_name=fk_data.get("constraint", ""),
                        cardinality=self._infer_cardinality(from_table, from_column, to_table, to_column)
                    )
                    
                    self.fk_relations.append(fk_relation)
                    
                    # Добавляем в граф
                    if HAS_NETWORKX:
                        self.graph.add_edge(
                            from_table, 
                            to_table,
                            from_column=from_column,
                            to_column=to_column,
                            constraint=fk_relation.constraint_name,
                            cardinality=fk_relation.cardinality,
                            weight=1
                        )
                    else:
                        self.graph.add_edge(
                            from_table,
                            to_table,
                            from_column=from_column,
                            to_column=to_column,
                            constraint=fk_relation.constraint_name,
                            cardinality=fk_relation.cardinality,
                            weight=1
                        )
            
            except Exception as e:
                logger.warning(f"Error parsing FK relationship {fk_data}: {e}")
        
        logger.info(f"Built relationship graph with {len(self.fk_relations)} FK relations")
    
    def _infer_cardinality(self, from_table: str, from_column: str, to_table: str, to_column: str) -> str:
        """Выводит кардинальность связи на основе схемы"""
        # Простая эвристика: если to_column это первичный ключ, то many_to_one
        to_table_info = self.schema_data.get("tables", {}).get(to_table, {})
        to_columns = to_table_info.get("columns", [])
        
        for col in to_columns:
            if col.get("name") == to_column and col.get("pk", False):
                return "many_to_one"
        
        # По умолчанию many_to_one
        return "many_to_one"
    
    def find_join_path(self, from_table: str, to_table: str) -> Optional[JoinPath]:
        """Находит оптимальный путь соединения между таблицами"""
        # Проверяем кэш
        cache_key = (from_table, to_table)
        if cache_key in self.path_cache:
            return self.path_cache[cache_key]
        
        # Если это одна и та же таблица
        if from_table == to_table:
            join_path = JoinPath(from_table=from_table, to_table=to_table)
            self.path_cache[cache_key] = join_path
            return join_path
        
        try:
            # Ищем кратчайший путь в графе
            if HAS_NETWORKX:
                if from_table in self.graph and to_table in self.graph:
                    path_nodes = nx.shortest_path(self.graph, from_table, to_table)
                else:
                    path_nodes = None
            else:
                path_nodes = self.graph.shortest_path(from_table, to_table)
            
            if not path_nodes or len(path_nodes) < 2:
                self.path_cache[cache_key] = None
                return None
            
            # Строим последовательность JOIN'ов
            joins = []
            for i in range(len(path_nodes) - 1):
                left_table = path_nodes[i]
                right_table = path_nodes[i + 1]
                
                # Находим связь между таблицами
                join_info = self._get_join_info(left_table, right_table)
                if join_info:
                    joins.append(join_info)
            
            join_path = JoinPath(
                from_table=from_table,
                to_table=to_table,
                joins=joins,
                confidence=self._calculate_path_confidence(joins)
            )
            
            self.path_cache[cache_key] = join_path
            return join_path
        
        except Exception as e:
            logger.error(f"Error finding join path from {from_table} to {to_table}: {e}")
            self.path_cache[cache_key] = None
            return None
    
    def _get_join_info(self, left_table: str, right_table: str) -> Optional[Dict[str, Any]]:
        """Получает информацию о соединении между двумя таблицами"""
        # Ищем прямую связь
        for fk in self.fk_relations:
            if fk.from_table == left_table and fk.to_table == right_table:
                return {
                    "type": "INNER",
                    "left_table": left_table,
                    "right_table": right_table,
                    "left_column": fk.from_column,
                    "right_column": fk.to_column,
                    "condition": f"{fk.from_full} = {fk.to_full}",
                    "cardinality": fk.cardinality,
                    "constraint": fk.constraint_name
                }
            elif fk.from_table == right_table and fk.to_table == left_table:
                return {
                    "type": "INNER",
                    "left_table": left_table,
                    "right_table": right_table,
                    "left_column": fk.to_column,
                    "right_column": fk.from_column,
                    "condition": f"{fk.to_full} = {fk.from_full}",
                    "cardinality": self._reverse_cardinality(fk.cardinality),
                    "constraint": fk.constraint_name
                }
        
        # Если прямой связи нет, ищем в графе
        if HAS_NETWORKX:
            edge_data = self.graph.get_edge_data(left_table, right_table)
        else:
            edge_data = self.graph.get_edge_data(left_table, right_table)
        
        if edge_data:
            return {
                "type": "INNER",
                "left_table": left_table,
                "right_table": right_table,
                "left_column": edge_data.get("from_column", "id"),
                "right_column": edge_data.get("to_column", "id"),
                "condition": f"{left_table}.{edge_data.get('from_column', 'id')} = {right_table}.{edge_data.get('to_column', 'id')}",
                "cardinality": edge_data.get("cardinality", "many_to_one"),
                "constraint": edge_data.get("constraint", "")
            }
        
        return None
    
    def _reverse_cardinality(self, cardinality: str) -> str:
        """Обращает кардинальность связи"""
        mapping = {
            "one_to_one": "one_to_one",
            "one_to_many": "many_to_one", 
            "many_to_one": "one_to_many",
            "many_to_many": "many_to_many"
        }
        return mapping.get(cardinality, cardinality)
    
    def _calculate_path_confidence(self, joins: List[Dict[str, Any]]) -> float:
        """Вычисляет уверенность в пути соединения"""
        if not joins:
            return 1.0
        
        # Базовая уверенность убывает с длиной пути
        base_confidence = 1.0 / (1 + len(joins) * 0.1)
        
        # Штрафуем за many_to_many соединения
        penalty = 0.0
        for join in joins:
            if join.get("cardinality") == "many_to_many":
                penalty += 0.2
        
        return max(0.1, base_confidence - penalty)
    
    def resolve_multi_table_joins(self, tables: List[str]) -> List[Dict[str, Any]]:
        """Разрешает соединения для множества таблиц"""
        if len(tables) <= 1:
            return []
        
        # Строим минимальное остовное дерево для оптимальных соединений
        joins = []
        connected_tables = {tables[0]}
        remaining_tables = set(tables[1:])
        
        while remaining_tables:
            best_join = None
            best_cost = float('inf')
            best_table = None
            
            # Ищем лучшее соединение среди оставшихся таблиц
            for connected_table in connected_tables:
                for remaining_table in remaining_tables:
                    join_path = self.find_join_path(connected_table, remaining_table)
                    
                    if join_path and join_path.cost < best_cost:
                        best_cost = join_path.cost
                        best_join = join_path
                        best_table = remaining_table
            
            if best_join and best_table:
                # Добавляем все JOIN'ы из пути
                joins.extend(best_join.joins)
                connected_tables.add(best_table)
                remaining_tables.remove(best_table)
            else:
                # Не можем соединить оставшиеся таблицы
                logger.warning(f"Cannot connect tables: {remaining_tables}")
                break
        
        return joins
    
    def optimize_join_order(self, joins: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Оптимизирует порядок JOIN'ов для производительности"""
        if len(joins) <= 1:
            return joins
        
        # Простая эвристика: сначала соединения many_to_one, потом остальные
        prioritized_joins = []
        other_joins = []
        
        for join in joins:
            if join.get("cardinality") == "many_to_one":
                prioritized_joins.append(join)
            else:
                other_joins.append(join)
        
        return prioritized_joins + other_joins
    
    def suggest_join_type(self, left_table: str, right_table: str, query_context: Dict[str, Any] = None) -> str:
        """Предлагает тип JOIN на основе контекста запроса"""
        join_path = self.find_join_path(left_table, right_table)
        
        if not join_path or not join_path.joins:
            return "INNER"
        
        # Анализируем контекст запроса
        if query_context:
            # Если в запросе есть агрегации, предпочитаем INNER JOIN
            if query_context.get("has_aggregations", False):
                return "INNER"
            
            # Если важна полнота данных, используем LEFT JOIN
            if query_context.get("preserve_left_table", False):
                return "LEFT"
        
        # По умолчанию INNER JOIN
        return "INNER"
    
    def get_table_relationships(self, table: str) -> List[Dict[str, Any]]:
        """Возвращает все связи для таблицы"""
        relationships = []
        
        for fk in self.fk_relations:
            if fk.from_table == table:
                relationships.append({
                    "direction": "outgoing",
                    "target_table": fk.to_table,
                    "local_column": fk.from_column,
                    "target_column": fk.to_column,
                    "cardinality": fk.cardinality,
                    "constraint": fk.constraint_name
                })
            elif fk.to_table == table:
                relationships.append({
                    "direction": "incoming", 
                    "source_table": fk.from_table,
                    "source_column": fk.from_column,
                    "local_column": fk.to_column,
                    "cardinality": self._reverse_cardinality(fk.cardinality),
                    "constraint": fk.constraint_name
                })
        
        return relationships
    
    def validate_join_plan(self, joins: List[Dict[str, Any]]) -> Tuple[bool, List[str]]:
        """Валидирует план соединений"""
        errors = []
        
        if not joins:
            return True, errors
        
        # Проверяем что все таблицы существуют
        all_tables = set()
        for join in joins:
            left_table = join.get("left_table")
            right_table = join.get("right_table")
            
            if left_table:
                all_tables.add(left_table)
            if right_table:
                all_tables.add(right_table)
        
        schema_tables = set(self.schema_data.get("tables", {}).keys())
        for table in all_tables:
            if table not in schema_tables:
                errors.append(f"Unknown table: {table}")
        
        # Проверяем что колонки существуют
        for join in joins:
            left_table = join.get("left_table")
            right_table = join.get("right_table") 
            left_column = join.get("left_column")
            right_column = join.get("right_column")
            
            if left_table and left_column:
                if not self._column_exists(left_table, left_column):
                    errors.append(f"Column {left_table}.{left_column} does not exist")
            
            if right_table and right_column:
                if not self._column_exists(right_table, right_column):
                    errors.append(f"Column {right_table}.{right_column} does not exist")
        
        # Проверяем цикличность
        if self._has_circular_joins(joins):
            errors.append("Circular join detected")
        
        return len(errors) == 0, errors
    
    def _column_exists(self, table: str, column: str) -> bool:
        """Проверяет существование колонки в таблице"""
        table_info = self.schema_data.get("tables", {}).get(table, {})
        columns = table_info.get("columns", [])
        
        for col in columns:
            if col.get("name") == column:
                return True
        
        return False
    
    def _has_circular_joins(self, joins: List[Dict[str, Any]]) -> bool:
        """Проверяет наличие циклических соединений"""
        # Строим граф из JOIN'ов
        graph = defaultdict(list)
        for join in joins:
            left = join.get("left_table")
            right = join.get("right_table")
            if left and right:
                graph[left].append(right)
                graph[right].append(left)
        
        # Проверяем цикличность с помощью DFS
        visited = set()
        
        def has_cycle(node: str, parent: str = None) -> bool:
            if node in visited:
                return True
            
            visited.add(node)
            
            for neighbor in graph[node]:
                if neighbor != parent and has_cycle(neighbor, node):
                    return True
            
            visited.remove(node)
            return False
        
        for node in graph:
            if node not in visited:
                if has_cycle(node):
                    return True
        
        return False


def main():
    """Функция для тестирования join resolver"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Join Resolver Test')
    parser.add_argument('--schema', type=str, default='schema.json', help='Schema JSON file')
    parser.add_argument('--tables', type=str, nargs='+', required=True, help='Tables to join')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    
    # Создаем resolver
    resolver = JoinResolver(args.schema)
    
    print(f"🔗 Analyzing joins for tables: {args.tables}")
    
    # Разрешаем соединения
    joins = resolver.resolve_multi_table_joins(args.tables)
    
    if joins:
        print(f"📋 Found {len(joins)} joins:")
        for i, join in enumerate(joins, 1):
            print(f"  {i}. {join['type']} JOIN {join['right_table']} ON {join['condition']}")
            print(f"     Cardinality: {join.get('cardinality', 'unknown')}")
    else:
        print("❌ No joins found or tables cannot be connected")
    
    # Валидируем план
    is_valid, errors = resolver.validate_join_plan(joins)
    if is_valid:
        print("✅ Join plan is valid")
    else:
        print("❌ Join plan validation failed:")
        for error in errors:
            print(f"   - {error}")
    
    # Показываем связи для каждой таблицы
    print(f"\n📊 Table relationships:")
    for table in args.tables:
        relationships = resolver.get_table_relationships(table)
        print(f"\n{table}:")
        if relationships:
            for rel in relationships:
                if rel['direction'] == 'outgoing':
                    print(f"  → {rel['target_table']} via {rel['local_column']} → {rel['target_column']}")
                else:
                    print(f"  ← {rel['source_table']} via {rel['source_column']} → {rel['local_column']}")
        else:
            print("  No relationships found")


if __name__ == "__main__":
    main()

