"""
RAG Retriever Module for BI-GPT Agent
Семантический поиск по схеме БД и бизнес-словарю
Поддерживает поиск таблиц, колонок, связей и бизнес-терминов
"""

import json
import re
import logging
import math
from typing import Dict, List, Any, Optional, Tuple, Set
from dataclasses import dataclass, field
from datetime import datetime
from collections import defaultdict
import sqlite3

# Опциональный импорт для векторного поиска
try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False

try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False

logger = logging.getLogger(__name__)


@dataclass
class SearchResult:
    """Результат поиска"""
    content: str
    relevance_score: float
    result_type: str  # 'table', 'column', 'business_term', 'relationship'
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        # Нормализуем score в диапазон 0-1
        self.relevance_score = max(0, min(1, self.relevance_score))


@dataclass
class BusinessTerm:
    """Бизнес-термин с определением и контекстом"""
    term: str
    definition: str
    synonyms: List[str] = field(default_factory=list)
    category: str = "general"
    examples: List[str] = field(default_factory=list)
    related_tables: List[str] = field(default_factory=list)
    related_columns: List[str] = field(default_factory=list)


class BusinessDictionary:
    """Расширенный бизнес-словарь с поддержкой синонимов и контекста"""
    
    def __init__(self):
        self.terms: Dict[str, BusinessTerm] = {}
        self._init_default_terms()
    
    def _init_default_terms(self):
        """Инициализация стандартных бизнес-терминов"""
        default_terms = [
            BusinessTerm(
                term="прибыль",
                definition="revenue - costs",
                synonyms=["доход", "профит", "выгода", "доходность"],
                category="финансы",
                examples=["прибыль за месяц", "общая прибыль"],
                related_columns=["revenue", "costs", "profit"]
            ),
            BusinessTerm(
                term="маржинальность",
                definition="(revenue - costs) / revenue * 100",
                synonyms=["маржа", "рентабельность", "доходность"],
                category="финансы",
                examples=["маржинальность по категориям", "средняя маржа"],
                related_columns=["revenue", "costs"]
            ),
            BusinessTerm(
                term="средний чек",
                definition="AVG(order_amount)",
                synonyms=["средний заказ", "AOV", "average order value"],
                category="продажи",
                examples=["средний чек клиентов", "AOV по сегментам"],
                related_tables=["orders"],
                related_columns=["amount", "order_amount"]
            ),
            BusinessTerm(
                term="выручка",
                definition="SUM(revenue)",
                synonyms=["оборот", "доходы", "продажи"],
                category="финансы",
                examples=["общая выручка", "выручка за период"],
                related_columns=["revenue", "amount", "sales"]
            ),
            BusinessTerm(
                term="остатки",
                definition="current_stock",
                synonyms=["склад", "запасы", "инвентарь", "stock"],
                category="логистика",
                examples=["остатки товаров", "остатки на складе"],
                related_tables=["inventory"],
                related_columns=["current_stock", "stock", "quantity"]
            ),
            BusinessTerm(
                term="клиенты",
                definition="customers",
                synonyms=["покупатели", "заказчики", "users", "пользователи"],
                category="CRM",
                examples=["активные клиенты", "новые клиенты"],
                related_tables=["customers", "users"]
            ),
            BusinessTerm(
                term="заказы",
                definition="orders",
                synonyms=["покупки", "сделки", "транзакции"],
                category="продажи",
                examples=["новые заказы", "выполненные заказы"],
                related_tables=["orders", "sales"]
            ),
            BusinessTerm(
                term="товары",
                definition="products",
                synonyms=["продукты", "items", "номенклатура"],
                category="каталог",
                examples=["популярные товары", "новые товары"],
                related_tables=["products", "items"]
            )
        ]
        
        for term in default_terms:
            self.add_term(term)
    
    def add_term(self, term: BusinessTerm):
        """Добавляет термин в словарь"""
        self.terms[term.term.lower()] = term
        
        # Добавляем синонимы как отдельные записи
        for synonym in term.synonyms:
            self.terms[synonym.lower()] = term
    
    def find_term(self, query: str) -> Optional[BusinessTerm]:
        """Ищет термин в словаре"""
        query_lower = query.lower().strip()
        return self.terms.get(query_lower)
    
    def search_terms(self, query: str, threshold: float = 0.3) -> List[Tuple[BusinessTerm, float]]:
        """Нечеткий поиск терминов"""
        query_lower = query.lower()
        results = []
        
        for term_key, term in self.terms.items():
            if term in [t[0] for t in results]:  # Избегаем дубликатов
                continue
            
            score = 0.0
            
            # Точное совпадение
            if term_key in query_lower:
                score = 1.0
            # Частичное совпадение
            elif any(word in query_lower for word in term_key.split()):
                score = 0.7
            # Совпадение в определении
            elif term_key in term.definition.lower():
                score = 0.5
            # Совпадение в примерах
            elif any(term_key in example.lower() for example in term.examples):
                score = 0.4
            
            if score >= threshold:
                results.append((term, score))
        
        return sorted(results, key=lambda x: x[1], reverse=True)


class SimpleVectorizer:
    """Простой векторизатор на основе TF-IDF без sklearn"""
    
    def __init__(self, max_features: int = 1000):
        self.max_features = max_features
        self.vocabulary_: Dict[str, int] = {}
        self.idf_: Dict[str, float] = {}
        self.documents_: List[str] = []
    
    def _tokenize(self, text: str) -> List[str]:
        """Простая токенизация"""
        # Приводим к нижнему регистру и разделяем по словам
        words = re.findall(r'\b\w+\b', text.lower())
        return words
    
    def fit(self, documents: List[str]):
        """Обучаем векторизатор"""
        self.documents_ = documents
        
        # Собираем словарь
        word_doc_count = defaultdict(int)
        all_words = set()
        
        for doc in documents:
            words = set(self._tokenize(doc))
            all_words.update(words)
            for word in words:
                word_doc_count[word] += 1
        
        # Ограничиваем размер словаря
        sorted_words = sorted(word_doc_count.items(), key=lambda x: x[1], reverse=True)
        vocab_words = [word for word, count in sorted_words[:self.max_features]]
        
        self.vocabulary_ = {word: i for i, word in enumerate(vocab_words)}
        
        # Вычисляем IDF
        n_docs = len(documents)
        for word in self.vocabulary_:
            df = word_doc_count[word]
            self.idf_[word] = math.log(n_docs / (1 + df))
    
    def transform(self, documents: List[str]) -> List[List[float]]:
        """Преобразуем документы в векторы"""
        if not self.vocabulary_:
            raise ValueError("Vectorizer not fitted")
        
        vectors = []
        
        for doc in documents:
            words = self._tokenize(doc)
            word_count = defaultdict(int)
            
            # Считаем частоты слов
            for word in words:
                if word in self.vocabulary_:
                    word_count[word] += 1
            
            # Создаем TF-IDF вектор
            vector = [0.0] * len(self.vocabulary_)
            
            if words:  # Избегаем деления на ноль
                for word, count in word_count.items():
                    if word in self.vocabulary_:
                        tf = count / len(words)
                        idf = self.idf_[word]
                        idx = self.vocabulary_[word]
                        vector[idx] = tf * idf
            
            vectors.append(vector)
        
        return vectors
    
    def fit_transform(self, documents: List[str]) -> List[List[float]]:
        """Обучаем и преобразуем"""
        self.fit(documents)
        return self.transform(documents)


def cosine_similarity_simple(vec1: List[float], vec2: List[float]) -> float:
    """Простое вычисление косинусного сходства"""
    if not vec1 or not vec2 or len(vec1) != len(vec2):
        return 0.0
    
    dot_product = sum(a * b for a, b in zip(vec1, vec2))
    norm1 = math.sqrt(sum(a * a for a in vec1))
    norm2 = math.sqrt(sum(b * b for b in vec2))
    
    if norm1 == 0 or norm2 == 0:
        return 0.0
    
    return dot_product / (norm1 * norm2)


class SchemaRetriever:
    """RAG-ретривер для поиска по схеме БД"""
    
    def __init__(self, schema_file: str = "schema.json"):
        self.schema_file = schema_file
        self.schema_data: Dict[str, Any] = {}
        self.business_dict = BusinessDictionary()
        
        # Индексы для поиска
        self.table_index: Dict[str, Dict[str, Any]] = {}
        self.column_index: Dict[str, Dict[str, Any]] = {}
        self.relationship_index: List[Dict[str, Any]] = []
        
        # Векторизатор
        self.vectorizer = None
        self.document_vectors: List[List[float]] = []
        self.documents: List[str] = []
        self.document_metadata: List[Dict[str, Any]] = []
        
        self._load_schema()
        self._build_indexes()
        self._build_vector_index()
    
    def _load_schema(self):
        """Загружает схему из JSON файла"""
        try:
            with open(self.schema_file, 'r', encoding='utf-8') as f:
                self.schema_data = json.load(f)
            logger.info(f"Schema loaded from {self.schema_file}")
        except FileNotFoundError:
            logger.warning(f"Schema file {self.schema_file} not found. Using empty schema.")
            self.schema_data = {"tables": {}, "fks": [], "schemas": []}
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing schema file: {e}")
            self.schema_data = {"tables": {}, "fks": [], "schemas": []}
    
    def _build_indexes(self):
        """Строит индексы для быстрого поиска"""
        # Индекс таблиц
        for table_name, table_info in self.schema_data.get("tables", {}).items():
            self.table_index[table_name] = {
                "name": table_name,
                "description": table_info.get("description", ""),
                "columns": [col["name"] for col in table_info.get("columns", [])],
                "column_count": len(table_info.get("columns", [])),
                "row_count": table_info.get("row_count", 0),
                "schema": table_info.get("schema", "public")
            }
        
        # Индекс колонок
        for table_name, table_info in self.schema_data.get("tables", {}).items():
            for column in table_info.get("columns", []):
                column_key = f"{table_name}.{column['name']}"
                self.column_index[column_key] = {
                    "table": table_name,
                    "name": column["name"],
                    "type": column["type"],
                    "tags": column.get("tags", []),
                    "description": column.get("description", ""),
                    "pk": column.get("pk", False),
                    "nullable": column.get("nullable", True)
                }
        
        # Индекс связей
        for fk in self.schema_data.get("fks", []):
            self.relationship_index.append({
                "from": fk["from"],
                "to": fk["to"],
                "constraint": fk.get("constraint", ""),
                "type": "foreign_key"
            })
        
        logger.info(f"Built indexes: {len(self.table_index)} tables, {len(self.column_index)} columns, {len(self.relationship_index)} relationships")
    
    def _build_vector_index(self):
        """Строит векторный индекс для семантического поиска"""
        documents = []
        metadata = []
        
        # Документы для таблиц
        for table_name, table_info in self.table_index.items():
            doc_text = f"table {table_name} {table_info['description']} columns: {' '.join(table_info['columns'])}"
            documents.append(doc_text)
            metadata.append({
                "type": "table",
                "name": table_name,
                "data": table_info
            })
        
        # Документы для колонок
        for column_key, column_info in self.column_index.items():
            tags_text = ' '.join(column_info['tags'])
            doc_text = f"column {column_info['name']} {column_info['type']} {column_info['description']} tags: {tags_text}"
            documents.append(doc_text)
            metadata.append({
                "type": "column",
                "name": column_key,
                "data": column_info
            })
        
        # Документы для бизнес-терминов
        for term_key, term in self.business_dict.terms.items():
            if term in [m["data"] for m in metadata if m.get("data") == term]:
                continue  # Избегаем дубликатов
            
            synonyms_text = ' '.join(term.synonyms)
            examples_text = ' '.join(term.examples)
            doc_text = f"business term {term.term} {term.definition} {synonyms_text} {examples_text} category: {term.category}"
            documents.append(doc_text)
            metadata.append({
                "type": "business_term",
                "name": term.term,
                "data": term
            })
        
        self.documents = documents
        self.document_metadata = metadata
        
        # Используем sklearn если доступен, иначе простой векторизатор
        if HAS_SKLEARN:
            try:
                self.vectorizer = TfidfVectorizer(max_features=1000, stop_words=None, lowercase=True)
                vectors_matrix = self.vectorizer.fit_transform(documents)
                self.document_vectors = vectors_matrix.toarray().tolist()
                logger.info("Using sklearn TfidfVectorizer for vector index")
            except Exception as e:
                logger.warning(f"sklearn vectorizer failed: {e}. Using simple vectorizer.")
                self._build_simple_vector_index()
        else:
            self._build_simple_vector_index()
        
        logger.info(f"Built vector index with {len(documents)} documents")
    
    def _build_simple_vector_index(self):
        """Строит простой векторный индекс"""
        self.vectorizer = SimpleVectorizer(max_features=1000)
        self.document_vectors = self.vectorizer.fit_transform(self.documents)
        logger.info("Using simple TfidfVectorizer for vector index")
    
    def search_tables(self, query: str, limit: int = 5) -> List[SearchResult]:
        """Поиск таблиц по запросу"""
        results = []
        query_lower = query.lower()
        
        for table_name, table_info in self.table_index.items():
            score = 0.0
            
            # Поиск в имени таблицы
            if query_lower in table_name.lower():
                score += 1.0
            
            # Поиск в описании
            if table_info['description'] and query_lower in table_info['description'].lower():
                score += 0.8
            
            # Поиск в именах колонок
            matching_columns = [col for col in table_info['columns'] if query_lower in col.lower()]
            if matching_columns:
                score += 0.6 * len(matching_columns) / len(table_info['columns'])
            
            if score > 0:
                results.append(SearchResult(
                    content=f"Table: {table_name}",
                    relevance_score=score,
                    result_type="table",
                    metadata=table_info
                ))
        
        return sorted(results, key=lambda x: x.relevance_score, reverse=True)[:limit]
    
    def search_columns(self, query: str, limit: int = 10) -> List[SearchResult]:
        """Поиск колонок по запросу"""
        results = []
        query_lower = query.lower()
        
        for column_key, column_info in self.column_index.items():
            score = 0.0
            
            # Поиск в имени колонки
            if query_lower in column_info['name'].lower():
                score += 1.0
            
            # Поиск в типе
            if query_lower in column_info['type'].lower():
                score += 0.7
            
            # Поиск в тегах
            matching_tags = [tag for tag in column_info['tags'] if query_lower in tag.lower()]
            if matching_tags:
                score += 0.8 * len(matching_tags) / len(column_info['tags']) if column_info['tags'] else 0
            
            # Поиск в описании
            if column_info['description'] and query_lower in column_info['description'].lower():
                score += 0.6
            
            if score > 0:
                results.append(SearchResult(
                    content=f"Column: {column_key} ({column_info['type']})",
                    relevance_score=score,
                    result_type="column",
                    metadata=column_info
                ))
        
        return sorted(results, key=lambda x: x.relevance_score, reverse=True)[:limit]
    
    def search_relationships(self, query: str, limit: int = 5) -> List[SearchResult]:
        """Поиск связей между таблицами"""
        results = []
        query_lower = query.lower()
        
        for rel in self.relationship_index:
            score = 0.0
            
            # Поиск в именах таблиц
            if any(query_lower in table.lower() for table in [rel['from'], rel['to']]):
                score += 1.0
            
            # Поиск в именах колонок
            from_parts = rel['from'].split('.')
            to_parts = rel['to'].split('.')
            
            if len(from_parts) > 1 and query_lower in from_parts[-1].lower():
                score += 0.8
            if len(to_parts) > 1 and query_lower in to_parts[-1].lower():
                score += 0.8
            
            if score > 0:
                results.append(SearchResult(
                    content=f"Relationship: {rel['from']} -> {rel['to']}",
                    relevance_score=score,
                    result_type="relationship",
                    metadata=rel
                ))
        
        return sorted(results, key=lambda x: x.relevance_score, reverse=True)[:limit]
    
    def semantic_search(self, query: str, limit: int = 10, min_score: float = 0.1) -> List[SearchResult]:
        """Семантический поиск по всему индексу"""
        if not self.vectorizer or not self.document_vectors:
            logger.warning("Vector index not available. Falling back to keyword search.")
            return self.keyword_search(query, limit)
        
        try:
            # Векторизуем запрос
            if HAS_SKLEARN and hasattr(self.vectorizer, 'transform'):
                query_vector = self.vectorizer.transform([query]).toarray()[0].tolist()
            else:
                query_vector = self.vectorizer.transform([query])[0]
            
            results = []
            
            # Вычисляем сходство с каждым документом
            for i, doc_vector in enumerate(self.document_vectors):
                if HAS_SKLEARN and HAS_NUMPY:
                    similarity = cosine_similarity([query_vector], [doc_vector])[0][0]
                else:
                    similarity = cosine_similarity_simple(query_vector, doc_vector)
                
                if similarity >= min_score:
                    metadata = self.document_metadata[i]
                    
                    content = f"{metadata['type'].title()}: {metadata['name']}"
                    if metadata['type'] == 'column':
                        content += f" ({metadata['data']['type']})"
                    elif metadata['type'] == 'business_term':
                        content += f" - {metadata['data'].definition}"
                    
                    results.append(SearchResult(
                        content=content,
                        relevance_score=similarity,
                        result_type=metadata['type'],
                        metadata=metadata['data']
                    ))
            
            return sorted(results, key=lambda x: x.relevance_score, reverse=True)[:limit]
        
        except Exception as e:
            logger.error(f"Semantic search failed: {e}. Falling back to keyword search.")
            return self.keyword_search(query, limit)
    
    def keyword_search(self, query: str, limit: int = 10) -> List[SearchResult]:
        """Поиск по ключевым словам (fallback)"""
        all_results = []
        
        # Поиск в таблицах
        all_results.extend(self.search_tables(query, limit // 2))
        
        # Поиск в колонках
        all_results.extend(self.search_columns(query, limit // 2))
        
        # Поиск в связях
        all_results.extend(self.search_relationships(query, limit // 4))
        
        # Поиск в бизнес-терминах
        business_results = self.business_dict.search_terms(query)
        for term, score in business_results[:limit // 4]:
            all_results.append(SearchResult(
                content=f"Business term: {term.term} - {term.definition}",
                relevance_score=score,
                result_type="business_term",
                metadata=term.__dict__
            ))
        
        return sorted(all_results, key=lambda x: x.relevance_score, reverse=True)[:limit]
    
    def search(self, query: str, search_type: str = "semantic", limit: int = 10) -> List[SearchResult]:
        """Универсальный поиск"""
        if search_type == "semantic":
            return self.semantic_search(query, limit)
        elif search_type == "tables":
            return self.search_tables(query, limit)
        elif search_type == "columns":
            return self.search_columns(query, limit)
        elif search_type == "relationships":
            return self.search_relationships(query, limit)
        else:
            return self.keyword_search(query, limit)
    
    def get_table_context(self, table_name: str) -> Dict[str, Any]:
        """Получает полный контекст таблицы"""
        if table_name not in self.table_index:
            return {}
        
        table_info = self.table_index[table_name]
        
        # Ищем связи
        related_tables = []
        for rel in self.relationship_index:
            if table_name in rel['from']:
                related_tables.append(rel['to'].split('.')[1] if '.' in rel['to'] else rel['to'])
            elif table_name in rel['to']:
                related_tables.append(rel['from'].split('.')[1] if '.' in rel['from'] else rel['from'])
        
        # Ищем связанные бизнес-термины
        related_terms = []
        for term_key, term in self.business_dict.terms.items():
            if table_name.lower() in term.related_tables or any(col in term.related_columns for col in table_info['columns']):
                if term not in related_terms:
                    related_terms.append(term)
        
        return {
            "table_info": table_info,
            "related_tables": list(set(related_tables)),
            "related_terms": [term.__dict__ for term in related_terms[:5]],
            "columns": [self.column_index.get(f"{table_name}.{col}", {}) for col in table_info['columns']]
        }
    
    def suggest_joins(self, tables: List[str]) -> List[Dict[str, str]]:
        """Предлагает варианты JOIN между таблицами"""
        joins = []
        
        for i, table1 in enumerate(tables):
            for table2 in tables[i+1:]:
                # Ищем прямые связи
                for rel in self.relationship_index:
                    if (table1 in rel['from'] and table2 in rel['to']) or \
                       (table2 in rel['from'] and table1 in rel['to']):
                        joins.append({
                            "from_table": table1,
                            "to_table": table2,
                            "join_condition": f"{rel['from']} = {rel['to']}",
                            "join_type": "INNER"
                        })
        
        return joins


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Schema Retriever Test')
    parser.add_argument('--schema', type=str, default='schema.json', help='Schema JSON file')
    parser.add_argument('--query', type=str, required=True, help='Search query')
    parser.add_argument('--type', type=str, default='semantic', 
                       choices=['semantic', 'tables', 'columns', 'relationships', 'keyword'],
                       help='Search type')
    parser.add_argument('--limit', type=int, default=10, help='Maximum number of results')
    
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.INFO)
    
    # Создаем ретривер
    retriever = SchemaRetriever(args.schema)
    
    # Выполняем поиск
    results = retriever.search(args.query, args.type, args.limit)
    
    print(f"🔍 Search query: '{args.query}'")
    print(f"📋 Search type: {args.type}")
    print(f"📊 Found {len(results)} results:\n")
    
    for i, result in enumerate(results, 1):
        print(f"{i}. {result.content}")
        print(f"   Score: {result.relevance_score:.3f}")
        print(f"   Type: {result.result_type}")
        if result.result_type == "table":
            print(f"   Columns: {result.metadata.get('column_count', 0)}")
        elif result.result_type == "column":
            print(f"   Table: {result.metadata.get('table', 'unknown')}")
            print(f"   Tags: {', '.join(result.metadata.get('tags', []))}")
        print()
