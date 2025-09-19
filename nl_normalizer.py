"""
Natural Language Normalizer for BI-GPT Agent
Нормализация естественных запросов: синонимы, даты, числа
Поддержка русского, казахского и английского языков
"""

import re
import logging
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta, date
from enum import Enum
import calendar

logger = logging.getLogger(__name__)


class Language(Enum):
    """Поддерживаемые языки"""
    RUSSIAN = "ru"
    KAZAKH = "kz" 
    ENGLISH = "en"


@dataclass
class NormalizedQuery:
    """Нормализованный запрос"""
    original: str
    normalized: str
    detected_language: Language
    extracted_dates: List[Dict[str, Any]] = field(default_factory=list)
    extracted_numbers: List[Dict[str, Any]] = field(default_factory=list)
    business_terms: List[str] = field(default_factory=list)
    intent: Optional[str] = None
    confidence: float = 1.0


class LanguageDetector:
    """Простой детектор языка"""
    
    def __init__(self):
        self.patterns = {
            Language.RUSSIAN: [
                r'\b(покажи|показать|вывести|найти|получить|дай|дайте)\b',
                r'\b(клиенты|заказы|продажи|товары|прибыль|выручка)\b',
                r'\b(за|по|для|с|в|на|от|до)\b',
                r'\b(сегодня|вчера|неделя|месяц|год)\b'
            ],
            Language.KAZAKH: [
                r'\b(көрсет|табу|алу|беру)\b',
                r'\b(клиенттер|тапсырыстар|сатулар|тауарлар)\b',
                r'\b(үшін|бойынша|дейін|кейін)\b'
            ],
            Language.ENGLISH: [
                r'\b(show|get|find|display|list|select)\b',
                r'\b(customers|orders|sales|products|revenue|profit)\b',
                r'\b(for|by|from|to|with|in|on)\b',
                r'\b(today|yesterday|week|month|year)\b'
            ]
        }
    
    def detect(self, text: str) -> Language:
        """Определяет язык текста"""
        text_lower = text.lower()
        scores = {}
        
        for lang, patterns in self.patterns.items():
            score = 0
            for pattern in patterns:
                matches = len(re.findall(pattern, text_lower))
                score += matches
            scores[lang] = score
        
        # Возвращаем язык с наибольшим счетом
        if scores:
            detected_lang = max(scores, key=scores.get)
            if scores[detected_lang] > 0:
                return detected_lang
        
        # По умолчанию русский
        return Language.RUSSIAN


class SynonymNormalizer:
    """Нормализатор синонимов для разных языков"""
    
    def __init__(self):
        self.synonyms = {
            Language.RUSSIAN: {
                # Глаголы действий
                'покажи': ['покажи', 'показать', 'вывести', 'отобрази', 'дай', 'дайте', 'выведи'],
                'найди': ['найди', 'найти', 'отыщи', 'ищи', 'поиск'],
                'получи': ['получи', 'получить', 'взять', 'извлечь'],
                'выбери': ['выбери', 'выбрать', 'отбери', 'отобрать', 'фильтруй'],
                
                # Бизнес-сущности
                'клиенты': ['клиенты', 'покупатели', 'заказчики', 'потребители', 'пользователи', 'юзеры'],
                'заказы': ['заказы', 'покупки', 'сделки', 'транзакции', 'ордера'],
                'товары': ['товары', 'продукты', 'изделия', 'номенклатура', 'items', 'продукция'],
                'продажи': ['продажи', 'реализация', 'сбыт', 'sales'],
                'остатки': ['остатки', 'склад', 'запасы', 'инвентарь', 'остаток', 'stock'],
                
                # Финансовые метрики  
                'выручка': ['выручка', 'оборот', 'доходы', 'поступления', 'revenue'],
                'прибыль': ['прибыль', 'доход', 'профит', 'profit', 'чистая прибыль'],
                'маржа': ['маржа', 'маржинальность', 'рентабельность', 'доходность'],
                'средний_чек': ['средний чек', 'средний заказ', 'aov', 'average order value'],
                
                # Агрегации
                'количество': ['количество', 'число', 'кол-во', 'count', 'штук'],
                'сумма': ['сумма', 'итого', 'всего', 'total', 'sum'],
                'среднее': ['среднее', 'средний', 'avg', 'average'],
                'максимум': ['максимум', 'макс', 'max', 'maximum', 'наибольший'],
                'минимум': ['минимум', 'мин', 'min', 'minimum', 'наименьший'],
                
                # Временные периоды
                'сегодня': ['сегодня', 'today'],
                'вчера': ['вчера', 'yesterday'],
                'неделя': ['неделя', 'week', 'за неделю', 'за последнюю неделю'],
                'месяц': ['месяц', 'month', 'за месяц', 'за последний месяц'],
                'квартал': ['квартал', 'quarter', 'за квартал'],
                'год': ['год', 'year', 'за год', 'за последний год'],
                
                # Фильтры и условия
                'где': ['где', 'с условием', 'при условии', 'where'],
                'больше': ['больше', 'более', 'свыше', 'выше', 'greater', 'gt'],
                'меньше': ['меньше', 'менее', 'ниже', 'less', 'lt'],
                'равно': ['равно', 'равен', 'equal', 'eq', '='],
                'не_равно': ['не равно', 'не равен', 'not equal', 'ne', '!='],
                
                # Сортировка
                'топ': ['топ', 'лучшие', 'top', 'первые'],
                'худшие': ['худшие', 'worst', 'bottom'],
                'сортировка': ['сортировать', 'упорядочить', 'order by', 'sort']
            },
            
            Language.ENGLISH: {
                # Action verbs
                'show': ['show', 'display', 'list', 'get', 'fetch', 'retrieve'],
                'find': ['find', 'search', 'look for', 'locate'],
                'select': ['select', 'choose', 'pick', 'filter'],
                
                # Business entities
                'customers': ['customers', 'clients', 'users', 'buyers'],
                'orders': ['orders', 'purchases', 'transactions'],
                'products': ['products', 'items', 'goods', 'merchandise'],
                'sales': ['sales', 'revenue'],
                'inventory': ['inventory', 'stock', 'warehouse'],
                
                # Financial metrics
                'revenue': ['revenue', 'income', 'sales', 'turnover'],
                'profit': ['profit', 'earnings', 'net income'],
                'margin': ['margin', 'profitability'],
                'average_order': ['average order', 'aov', 'average order value'],
                
                # Aggregations
                'count': ['count', 'number', 'total number'],
                'sum': ['sum', 'total', 'amount'],
                'average': ['average', 'avg', 'mean'],
                'maximum': ['maximum', 'max', 'highest'],
                'minimum': ['minimum', 'min', 'lowest'],
                
                # Time periods
                'today': ['today'],
                'yesterday': ['yesterday'],
                'week': ['week', 'last week', 'this week'],
                'month': ['month', 'last month', 'this month'],
                'quarter': ['quarter', 'last quarter'],
                'year': ['year', 'last year', 'this year']
            },
            
            Language.KAZAKH: {
                # Действия
                'көрсет': ['көрсет', 'көрсетіңіз', 'шығар'],
                'тап': ['тап', 'табу', 'іздеу'],
                
                # Сущности
                'клиенттер': ['клиенттер', 'сатып алушылар'],
                'тапсырыстар': ['тапсырыстар', 'сатып алулар'],
                'тауарлар': ['тауарлар', 'өнімдер'],
                
                # Время
                'бүгін': ['бүгін'],
                'кеше': ['кеше'],
                'апта': ['апта', 'аптада'],
                'ай': ['ай', 'айда']
            }
        }
    
    def normalize_synonyms(self, text: str, language: Language) -> str:
        """Нормализует синонимы в тексте"""
        if language not in self.synonyms:
            return text
        
        normalized_text = text.lower()
        
        # Проходим по всем группам синонимов
        for canonical_form, synonym_list in self.synonyms[language].items():
            for synonym in synonym_list:
                # Используем границы слов для точного поиска
                pattern = r'\b' + re.escape(synonym) + r'\b'
                normalized_text = re.sub(pattern, canonical_form, normalized_text, flags=re.IGNORECASE)
        
        return normalized_text


class DateTimeNormalizer:
    """Нормализатор дат и времени"""
    
    def __init__(self, timezone: str = "Asia/Almaty"):
        self.timezone = timezone
        
        # Паттерны для разных языков
        self.date_patterns = {
            Language.RUSSIAN: {
                'сегодня': 'CURRENT_DATE',
                'вчера': 'CURRENT_DATE - INTERVAL 1 DAY',
                'завтра': 'CURRENT_DATE + INTERVAL 1 DAY',
                'за неделю': 'CURRENT_DATE - INTERVAL 7 DAY',
                'за месяц': 'CURRENT_DATE - INTERVAL 30 DAY', 
                'за квартал': 'CURRENT_DATE - INTERVAL 90 DAY',
                'за год': 'CURRENT_DATE - INTERVAL 365 DAY',
                'неделя': 'CURRENT_DATE - INTERVAL 7 DAY',
                'месяц': 'CURRENT_DATE - INTERVAL 30 DAY',
                'квартал': 'CURRENT_DATE - INTERVAL 90 DAY',
                'год': 'CURRENT_DATE - INTERVAL 365 DAY',
                r'за последние (\d+) дня?': r'CURRENT_DATE - INTERVAL \1 DAY',
                r'за последние (\d+) недель?': r'CURRENT_DATE - INTERVAL \1*7 DAY',
                r'за последние (\d+) месяцев?': r'CURRENT_DATE - INTERVAL \1*30 DAY',
                r'последние (\d+) дня?': r'CURRENT_DATE - INTERVAL \1 DAY',
                r'последние (\d+) недель?': r'CURRENT_DATE - INTERVAL \1*7 DAY',
                r'последние (\d+) месяцев?': r'CURRENT_DATE - INTERVAL \1*30 DAY'
            },
            Language.ENGLISH: {
                'today': 'CURRENT_DATE',
                'yesterday': 'CURRENT_DATE - INTERVAL 1 DAY',
                'tomorrow': 'CURRENT_DATE + INTERVAL 1 DAY',
                'last week': 'CURRENT_DATE - INTERVAL 7 DAY',
                'last month': 'CURRENT_DATE - INTERVAL 30 DAY',
                'last quarter': 'CURRENT_DATE - INTERVAL 90 DAY',
                'last year': 'CURRENT_DATE - INTERVAL 365 DAY',
                r'last (\d+) days?': r'CURRENT_DATE - INTERVAL \1 DAY',
                r'last (\d+) weeks?': r'CURRENT_DATE - INTERVAL \1*7 DAY',
                r'last (\d+) months?': r'CURRENT_DATE - INTERVAL \1*30 DAY'
            },
            Language.KAZAKH: {
                'бүгін': 'CURRENT_DATE',
                'кеше': 'CURRENT_DATE - INTERVAL 1 DAY',
                'ертең': 'CURRENT_DATE + INTERVAL 1 DAY',
                'апта': 'CURRENT_DATE - INTERVAL 7 DAY',
                'ай': 'CURRENT_DATE - INTERVAL 30 DAY'
            }
        }
        
        # Названия месяцев
        self.months = {
            Language.RUSSIAN: {
                'январь': 1, 'января': 1, 'февраль': 2, 'февраля': 2,
                'март': 3, 'марта': 3, 'апрель': 4, 'апреля': 4,
                'май': 5, 'мая': 5, 'июнь': 6, 'июня': 6,
                'июль': 7, 'июля': 7, 'август': 8, 'августа': 8,
                'сентябрь': 9, 'сентября': 9, 'октябрь': 10, 'октября': 10,
                'ноябрь': 11, 'ноября': 11, 'декабрь': 12, 'декабря': 12
            },
            Language.ENGLISH: {
                'january': 1, 'february': 2, 'march': 3, 'april': 4,
                'may': 5, 'june': 6, 'july': 7, 'august': 8,
                'september': 9, 'october': 10, 'november': 11, 'december': 12,
                'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4,
                'may': 5, 'jun': 6, 'jul': 7, 'aug': 8,
                'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
            }
        }
    
    def extract_dates(self, text: str, language: Language) -> List[Dict[str, Any]]:
        """Извлекает и нормализует даты из текста"""
        extracted_dates = []
        
        if language not in self.date_patterns:
            return extracted_dates
        
        patterns = self.date_patterns[language]
        
        for pattern, sql_expression in patterns.items():
            if isinstance(pattern, str):
                # Простая строка
                if pattern in text.lower():
                    extracted_dates.append({
                        'original': pattern,
                        'sql_expression': sql_expression,
                        'type': 'relative_date'
                    })
            else:
                # Регулярное выражение
                matches = re.finditer(pattern, text.lower())
                for match in matches:
                    # Заменяем группы в SQL выражении
                    sql_expr = sql_expression
                    for i, group in enumerate(match.groups(), 1):
                        sql_expr = sql_expr.replace(f'\\{i}', group)
                    
                    extracted_dates.append({
                        'original': match.group(),
                        'sql_expression': sql_expr,
                        'type': 'relative_date_with_number'
                    })
        
        # Ищем абсолютные даты (DD.MM.YYYY, YYYY-MM-DD)
        absolute_date_patterns = [
            r'\b(\d{1,2})\.(\d{1,2})\.(\d{4})\b',  # DD.MM.YYYY
            r'\b(\d{4})-(\d{1,2})-(\d{1,2})\b',   # YYYY-MM-DD
            r'\b(\d{1,2})/(\d{1,2})/(\d{4})\b'    # DD/MM/YYYY
        ]
        
        for pattern in absolute_date_patterns:
            matches = re.finditer(pattern, text)
            for match in matches:
                if pattern.endswith(r'\.(\d{4})\b'):  # DD.MM.YYYY
                    day, month, year = match.groups()
                elif pattern.endswith(r'-(\d{1,2})\b'):  # YYYY-MM-DD
                    year, month, day = match.groups()
                else:  # DD/MM/YYYY
                    day, month, year = match.groups()
                
                try:
                    # Валидируем дату
                    date_obj = datetime(int(year), int(month), int(day))
                    sql_date = f"'{date_obj.strftime('%Y-%m-%d')}'"
                    
                    extracted_dates.append({
                        'original': match.group(),
                        'sql_expression': sql_date,
                        'type': 'absolute_date',
                        'parsed_date': date_obj.isoformat()
                    })
                except ValueError:
                    # Неправильная дата, пропускаем
                    continue
        
        return extracted_dates
    
    def normalize_dates(self, text: str, language: Language) -> str:
        """Заменяет даты в тексте на SQL выражения"""
        normalized_text = text
        extracted_dates = self.extract_dates(text, language)
        
        # Заменяем в порядке убывания длины, чтобы избежать частичных замен
        for date_info in sorted(extracted_dates, key=lambda x: len(x['original']), reverse=True):
            normalized_text = normalized_text.replace(
                date_info['original'], 
                f"[DATE:{date_info['sql_expression']}]"
            )
        
        return normalized_text


class NumberNormalizer:
    """Нормализатор чисел и количественных выражений"""
    
    def __init__(self):
        # Словесные числа
        self.number_words = {
            Language.RUSSIAN: {
                'один': 1, 'одна': 1, 'два': 2, 'две': 2, 'три': 3, 'четыре': 4, 'пять': 5,
                'шесть': 6, 'семь': 7, 'восемь': 8, 'девять': 9, 'десять': 10,
                'одиннадцать': 11, 'двенадцать': 12, 'тринадцать': 13, 'четырнадцать': 14, 'пятнадцать': 15,
                'двадцать': 20, 'тридцать': 30, 'сорок': 40, 'пятьдесят': 50,
                'сто': 100, 'тысяча': 1000, 'миллион': 1000000
            },
            Language.ENGLISH: {
                'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5,
                'six': 6, 'seven': 7, 'eight': 8, 'nine': 9, 'ten': 10,
                'eleven': 11, 'twelve': 12, 'thirteen': 13, 'fourteen': 14, 'fifteen': 15,
                'twenty': 20, 'thirty': 30, 'forty': 40, 'fifty': 50,
                'hundred': 100, 'thousand': 1000, 'million': 1000000
            }
        }
    
    def extract_numbers(self, text: str, language: Language) -> List[Dict[str, Any]]:
        """Извлекает числа из текста"""
        extracted_numbers = []
        
        # Цифровые числа
        digit_patterns = [
            r'\b(\d+(?:\.\d+)?)\s*(?:тыс|тысяч|thousand|k)\b',  # Тысячи
            r'\b(\d+(?:\.\d+)?)\s*(?:млн|миллионов?|million|m)\b',  # Миллионы
            r'\b(\d+(?:\.\d+)?)\s*(?:млрд|миллиардов?|billion|b)\b',  # Миллиарды
            r'\b(\d+(?:[\.,]\d+)?)\b'  # Простые числа
        ]
        
        for pattern in digit_patterns:
            matches = re.finditer(pattern, text.lower())
            for match in matches:
                number_str = match.group(1) if match.groups() else match.group()
                full_match = match.group()
                
                try:
                    # Обрабатываем запятые как десятичные разделители
                    number_str = number_str.replace(',', '.')
                    base_number = float(number_str)
                    
                    # Применяем множители
                    if any(unit in full_match for unit in ['тыс', 'тысяч', 'thousand', 'k']):
                        final_number = base_number * 1000
                    elif any(unit in full_match for unit in ['млн', 'миллионов', 'миллион', 'million', 'm']):
                        final_number = base_number * 1000000
                    elif any(unit in full_match for unit in ['млрд', 'миллиардов', 'миллиард', 'billion', 'b']):
                        final_number = base_number * 1000000000
                    else:
                        final_number = base_number
                    
                    extracted_numbers.append({
                        'original': full_match,
                        'value': final_number,
                        'type': 'numeric'
                    })
                except ValueError:
                    continue
        
        # Словесные числа
        if language in self.number_words:
            for word, value in self.number_words[language].items():
                if word in text.lower():
                    extracted_numbers.append({
                        'original': word,
                        'value': value,
                        'type': 'word_number'
                    })
        
        return extracted_numbers


class IntentClassifier:
    """Классификатор намерений пользователя"""
    
    def __init__(self):
        self.intent_patterns = {
            'select': [
                r'\b(покажи|показать|вывести|найди|получи|дай|выбери|list|show|get|select|find|display)\b'
            ],
            'count': [
                r'\b(количество|число|кол-во|сколько|count|number of)\b'
            ],
            'aggregate': [
                r'\b(сумма|итого|всего|среднее|средний|максимум|минимум|sum|total|average|avg|max|min)\b'
            ],
            'filter': [
                r'\b(где|с условием|при условии|больше|меньше|равно|where|with|having|greater|less|equal)\b'
            ],
            'top': [
                r'\b(топ|лучшие|первые|top|best|highest|largest)\b'
            ],
            'trend': [
                r'\b(динамика|тренд|изменение|рост|снижение|trend|growth|change|over time)\b'
            ],
            'compare': [
                r'\b(сравни|сравнение|против|vs|compare|comparison|versus)\b'
            ]
        }
    
    def classify_intent(self, text: str) -> Tuple[Optional[str], float]:
        """Определяет намерение пользователя"""
        text_lower = text.lower()
        scores = {}
        
        for intent, patterns in self.intent_patterns.items():
            score = 0
            for pattern in patterns:
                matches = len(re.findall(pattern, text_lower))
                score += matches
            
            if score > 0:
                scores[intent] = score
        
        if scores:
            intent = max(scores, key=scores.get)
            confidence = scores[intent] / len(text.split())  # Нормализуем по длине текста
            return intent, min(confidence, 1.0)
        
        return None, 0.0


class NLNormalizer:
    """Основной класс нормализатора естественного языка"""
    
    def __init__(self):
        self.language_detector = LanguageDetector()
        self.synonym_normalizer = SynonymNormalizer()
        self.datetime_normalizer = DateTimeNormalizer()
        self.number_normalizer = NumberNormalizer()
        self.intent_classifier = IntentClassifier()
    
    def normalize(self, query: str) -> NormalizedQuery:
        """Выполняет полную нормализацию запроса"""
        logger.debug(f"Normalizing query: {query}")
        
        # Определяем язык
        detected_language = self.language_detector.detect(query)
        logger.debug(f"Detected language: {detected_language}")
        
        # Нормализуем синонимы
        normalized_text = self.synonym_normalizer.normalize_synonyms(query, detected_language)
        logger.debug(f"After synonym normalization: {normalized_text}")
        
        # Извлекаем и нормализуем даты
        extracted_dates = self.datetime_normalizer.extract_dates(normalized_text, detected_language)
        normalized_text = self.datetime_normalizer.normalize_dates(normalized_text, detected_language)
        logger.debug(f"After date normalization: {normalized_text}")
        
        # Извлекаем числа
        extracted_numbers = self.number_normalizer.extract_numbers(normalized_text, detected_language)
        logger.debug(f"Extracted numbers: {extracted_numbers}")
        
        # Определяем намерение
        intent, confidence = self.intent_classifier.classify_intent(normalized_text)
        logger.debug(f"Detected intent: {intent} (confidence: {confidence})")
        
        # Извлекаем бизнес-термины (простая эвристика)
        business_terms = self._extract_business_terms(normalized_text, detected_language)
        
        return NormalizedQuery(
            original=query,
            normalized=normalized_text,
            detected_language=detected_language,
            extracted_dates=extracted_dates,
            extracted_numbers=extracted_numbers,
            business_terms=business_terms,
            intent=intent,
            confidence=confidence
        )
    
    def _extract_business_terms(self, text: str, language: Language) -> List[str]:
        """Извлекает бизнес-термины из нормализованного текста"""
        business_term_patterns = {
            Language.RUSSIAN: [
                'клиенты', 'заказы', 'товары', 'продажи', 'остатки',
                'выручка', 'прибыль', 'маржа', 'средний_чек',
                'количество', 'сумма', 'среднее', 'максимум', 'минимум'
            ],
            Language.ENGLISH: [
                'customers', 'orders', 'products', 'sales', 'inventory',
                'revenue', 'profit', 'margin', 'average_order',
                'count', 'sum', 'average', 'maximum', 'minimum'
            ]
        }
        
        terms = []
        if language in business_term_patterns:
            for term in business_term_patterns[language]:
                if term in text.lower():
                    terms.append(term)
        
        return terms


def main():
    """Функция для тестирования нормализатора"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Natural Language Normalizer Test')
    parser.add_argument('--query', type=str, required=True, help='Query to normalize')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    
    # Создаем нормализатор
    normalizer = NLNormalizer()
    
    # Нормализуем запрос
    result = normalizer.normalize(args.query)
    
    print(f"🔤 Original query: {result.original}")
    print(f"🌐 Detected language: {result.detected_language.value}")
    print(f"✨ Normalized query: {result.normalized}")
    print(f"🎯 Intent: {result.intent} (confidence: {result.confidence:.3f})")
    
    if result.extracted_dates:
        print(f"📅 Extracted dates:")
        for date_info in result.extracted_dates:
            print(f"   - {date_info['original']} -> {date_info['sql_expression']}")
    
    if result.extracted_numbers:
        print(f"🔢 Extracted numbers:")
        for number_info in result.extracted_numbers:
            print(f"   - {number_info['original']} -> {number_info['value']}")
    
    if result.business_terms:
        print(f"💼 Business terms: {', '.join(result.business_terms)}")


if __name__ == "__main__":
    main()
