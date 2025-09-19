#!/usr/bin/env python3
"""
Интегрированное приложение BI-GPT Agent
Объединяет основную функциональность с backend архитектурой
"""

import streamlit as st
import sys
import os
from typing import Optional
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
import time

# Добавляем пути для импорта
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'backend'))

# Импорты из основного приложения
try:
    from bi_gpt_agent import BIGPTAgent, QueryMetrics
    from config import get_settings
    MAIN_APP_AVAILABLE = True
except ImportError as e:
    st.error(f"❌ Не удалось загрузить основное приложение: {e}")
    MAIN_APP_AVAILABLE = False

# Импорты из backend
try:
    from app.config.settings import UIText
    from app.infrastructure.db.postgres import PostgresConfig, make_postgres_engine
    from app.application.use_cases import GetSchemaOverviewUC
    BACKEND_AVAILABLE = True
except ImportError as e:
    st.error(f"❌ Не удалось загрузить backend: {e}")
    BACKEND_AVAILABLE = False

# Конфигурация страницы
st.set_page_config(
    page_title="BI-GPT Agent - Integrated",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Кастомные стили
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        margin: 0.5rem 0;
    }
    .success-box {
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        border-radius: 5px;
        padding: 1rem;
        margin: 1rem 0;
    }
    .error-box {
        background-color: #f8d7da;
        border: 1px solid #f5c6cb;
        border-radius: 5px;
        padding: 1rem;
        margin: 1rem 0;
    }
    .info-box {
        background-color: #d1ecf1;
        border: 1px solid #bee5eb;
        border-radius: 5px;
        padding: 1rem;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

def analyze_sql_error(error_message: str) -> str:
    """Анализирует ошибку SQL и возвращает рекомендации"""
    error_lower = error_message.lower()
    
    if "column" in error_lower and "does not exist" in error_lower:
        return "Ошибка: обращение к несуществующей колонке. Проверьте правильность названий полей в схеме БД."
    
    elif "table" in error_lower and ("does not exist" in error_lower or "doesn't exist" in error_lower):
        return "Ошибка: обращение к несуществующей таблице. Проверьте правильность названий таблиц в схеме БД."
    
    elif "syntax error" in error_lower:
        return "Ошибка синтаксиса SQL. Проверьте правильность написания SQL команд."
    
    elif "permission denied" in error_lower or "access denied" in error_lower:
        return "Ошибка доступа. Недостаточно прав для выполнения операции."
    
    elif "foreign key" in error_lower:
        return "Ошибка внешнего ключа. Проверьте связи между таблицами."
    
    elif "duplicate key" in error_lower:
        return "Ошибка дублирования ключа. Попытка вставить дублирующееся значение в уникальное поле."
    
    elif "timeout" in error_lower:
        return "Превышено время ожидания. Запрос выполняется слишком долго."
    
    elif "connection" in error_lower:
        return "Ошибка подключения к базе данных. Проверьте соединение."
    
    else:
        return "Неизвестная ошибка. Проверьте синтаксис SQL и схему базы данных."

def init_session_state():
    """Инициализация состояния сессии"""
    if "engine" not in st.session_state:
        st.session_state.engine = None
    if "last_error" not in st.session_state:
        st.session_state.last_error = None
    if "agent" not in st.session_state:
        st.session_state.agent = None
    if "query_history" not in st.session_state:
        st.session_state.query_history = []
    if "schema_overview" not in st.session_state:
        st.session_state.schema_overview = None
    if "temperature" not in st.session_state:
        st.session_state.temperature = 0.0
    if "max_tokens" not in st.session_state:
        st.session_state.max_tokens = 400
    if "model_choice" not in st.session_state:
        st.session_state.model_choice = "Fine-tuned Phi-3 + LoRA"
    if "show_debug_info" not in st.session_state:
        st.session_state.show_debug_info = True
    if "enable_validation" not in st.session_state:
        st.session_state.enable_validation = True
    if "prompt_mode" not in st.session_state:
        st.session_state.prompt_mode = "few_shot"

def render_database_connection():
    """Рендер формы подключения к базе данных"""
    st.sidebar.header("🔌 Подключение к базе данных")
    
    host = st.sidebar.text_input("Host", value="localhost")
    port = st.sidebar.number_input("Port", min_value=1, max_value=65535, value=5432, step=1)
    user = st.sidebar.text_input("Username", value="olgasnissarenko")
    password = st.sidebar.text_input("Password", type="password", value="")
    database = st.sidebar.text_input("Database Name", value="bi_demo")
    sslmode = st.sidebar.selectbox("SSL Mode", options=["", "require", "disable"], index=2, help="Опционально")
    
    st.sidebar.divider()
    
    # Выбор модели
    st.sidebar.header("🤖 Выбор модели")
    model_choice = st.sidebar.radio(
        "Модель для генерации SQL:",
        ["Fine-tuned Phi-3 + LoRA", "Custom API Model"],
        index=0,
        help="Fine-tuned модель работает локально, Custom API требует URL и ключ"
    )
    
    # Дополнительные настройки для Custom API
    if model_choice == "Custom API Model":
        api_url = st.sidebar.text_input(
            "API URL", 
            value="https://vsjz8fv63q4oju-8000.proxy.runpod.net/v1",
            help="Введите URL вашего API endpoint"
        )
        api_key = st.sidebar.text_input("API Key", type="password", help="Введите ваш API ключ")
        if not api_key:
            st.sidebar.warning("⚠️ Для использования Custom API нужен API ключ")
    
    # Настройки генерации SQL
    st.sidebar.divider()
    st.sidebar.header("⚙️ Настройки генерации")
    
    # Режим промпта
    prompt_mode = st.sidebar.radio(
        "Режим промпта:",
        ["Few-shot (с примерами)", "One-shot (простой)"],
        index=0,
        help="Few-shot: сложный промпт с примерами запросов. One-shot: простой промпт только с правилами."
    )
    
    # Конвертируем в формат для API
    prompt_mode_value = "few_shot" if prompt_mode == "Few-shot (с примерами)" else "one_shot"
    
    temperature = st.sidebar.slider(
        "Temperature", 
        min_value=0.0, 
        max_value=2.0, 
        value=0.0, 
        step=0.1,
        help="Контролирует случайность ответов. 0 = детерминированный, 2 = очень случайный"
    )
    
    max_tokens = st.sidebar.slider(
        "Max Tokens", 
        min_value=100, 
        max_value=1000, 
        value=400, 
        step=50,
        help="Максимальное количество токенов в ответе"
    )
    
    # Дополнительные настройки
    with st.sidebar.expander("🔧 Дополнительные настройки"):
        show_debug_info = st.checkbox("Показать отладочную информацию", value=True)
        enable_validation = st.checkbox("Включить расширенную валидацию SQL", value=True)
        auto_retry = st.checkbox("Автоматические повторы при ошибках", value=True)
        max_retries = st.number_input("Максимум повторов", min_value=1, max_value=5, value=2)
    
    # Сохраняем настройки в session_state
    st.session_state.temperature = temperature
    st.session_state.max_tokens = max_tokens
    st.session_state.model_choice = model_choice
    st.session_state.show_debug_info = show_debug_info
    st.session_state.enable_validation = enable_validation
    st.session_state.prompt_mode = prompt_mode_value
    
    connect_clicked = st.sidebar.button("🔗 Подключиться", type="primary")
    
    if connect_clicked:
        try:
            cfg = PostgresConfig(
                host=host, port=int(port), user=user, password=password,
                database=database, sslmode=(sslmode or None)
            )
            st.session_state.engine = make_postgres_engine(cfg)
            st.session_state.last_error = None
            
            # Получаем схему базы данных
            if BACKEND_AVAILABLE:
                uc = GetSchemaOverviewUC(st.session_state.engine, schema="public")
                st.session_state.schema_overview = uc.execute()
            
            # Инициализируем агента
            if MAIN_APP_AVAILABLE and st.session_state.engine:
                try:
                    # Определяем параметры для инициализации агента
                    if model_choice == "Fine-tuned Phi-3 + LoRA":
                        st.session_state.agent = BIGPTAgent(
                            use_finetuned=True,
                            model_provider="finetuned"
                        )
                        st.sidebar.success("✅ Подключение успешно! Fine-tuned агент инициализирован.")
                    else:
                        # Custom API Model
                        if not api_key:
                            st.sidebar.error("❌ Для Custom API нужен API ключ")
                        else:
                            st.session_state.agent = BIGPTAgent(
                                api_key=api_key,
                                base_url=api_url,
                                model_provider="local"
                            )
                            st.sidebar.success("✅ Подключение успешно! Custom API агент инициализирован.")
                except Exception as e:
                    st.sidebar.warning(f"⚠️ Подключение к БД успешно, но агент не инициализирован: {e}")
            else:
                st.sidebar.success("✅ Подключение к базе данных успешно!")
                
        except Exception as e:
            st.session_state.engine = None
            st.session_state.last_error = str(e)
            st.sidebar.error("❌ Ошибка подключения")
            st.sidebar.code(str(e))

def render_schema_overview():
    """Рендер обзора схемы базы данных"""
    if not st.session_state.schema_overview:
        return
    
    st.subheader("🧱 Схема базы данных")
    
    # Метрики
    c1, c2, c3 = st.columns(3)
    c1.metric("Таблиц", st.session_state.schema_overview.tables_count)
    c2.metric("Колонок", st.session_state.schema_overview.columns_count)
    c3.metric("Тип БД", st.session_state.schema_overview.db_type)
    
    st.divider()
    
    # Детали таблиц
    for tbl in st.session_state.schema_overview.tables:
        with st.expander(f"📊 {tbl.name} ({len(tbl.columns)} колонок)"):
            data = [{
                "Колонка": col.name,
                "Тип": col.data_type,
                "Nullable": "✓" if col.is_nullable else "✗",
                "По умолчанию": col.default if col.default is not None else "None"
            } for col in tbl.columns]
            st.dataframe(data, use_container_width=True)

def render_natural_language_query():
    """Рендер интерфейса для естественного языка"""
    if not st.session_state.agent:
        st.info("🔌 Сначала подключитесь к базе данных")
        return
    
    st.subheader("💬 Задайте вопрос на естественном языке")
    
    # Поле ввода
    user_query = st.text_area(
        "Введите ваш вопрос:",
        placeholder="Например: Покажи всех клиентов из Москвы с суммой заказов больше 10000",
        height=100
    )
    
    col1, col2 = st.columns([1, 4])
    
    with col1:
        generate_clicked = st.button("🚀 Сгенерировать SQL", type="primary")
    
    with col2:
        if st.button("📊 Показать примеры"):
            st.session_state.show_examples = not st.session_state.get('show_examples', False)
    
    # Примеры запросов
    if st.session_state.get('show_examples', False):
        st.info("""
        **Примеры запросов:**
        - Покажи всех клиентов из Москвы
        - Какая средняя сумма заказа по месяцам?
        - Топ-10 товаров по продажам
        - Покажи динамику выручки за последний год
        - Сколько заказов было вчера?
        """)
    
    if generate_clicked and user_query:
        # Показываем параметры генерации
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Temperature", f"{st.session_state.get('temperature', 0.0):.1f}")
        with col2:
            st.metric("Max Tokens", st.session_state.get('max_tokens', 400))
        with col3:
            st.metric("Модель", st.session_state.get('model_choice', 'Fine-tuned Phi-3 + LoRA'))
        with col4:
            prompt_mode_display = "Few-shot" if st.session_state.get('prompt_mode', 'few_shot') == 'few_shot' else "One-shot"
            st.metric("Режим промпта", prompt_mode_display)
        
        # Создаем контейнеры для отображения прогресса
        progress_container = st.container()
        attempts_container = st.container()
        
        with st.spinner("🤖 Генерирую PostgreSQL SQL запрос..."):
            try:
                # Получаем настройки из sidebar
                temperature = st.session_state.get('temperature', 0.0)
                max_tokens = st.session_state.get('max_tokens', 400)
                
                # Генерируем SQL с параметрами
                result = st.session_state.agent.process_query(
                    user_query, 
                    temperature=temperature, 
                    max_tokens=max_tokens,
                    prompt_mode=st.session_state.get('prompt_mode', 'few_shot')
                )
                
                # Показываем информацию о попытках
                if result.get('attempts_info'):
                    with attempts_container:
                        st.subheader("🔄 Процесс генерации")
                        
                        attempts_info = result['attempts_info']
                        total_attempts = len(attempts_info)
                        successful_attempts = sum(1 for attempt in attempts_info if attempt['success'])
                        
                        # Прогресс-бар
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        
                        for i, attempt in enumerate(attempts_info):
                            attempt_num = attempt['attempt']
                            
                            if attempt['success']:
                                status_text.success(f"✅ Попытка {attempt_num} успешна! Время: {attempt['generation_time']:.3f}с")
                                progress_bar.progress(1.0)
                            else:
                                status_text.warning(f"⚠️ Попытка {attempt_num} неудачна. Время: {attempt['generation_time']:.3f}с")
                                progress_bar.progress(attempt_num / total_attempts)
                                
                                # Показываем детали ошибки
                                with st.expander(f"❌ Детали ошибки попытки {attempt_num}", expanded=False):
                                    st.error(f"**Тип ошибки:** {attempt.get('error_type', 'Unknown')}")
                                    st.code(attempt['error'], language='text')
                                    
                                    # Анализ ошибки
                                    error_analysis = analyze_sql_error(attempt['error'])
                                    if error_analysis:
                                        st.info(f"**Анализ:** {error_analysis}")
                                
                                # Небольшая задержка для визуального эффекта
                                import time
                                time.sleep(0.5)
                        
                        # Итоговая статистика
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Всего попыток", total_attempts)
                        with col2:
                            st.metric("Успешных", successful_attempts)
                        with col3:
                            st.metric("Неудачных", total_attempts - successful_attempts)
                
                if result and result.get('sql'):
                    st.success("✅ PostgreSQL SQL запрос сгенерирован!")
                    
                    # Показываем информацию о генерации
                    if st.session_state.get('show_debug_info', True):
                        with st.expander("🔍 Информация о генерации", expanded=False):
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                if result.get('metrics'):
                                    metrics = result['metrics']
                                    st.write(f"**Время генерации:** {metrics.execution_time:.3f}с")
                                    st.write(f"**Точность SQL:** {'✅' if metrics.sql_accuracy else '❌'}")
                                    st.write(f"**Бизнес-термины:** {metrics.business_terms_used}")
                                    st.write(f"**PII обнаружено:** {'⚠️' if metrics.pii_detected else '✅'}")
                            
                            with col2:
                                st.write(f"**Temperature:** {temperature}")
                                st.write(f"**Max Tokens:** {max_tokens}")
                                st.write(f"**Модель:** {st.session_state.get('model_choice', 'Unknown')}")
                                if result.get('business_terms'):
                                    st.write(f"**Найденные термины:** {', '.join(result['business_terms'][:3])}")
                    
                    # Показываем SQL
                    st.subheader("📝 Сгенерированный SQL:")
                    st.code(result['sql'], language='sql')
                    
                    # Показываем анализ рисков если включен
                    if st.session_state.get('enable_validation', True) and result.get('risk_analysis'):
                        risk_analysis = result['risk_analysis']
                        
                        # Цветовая индикация риска
                        risk_colors = {
                            'low': '🟢',
                            'medium': '🟡', 
                            'high': '🟠',
                            'critical': '🔴'
                        }
                        
                        risk_icon = risk_colors.get(risk_analysis.risk_level.value, '⚪')
                        
                        with st.expander(f"{risk_icon} Анализ рисков SQL", expanded=False):
                            col1, col2, col3 = st.columns(3)
                            
                            with col1:
                                st.metric("Уровень риска", risk_analysis.risk_level.value.upper())
                                st.metric("Сложность", risk_analysis.complexity_score)
                            
                            with col2:
                                st.metric("JOIN'ов", risk_analysis.join_count)
                                st.metric("Подзапросов", risk_analysis.subquery_count)
                            
                            with col3:
                                st.metric("Ошибок", len(risk_analysis.errors))
                                st.metric("Предупреждений", len(risk_analysis.warnings))
                            
                            if risk_analysis.errors:
                                st.error("**Ошибки:**")
                                for error in risk_analysis.errors:
                                    st.write(f"• {error}")
                            
                            if risk_analysis.warnings:
                                st.warning("**Предупреждения:**")
                                for warning in risk_analysis.warnings:
                                    st.write(f"• {warning}")
                            
                            if risk_analysis.recommendations:
                                st.info("**Рекомендации:**")
                                for rec in risk_analysis.recommendations:
                                    st.write(f"• {rec}")
                    
                    # Показываем результаты
                    if result.get('data') is not None:
                        st.subheader("📊 Результаты запроса:")
                        
                        if isinstance(result['data'], pd.DataFrame) and not result['data'].empty:
                            st.dataframe(result['data'], use_container_width=True)
                            
                            # Простые визуализации
                            if len(result['data']) > 1:
                                numeric_cols = result['data'].select_dtypes(include=['number']).columns
                                if len(numeric_cols) > 0:
                                    st.subheader("📈 Визуализация")
                                    
                                    chart_type = st.selectbox(
                                        "Тип графика:",
                                        ["Столбчатая диаграмма", "Линейный график", "Круговая диаграмма"]
                                    )
                                    
                                    if chart_type == "Столбчатая диаграмма":
                                        fig = px.bar(result['data'], x=result['data'].columns[0], y=numeric_cols[0])
                                        st.plotly_chart(fig, use_container_width=True)
                                    elif chart_type == "Линейный график":
                                        fig = px.line(result['data'], x=result['data'].columns[0], y=numeric_cols[0])
                                        st.plotly_chart(fig, use_container_width=True)
                                    elif chart_type == "Круговая диаграмма":
                                        fig = px.pie(result['data'], values=numeric_cols[0], names=result['data'].columns[0])
                                        st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.info("Запрос выполнен, но данных не найдено")
                    
                    # Сохраняем в историю с дополнительной информацией
                    history_item = {
                        'query': user_query,
                        'sql': result['sql'],
                        'timestamp': datetime.now(),
                        'success': True,
                        'temperature': temperature,
                        'max_tokens': max_tokens,
                        'model': st.session_state.get('model_choice', 'Unknown')
                    }
                    
                    # Добавляем метрики если есть
                    if result.get('metrics'):
                        metrics = result['metrics']
                        history_item.update({
                            'execution_time': metrics.execution_time,
                            'sql_accuracy': metrics.sql_accuracy,
                            'business_terms_used': metrics.business_terms_used,
                            'pii_detected': metrics.pii_detected
                        })
                    
                    # Добавляем анализ рисков если есть
                    if result.get('risk_analysis'):
                        risk = result['risk_analysis']
                        history_item.update({
                            'risk_level': risk.risk_level.value,
                            'complexity_score': risk.complexity_score,
                            'join_count': risk.join_count,
                            'subquery_count': risk.subquery_count
                        })
                    
                    st.session_state.query_history.append(history_item)
                else:
                    st.error("❌ Не удалось сгенерировать PostgreSQL SQL запрос")
                    
                    # Показываем информацию о попытках если есть
                    if result.get('attempts_info'):
                        with st.expander("🔄 Детали неудачных попыток", expanded=True):
                            for attempt in result['attempts_info']:
                                if not attempt['success']:
                                    st.error(f"**Попытка {attempt['attempt']}:** {attempt.get('error_type', 'Unknown')}")
                                    st.code(attempt['error'], language='text')
                                    
                                    # Анализ ошибки
                                    error_analysis = analyze_sql_error(attempt['error'])
                                    if error_analysis:
                                        st.info(f"**Анализ:** {error_analysis}")
                    
                    # Сохраняем ошибку в историю
                    history_item = {
                        'query': user_query,
                        'sql': None,
                        'timestamp': datetime.now(),
                        'success': False,
                        'error': 'Не удалось сгенерировать SQL запрос',
                        'temperature': temperature,
                        'max_tokens': max_tokens,
                        'model': st.session_state.get('model_choice', 'Unknown')
                    }
                    
                    if result.get('attempts_info'):
                        history_item['attempts_info'] = result['attempts_info']
                    
                    st.session_state.query_history.append(history_item)
                    
            except Exception as e:
                st.error(f"❌ Ошибка: {e}")
                
                # Сохраняем ошибку в историю
                st.session_state.query_history.append({
                    'query': user_query,
                    'sql': None,
                    'timestamp': datetime.now(),
                    'success': False,
                    'error': str(e),
                    'temperature': temperature,
                    'max_tokens': max_tokens,
                    'model': st.session_state.get('model_choice', 'Unknown')
                })

def render_query_history():
    """Рендер истории запросов"""
    if not st.session_state.query_history:
        return
    
    st.subheader("📚 История запросов")
    
    for i, item in enumerate(reversed(st.session_state.query_history[-10:])):  # Показываем последние 10
        # Определяем иконку статуса
        status_icon = "✅" if item['success'] else "❌"
        
        with st.expander(f"{status_icon} Запрос {len(st.session_state.query_history) - i}: {item['query'][:50]}..."):
            col1, col2 = st.columns([3, 1])
            
            with col1:
                st.write(f"**Вопрос:** {item['query']}")
                if item['sql']:
                    st.code(item['sql'], language='sql')
                if not item['success'] and 'error' in item:
                    st.error(f"Ошибка: {item['error']}")
                
                # Показываем дополнительную информацию если есть
                if item.get('execution_time'):
                    st.write(f"**Время выполнения:** {item['execution_time']:.3f}с")
                
                if item.get('temperature') is not None:
                    st.write(f"**Temperature:** {item['temperature']}")
                
                if item.get('max_tokens'):
                    st.write(f"**Max Tokens:** {item['max_tokens']}")
                
                if item.get('model'):
                    st.write(f"**Модель:** {item['model']}")
                
                if item.get('risk_level'):
                    risk_colors = {'low': '🟢', 'medium': '🟡', 'high': '🟠', 'critical': '🔴'}
                    risk_icon = risk_colors.get(item['risk_level'], '⚪')
                    st.write(f"**Уровень риска:** {risk_icon} {item['risk_level'].upper()}")
                
                if item.get('complexity_score') is not None:
                    st.write(f"**Сложность:** {item['complexity_score']}")
            
            with col2:
                st.write(f"**Время:** {item['timestamp'].strftime('%H:%M:%S')}")
                if item['success']:
                    st.success("✅ Успешно")
                    
                    # Показываем метрики качества
                    if item.get('sql_accuracy') is not None:
                        st.write(f"**Точность SQL:** {'✅' if item['sql_accuracy'] else '❌'}")
                    
                    if item.get('business_terms_used') is not None:
                        st.write(f"**Бизнес-термины:** {item['business_terms_used']}")
                    
                    if item.get('pii_detected') is not None:
                        st.write(f"**PII:** {'⚠️' if item['pii_detected'] else '✅'}")
                    
                    if item.get('join_count') is not None:
                        st.write(f"**JOIN'ов:** {item['join_count']}")
                    
                if item.get('subquery_count') is not None:
                    st.write(f"**Подзапросов:** {item['subquery_count']}")
                
                # Показываем информацию о попытках если есть
                if item.get('attempts_info'):
                    st.write("**Попытки генерации:**")
                    for attempt in item['attempts_info']:
                        if attempt['success']:
                            st.write(f"  ✅ Попытка {attempt['attempt']}: {attempt['generation_time']:.3f}с")
                        else:
                            st.write(f"  ❌ Попытка {attempt['attempt']}: {attempt.get('error_type', 'Unknown')}")
                
                if not item['success']:
                    st.error("❌ Ошибка")

def render_performance_metrics():
    """Рендер метрик производительности"""
    if not st.session_state.agent:
        st.info("🔌 Сначала подключитесь к базе данных")
        return
    
    st.subheader("📊 Метрики производительности")
    
    # Получаем метрики от агента
    try:
        metrics = st.session_state.agent.get_performance_metrics()
        
        if not metrics:
            st.info("📈 Пока нет данных о производительности. Выполните несколько запросов для сбора метрик.")
            return
        
        # Основные метрики
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Среднее время выполнения", 
                f"{metrics.get('avg_execution_time', 0):.3f}с",
                help="Среднее время генерации и выполнения SQL запросов"
            )
        
        with col2:
            st.metric(
                "Точность SQL", 
                f"{metrics.get('sql_accuracy_rate', 0)*100:.1f}%",
                help="Процент успешно выполненных SQL запросов"
            )
        
        with col3:
            st.metric(
                "Частота ошибок", 
                f"{metrics.get('error_rate', 0)*100:.1f}%",
                help="Процент запросов с ошибками"
            )
        
        with col4:
            st.metric(
                "Всего запросов", 
                metrics.get('total_queries', 0),
                help="Общее количество обработанных запросов"
            )
        
        st.divider()
        
        # Детальная статистика
        st.subheader("📈 Детальная статистика")
        
        # График времени выполнения
        if st.session_state.query_history:
            execution_times = []
            timestamps = []
            
            for item in st.session_state.query_history:
                if item.get('success') and 'execution_time' in item:
                    execution_times.append(item['execution_time'])
                    timestamps.append(item['timestamp'])
            
            if execution_times:
                import plotly.graph_objects as go
                
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=timestamps,
                    y=execution_times,
                    mode='lines+markers',
                    name='Время выполнения',
                    line=dict(color='#1f77b4')
                ))
                
                fig.update_layout(
                    title="Время выполнения запросов",
                    xaxis_title="Время",
                    yaxis_title="Секунды",
                    hovermode='x unified'
                )
                
                st.plotly_chart(fig, use_container_width=True)
        
        # Статистика по типам запросов
        if st.session_state.query_history:
            st.subheader("📋 Статистика по запросам")
            
            successful_queries = [q for q in st.session_state.query_history if q.get('success')]
            failed_queries = [q for q in st.session_state.query_history if not q.get('success')]
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**Успешные запросы:**")
                for i, query in enumerate(successful_queries[-5:], 1):  # Последние 5
                    with st.expander(f"✅ Запрос {i}: {query['query'][:50]}..."):
                        st.write(f"**Время:** {query['timestamp'].strftime('%H:%M:%S')}")
                        if 'execution_time' in query:
                            st.write(f"**Время выполнения:** {query['execution_time']:.3f}с")
                        if 'sql' in query and query['sql']:
                            st.code(query['sql'], language='sql')
            
            with col2:
                st.write("**Неудачные запросы:**")
                for i, query in enumerate(failed_queries[-5:], 1):  # Последние 5
                    with st.expander(f"❌ Запрос {i}: {query['query'][:50]}..."):
                        st.write(f"**Время:** {query['timestamp'].strftime('%H:%M:%S')}")
                        if 'error' in query:
                            st.error(f"**Ошибка:** {query['error']}")
        
        # Рекомендации по оптимизации
        st.subheader("💡 Рекомендации по оптимизации")
        
        recommendations = []
        
        if metrics.get('avg_execution_time', 0) > 2.0:
            recommendations.append("• Высокое время выполнения - рассмотрите уменьшение max_tokens")
        
        if metrics.get('error_rate', 0) > 0.2:
            recommendations.append("• Высокая частота ошибок - попробуйте увеличить temperature")
        
        if metrics.get('sql_accuracy_rate', 0) < 0.8:
            recommendations.append("• Низкая точность SQL - проверьте качество входных запросов")
        
        if not recommendations:
            recommendations.append("• Система работает оптимально! 🎉")
        
        for rec in recommendations:
            st.write(rec)
        
    except Exception as e:
        st.error(f"❌ Ошибка получения метрик: {e}")

def main():
    """Главная функция приложения"""
    init_session_state()
    
    # Заголовок
    st.markdown('<h1 class="main-header">🤖 BI-GPT Agent - Integrated</h1>', unsafe_allow_html=True)
    st.markdown("**Интегрированная система для работы с базами данных на естественном языке**")
    
    # Проверка доступности модулей
    if not MAIN_APP_AVAILABLE and not BACKEND_AVAILABLE:
        st.error("❌ Ни один из модулей приложения не доступен. Проверьте установку зависимостей.")
        return
    
    # Рендер интерфейса
    render_database_connection()
    
    if st.session_state.engine is None:
        st.info("🔌 Введите параметры подключения к базе данных в боковой панели и нажмите «Подключиться»")
        return
    
    # Основной контент
    tab1, tab2, tab3, tab4 = st.tabs(["🗄️ Схема БД", "💬 Естественный язык", "📚 История", "📊 Метрики"])
    
    with tab1:
        render_schema_overview()
    
    with tab2:
        render_natural_language_query()
    
    with tab3:
        render_query_history()
    
    with tab4:
        render_performance_metrics()
    
    # Футер
    st.divider()
    st.caption("BI-GPT Agent v1.0 - Интегрированная версия")

if __name__ == "__main__":
    main()
