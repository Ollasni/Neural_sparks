import streamlit as st
from typing import Optional, List, Dict, Any
import sys
import os
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

# Добавляем путь к модулю app
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from app.config.settings import UIText
from app.infrastructure.db.postgres import PostgresConfig, make_postgres_engine
from app.application.use_cases import GetSchemaOverviewUC
from app.application.sql_executor import SQLExecutor, QueryValidator

st.set_page_config(page_title="Streamlit", layout="wide")

# --------- Кастомный CSS для увеличенного шрифта ----------
# --------- Кастомный CSS для аккуратного увеличения ----------
st.markdown("""
    <style>
    /* Только значения в метриках */
    [data-testid="stMetricValue"] {
        font-size: 160% !important;
    }
    /* Только текст в таблицах */
    .stDataFrame div {
        font-size: 115% !important;
    }
    </style>
    """, unsafe_allow_html=True)

# --------- Sidebar: форма подключения ----------
st.sidebar.header("🔌 Подключение к БД")

# Переключатель между локальной и удаленной БД
db_type = st.sidebar.radio(
    "Тип подключения",
    ["Локальная БД", "Удаленная БД (Neon)"],
    index=0
)

if db_type == "Локальная БД":
    host = st.sidebar.text_input("Host", value="localhost")
    port = st.sidebar.number_input("Port", min_value=1, max_value=65535, value=5432, step=1)
    user = st.sidebar.text_input("Username", value="olgasnissarenko")
    password = st.sidebar.text_input("Password", type="password", value="")
    database = st.sidebar.text_input("Database Name", value="bi_demo")
    sslmode = st.sidebar.selectbox("SSL Mode", options=["disable", "require", "prefer"], index=0, help="Режим SSL")
else:
    st.sidebar.info("Настройки для Neon")
    host = st.sidebar.text_input("Host", value="ep-young-tree-agad2ram-pooler.c-2.eu-central-1.aws.neon.tech")
    port = st.sidebar.number_input("Port", min_value=1, max_value=65535, value=5432, step=1)
    user = st.sidebar.text_input("Username", value="neondb_owner")
    password = st.sidebar.text_input("Password", type="password", value="npg_TrW8nyL4CItx")
    database = st.sidebar.text_input("Database Name", value="neondb")
    sslmode = st.sidebar.selectbox("SSL Mode", options=["require", "prefer", "disable"], index=0, help="Режим SSL")

connect_clicked = st.sidebar.button("🔗 Подключиться", type="primary")

if "engine" not in st.session_state:
    st.session_state.engine = None
if "last_error" not in st.session_state:
    st.session_state.last_error = None
if "connection_info" not in st.session_state:
    st.session_state.connection_info = None

if connect_clicked:
    try:
        cfg = PostgresConfig(
            host=host, port=int(port), user=user, password=password,
            database=database, sslmode=sslmode
        )
        st.session_state.engine = make_postgres_engine(cfg)
        st.session_state.last_error = None
        st.session_state.connection_info = {
            "host": host,
            "port": port,
            "user": user,
            "database": database,
            "sslmode": sslmode
        }
        st.sidebar.success(UIText.connect_success)
    except Exception as e:
        st.session_state.engine = None
        st.session_state.last_error = str(e)
        st.sidebar.error(UIText.connect_fail)
        st.sidebar.code(str(e))

# --------- Основной экран ----------
st.title(UIText.title)

if st.session_state.engine is None:
    st.info("🔌 Введите параметры подключения слева и нажмите «Подключиться».")
else:
    # Показываем информацию о подключении
    if st.session_state.connection_info:
        info = st.session_state.connection_info
        st.success(f"✅ Подключено к {info['host']}:{info['port']}/{info['database']} как {info['user']}")
    
    # Создаем табы для разных функций
    tab1, tab2, tab3 = st.tabs(["📊 Схема БД", "🔍 SQL Запросы", "📈 Быстрые запросы"])
    
    with tab1:
        st.subheader(UIText.schema_title)
        uc = GetSchemaOverviewUC(st.session_state.engine, schema="public")
        try:
            overview = uc.execute()
            
            c1, c2, c3 = st.columns(3)
            c1.metric("Таблиц", overview.tables_count)
            c2.metric("Колонок", overview.columns_count)
            c3.metric("Тип БД", overview.db_type)

            st.divider()

            # Аккордеоны по таблицам
            for tbl in overview.tables:
                with st.expander(f"{tbl.name} ({len(tbl.columns)} колонок)"):
                    data = [{
                        "name": col.name,
                        "type": col.data_type,
                        "nullable": "✓" if col.is_nullable else "",
                        "default": col.default if col.default is not None else "None"
                    } for col in tbl.columns]
                    st.dataframe(data, use_container_width=True)
                    
        except Exception as e:
            st.error(f"Не удалось получить схему: {e}")
    
    with tab2:
        st.subheader("🔍 Выполнение SQL запросов")
        
        # SQL редактор
        sql_query = st.text_area(
            "Введите SQL запрос:",
            placeholder="SELECT * FROM table_name LIMIT 10;",
            height=150,
            help="Введите SQL запрос для выполнения. Будьте осторожны с изменяющими запросами!"
        )
        
        col1, col2 = st.columns([1, 4])
        with col1:
            execute_clicked = st.button("▶️ Выполнить", type="primary")
        with col2:
            limit = st.number_input("Лимит строк:", min_value=1, max_value=10000, value=1000, step=100)
        
        if execute_clicked and sql_query.strip():
            # Валидация запроса
            dangerous_ops = QueryValidator.contains_dangerous_operations(sql_query)
            if dangerous_ops:
                st.warning(f"⚠️ Запрос содержит потенциально опасные операции: {', '.join(dangerous_ops)}")
                
                # Показываем кнопку для подтверждения
                if st.button("🚨 Выполнить несмотря на предупреждение", type="secondary"):
                    executor = SQLExecutor(st.session_state.engine)
                    success, result, error = executor.execute_query(sql_query, limit)
                    
                    if success:
                        if isinstance(result, pd.DataFrame):
                            if len(result) > 0:
                                st.success(f"✅ Запрос выполнен успешно! Получено {len(result)} строк")
                                
                                # Показываем статистику
                                col1, col2, col3 = st.columns(3)
                                col1.metric("Строк", len(result))
                                col2.metric("Колонок", len(result.columns))
                                col3.metric("Размер", f"{result.memory_usage(deep=True).sum() / 1024:.1f} KB")
                                
                                # Показываем данные
                                st.dataframe(result, use_container_width=True)
                                
                                # Кнопки экспорта
                                col1, col2, col3 = st.columns(3)
                                with col1:
                                    csv = result.to_csv(index=False)
                                    st.download_button(
                                        "📥 Скачать CSV",
                                        csv,
                                        "query_result.csv",
                                        "text/csv"
                                    )
                                with col2:
                                    json = result.to_json(orient='records', indent=2)
                                    st.download_button(
                                        "📥 Скачать JSON",
                                        json,
                                        "query_result.json",
                                        "application/json"
                                    )
                            else:
                                st.info("Запрос выполнен, но данных не найдено")
                        else:
                            st.success(f"✅ {result}")
                    else:
                        st.error(f"❌ Ошибка: {error}")
            else:
                # Безопасный запрос - выполняем сразу
                executor = SQLExecutor(st.session_state.engine)
                success, result, error = executor.execute_query(sql_query, limit)
                
                if success:
                    if isinstance(result, pd.DataFrame):
                        if len(result) > 0:
                            st.success(f"✅ Запрос выполнен успешно! Получено {len(result)} строк")
                            
                            # Показываем статистику
                            col1, col2, col3 = st.columns(3)
                            col1.metric("Строк", len(result))
                            col2.metric("Колонок", len(result.columns))
                            col3.metric("Размер", f"{result.memory_usage(deep=True).sum() / 1024:.1f} KB")
                            
                            # Показываем данные
                            st.dataframe(result, use_container_width=True)
                            
                            # Кнопки экспорта
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                csv = result.to_csv(index=False)
                                st.download_button(
                                    "📥 Скачать CSV",
                                    csv,
                                    "query_result.csv",
                                    "text/csv"
                                )
                            with col2:
                                json = result.to_json(orient='records', indent=2)
                                st.download_button(
                                    "📥 Скачать JSON",
                                    json,
                                    "query_result.json",
                                    "application/json"
                                )
                        else:
                            st.info("Запрос выполнен, но данных не найдено")
                    else:
                        st.success(f"✅ {result}")
                        
                    # Добавляем в историю
                    if "query_history" not in st.session_state:
                        st.session_state.query_history = []
                    st.session_state.query_history.append((sql_query, pd.Timestamp.now()))
                else:
                    st.error(f"❌ Ошибка: {error}")
        
        # История запросов
        if "query_history" not in st.session_state:
            st.session_state.query_history = []
        
        if st.session_state.query_history:
            st.subheader("📜 История запросов")
            for i, (query, timestamp) in enumerate(reversed(st.session_state.query_history[-10:])):
                if st.button(f"📋 {query[:50]}...", key=f"hist_{i}"):
                    st.session_state.sql_query = query
                    st.rerun()
    
    with tab3:
        st.subheader("📈 Быстрые запросы")
        
        # Предустановленные запросы
        quick_queries = {
            "Показать все таблицы": "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';",
            "Размер базы данных": "SELECT pg_size_pretty(pg_database_size(current_database()));",
            "Активные подключения": "SELECT count(*) as active_connections FROM pg_stat_activity;",
            "Последние 10 записей": "SELECT * FROM {table_name} ORDER BY {id_column} DESC LIMIT 10;",
            "Статистика таблиц": """
                SELECT 
                    schemaname,
                    tablename,
                    attname,
                    n_distinct,
                    correlation
                FROM pg_stats 
                WHERE schemaname = 'public'
                LIMIT 20;
            """,
            "Индексы": """
                SELECT 
                    t.relname as table_name,
                    i.relname as index_name,
                    pg_size_pretty(pg_relation_size(i.oid)) as size
                FROM pg_class t
                JOIN pg_index ix ON t.oid = ix.indrelid
                JOIN pg_class i ON i.oid = ix.indexrelid
                WHERE t.relkind = 'r'
                ORDER BY pg_relation_size(i.oid) DESC
                LIMIT 10;
            """
        }
        
        selected_query = st.selectbox("Выберите быстрый запрос:", list(quick_queries.keys()))
        
        if st.button("🚀 Выполнить быстрый запрос"):
            query = quick_queries[selected_query]
            st.text_area("SQL запрос:", value=query, height=100, disabled=True)
            
            executor = SQLExecutor(st.session_state.engine)
            success, result, error = executor.execute_query(query)
            
            if success:
                if isinstance(result, pd.DataFrame):
                    st.success(f"✅ Запрос выполнен успешно! Получено {len(result)} строк")
                    st.dataframe(result, use_container_width=True)
                else:
                    st.success(f"✅ {result}")
            else:
                st.error(f"❌ Ошибка: {error}")

st.caption("💡 Используйте табы для работы со схемой БД, выполнения SQL запросов и быстрого анализа данных.")
