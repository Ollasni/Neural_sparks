"""
Streamlit интерфейс для BI-GPT Agent
Демонстрирует максимальное качество UX и функциональность
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
import time

from bi_gpt_agent import BIGPTAgent

# Конфигурация страницы
st.set_page_config(
    page_title="BI-GPT Agent",
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
</style>
""", unsafe_allow_html=True)

# Инициализация агента
@st.cache_resource
def init_agent(api_key=None, base_url=None):
    """Инициализация BI-GPT агента"""
    return BIGPTAgent(api_key=api_key, base_url=base_url)

def display_metrics_dashboard(agent):
    """Отображает дашборд с метриками"""
    metrics = agent.get_performance_metrics()
    
    if not metrics:
        st.info("Пока нет данных о запросах")
        return
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Время выполнения", 
            f"{metrics.get('avg_execution_time', 0):.2f}s",
            delta=None
        )
    
    with col2:
        accuracy = metrics.get('sql_accuracy_rate', 0) * 100
        st.metric(
            "Точность SQL", 
            f"{accuracy:.1f}%",
            delta=None
        )
    
    with col3:
        error_rate = metrics.get('error_rate', 0) * 100
        st.metric(
            "Частота ошибок", 
            f"{error_rate:.1f}%",
            delta=None
        )
    
    with col4:
        st.metric(
            "Всего запросов", 
            int(metrics.get('total_queries', 0)),
            delta=None
        )

def create_result_visualization(df, query_type):
    """Создает визуализацию результатов"""
    if df.empty:
        return None
    
    # Определяем тип визуализации на основе данных
    numeric_cols = df.select_dtypes(include=['number']).columns
    
    if len(numeric_cols) == 0:
        return None
    
    # Если есть временные данные
    date_cols = [col for col in df.columns if 'date' in col.lower() or 'время' in col.lower()]
    
    if date_cols and len(numeric_cols) > 0:
        # Временной график
        fig = px.line(
            df, 
            x=date_cols[0], 
            y=numeric_cols[0],
            title=f"Динамика {numeric_cols[0]}",
            template="plotly_white"
        )
        return fig
    
    elif len(df) <= 20 and len(numeric_cols) >= 1:
        # Столбчатая диаграмма для небольших наборов
        if len(df.columns) >= 2:
            x_col = df.columns[0]
            y_col = numeric_cols[0]
            fig = px.bar(
                df, 
                x=x_col, 
                y=y_col,
                title=f"{y_col} по {x_col}",
                template="plotly_white"
            )
            return fig
    
    # Сводная таблица для больших наборов данных
    return None

def main():
    """Основная функция приложения"""
    
    # Заголовок
    st.title("BI-GPT Agent")
    st.subheader("Natural Language to SQL Converter")
    
    # Боковая панель
    with st.sidebar:
        st.header("Settings")
        
        # Выбор модели
        model_type = st.selectbox(
            "Select Model:",
            ["Local Llama-4-Scout", "OpenAI GPT-4"],
            index=0
        )
        
        if model_type == "Local Llama-4-Scout":
            base_url = st.text_input(
                "Model URL",
                value="https://bkwg3037dnb7aq-8000.proxy.runpod.net/v1"
            )
            api_key = st.text_input(
                "API Key",
                value="app-yzNqYV4e205Vui63kMQh1ckU",
                type="password"
            )
            st.session_state['base_url'] = base_url
            st.session_state['api_key'] = api_key
            st.success("Using Llama-4-Scout model")
        else:
            api_key = st.text_input(
                "OpenAI API Key", 
                type="password"
            )
            st.session_state['api_key'] = api_key
            st.session_state['base_url'] = None
            st.info("Using OpenAI GPT-4")
        
        st.markdown("---")
        
        # Примеры запросов
        st.header("Example Queries")
        example_queries = [
            "покажи всех клиентов",
            "прибыль за последние 2 дня",
            "средний чек клиентов",
            "остатки товаров на складе",
            "количество заказов",
            "топ 3 клиента по выручке",
            "средняя маржинальность по категориям",
            "заказы за сегодня",
            "клиенты премиум сегмента",
            "товары с низкими остатками"
        ]
        
        for query in example_queries:
            if st.button(query, key=f"example_{query}"):
                st.session_state['current_query'] = query
        
        st.markdown("---")
        st.header("Business Terms")
        st.markdown("""
        **Supported terms:**
        - Profit, revenue, margin
        - Average check, turnover  
        - Today, last week, last month
        - Orders, customers, products
        - Inventory, warehouse
        """)
    
    # Инициализация агента с настройками
    api_key = st.session_state.get('api_key', 'app-yzNqYV4e205Vui63kMQh1ckU')
    base_url = st.session_state.get('base_url', 'https://bkwg3037dnb7aq-8000.proxy.runpod.net/v1')
    agent = init_agent(api_key, base_url)
    
    # Основная область
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.header("Natural Language Query")
        
        # Поле ввода запроса
        user_query = st.text_area(
            "Enter your question:",
            value=st.session_state.get('current_query', ''),
            height=100,
            placeholder="Example: покажи прибыль за последние 2 дня"
        )
        
        # Кнопки действий
        col_btn1, col_btn2, col_btn3 = st.columns(3)
        
        with col_btn1:
            process_btn = st.button("Execute Query", type="primary")
        
        with col_btn2:
            if st.button("Clear"):
                st.session_state['current_query'] = ''
                st.rerun()
        
        with col_btn3:
            if st.button("Show DB Schema"):
                st.session_state['show_schema'] = True
    
    with col2:
        st.header("Performance Metrics")
        display_metrics_dashboard(agent)
    
    # Обработка запроса
    if process_btn and user_query.strip():
        with st.spinner("Processing query and generating SQL..."):
            start_time = time.time()
            result = agent.process_query(user_query)
            processing_time = time.time() - start_time
        
        # Отображение результатов
        if 'error' in result:
            st.error(f"Error: {result['error']}")
            if result.get('sql'):
                st.code(result['sql'], language='sql')
        else:
            st.success("Query executed successfully")
            
            # Вкладки для результатов
            tab1, tab2, tab3, tab4 = st.tabs(["Results", "SQL", "Visualization", "Analysis"])
            
            with tab1:
                st.subheader("Query Results")
                df = result['results']
                
                if df.empty:
                    st.info("No data returned")
                else:
                    st.dataframe(df, use_container_width=True)
                    
                    # Базовая статистика
                    if len(df) > 0:
                        col_stat1, col_stat2 = st.columns(2)
                        with col_stat1:
                            st.metric("Rows", len(df))
                        with col_stat2:
                            st.metric("Columns", len(df.columns))
            
            with tab2:
                st.subheader("Generated SQL")
                st.code(result['sql'], language='sql')
                
                # Информация о бизнес-терминах
                if result.get('business_terms'):
                    st.subheader("Business Terms Used")
                    for term in result['business_terms']:
                        st.text(f"- {term}")
            
            with tab3:
                st.subheader("Визуализация данных")
                if not result['results'].empty:
                    fig = create_result_visualization(result['results'], user_query)
                    if fig:
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("Автоматическая визуализация недоступна для данного типа данных")
                        
                        # Предложение ручной визуализации
                        numeric_cols = result['results'].select_dtypes(include=['number']).columns
                        if len(numeric_cols) > 0:
                            st.subheader("Ручная настройка графика")
                            chart_type = st.selectbox("Тип графика", ["Столбчатая диаграмма", "Круговая диаграмма", "Линейный график"])
                            
                            if chart_type == "Столбчатая диаграмма" and len(result['results']) <= 50:
                                x_axis = st.selectbox("Ось X", result['results'].columns)
                                y_axis = st.selectbox("Ось Y", numeric_cols)
                                
                                if st.button("Построить график"):
                                    fig = px.bar(result['results'], x=x_axis, y=y_axis)
                                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Нет данных для визуализации")
            
            with tab4:
                st.subheader("🧠 Интеллектуальный анализ")
                st.write(result.get('explanation', 'Анализ недоступен'))
                
                # Метрики запроса
                if result.get('metrics'):
                    metrics = result['metrics']
                    
                    metric_col1, metric_col2, metric_col3 = st.columns(3)
                    with metric_col1:
                        st.metric("Время выполнения", f"{metrics.execution_time:.2f}s")
                    with metric_col2:
                        st.metric("Бизнес-термины", metrics.business_terms_used)
                    with metric_col3:
                        st.metric("Точность", f"{metrics.aggregation_accuracy:.1%}")
    
    # Показ схемы БД
    if st.session_state.get('show_schema'):
        st.header("🗄️ Схема базы данных")
        
        schema_info = {
            "orders": ["id", "customer_id", "order_date", "amount", "status"],
            "customers": ["id", "name", "email", "registration_date", "segment"],
            "products": ["id", "name", "category", "price", "cost"],
            "sales": ["id", "order_id", "product_id", "quantity", "revenue", "costs"],
            "inventory": ["id", "product_id", "current_stock", "warehouse"]
        }
        
        for table, columns in schema_info.items():
            with st.expander(f"📋 Таблица: {table}"):
                st.write("**Поля:**")
                for col in columns:
                    st.write(f"• {col}")
        
        if st.button("Скрыть схему"):
            st.session_state['show_schema'] = False
            st.rerun()
    
    # Футер
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666;'>
        🤖 BI-GPT Agent |
        💼 Бизнес-ценность • 🔒 Безопасность • 🎯 Точность ML/AI
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
