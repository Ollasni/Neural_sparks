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
import os

# Загружаем переменные окружения из .env файла
from dotenv import load_dotenv
load_dotenv()

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
    # Проверяем, нужно ли использовать fine-tuned модель
    use_finetuned = os.getenv("USE_FINETUNED_MODEL", "false").lower() == "true"
    
    if use_finetuned:
        st.success("🎯 Используется fine-tuned модель Phi-3 + LoRA")
        return BIGPTAgent(use_finetuned=True)
    else:
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

def display_risk_analysis(risk_analysis):
    """Отображает анализ риска SQL запроса"""
    if not risk_analysis:
        return
    
    from advanced_sql_validator import RiskLevel
    
    # Получаем иконку и цвет для уровня риска
    risk_icon = "❓"
    risk_color = "#6c757d"
    risk_text = "Неизвестно"
    
    if hasattr(risk_analysis, 'risk_level'):
        risk_level = risk_analysis.risk_level
        if risk_level == RiskLevel.LOW:
            risk_icon = "✅"
            risk_color = "#28a745"
            risk_text = "Низкий риск"
        elif risk_level == RiskLevel.MEDIUM:
            risk_icon = "⚠️"
            risk_color = "#ffc107"
            risk_text = "Средний риск"
        elif risk_level == RiskLevel.HIGH:
            risk_icon = "🔶"
            risk_color = "#fd7e14"
            risk_text = "Высокий риск"
        elif risk_level == RiskLevel.CRITICAL:
            risk_icon = "🚨"
            risk_color = "#dc3545"
            risk_text = "Критический риск"
    
    # Отображаем уровень риска
    st.markdown(f"""
    <div style="background-color: {risk_color}20; border-left: 4px solid {risk_color}; padding: 10px; margin: 10px 0; border-radius: 4px;">
        <h4 style="margin: 0; color: {risk_color};">
            {risk_icon} Уровень риска: {risk_text}
        </h4>
    </div>
    """, unsafe_allow_html=True)
    
    # Показываем детали анализа
    if hasattr(risk_analysis, 'warnings') and risk_analysis.warnings:
        st.warning("⚠️ Предупреждения:")
        for warning in risk_analysis.warnings[:3]:  # Показываем первые 3
            st.write(f"• {warning}")
    
    if hasattr(risk_analysis, 'errors') and risk_analysis.errors:
        st.error("❌ Ошибки:")
        for error in risk_analysis.errors[:3]:  # Показываем первые 3
            st.write(f"• {error}")
    
    # Показываем метрики сложности
    if hasattr(risk_analysis, 'complexity_score'):
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Сложность", risk_analysis.complexity_score)
        with col2:
            st.metric("JOIN'ов", getattr(risk_analysis, 'join_count', 0))
        with col3:
            st.metric("Подзапросов", getattr(risk_analysis, 'subquery_count', 0))
    
    # Показываем рекомендации
    if hasattr(risk_analysis, 'recommendations') and risk_analysis.recommendations:
        st.info("💡 Рекомендации:")
        for rec in risk_analysis.recommendations[:3]:  # Показываем первые 3
            st.write(f"• {rec}")


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
            ["Llama 4 API (RunPod)", "Local Fine-tuned (Phi-3)"],
            index=0
        )
        
        # Настройки модели
        st.subheader("Model Parameters")
        temperature = st.slider(
            "Temperature", 
            min_value=0.0, 
            max_value=2.0, 
            value=0.0, 
            step=0.1,
            help="Контролирует случайность генерации. 0.0 = детерминированно, 2.0 = очень случайно"
        )
        
        max_tokens = st.slider(
            "Max Tokens", 
            min_value=50, 
            max_value=1000, 
            value=400, 
            step=50,
            help="Максимальное количество токенов в ответе"
        )
        
        # Сохраняем параметры в session state
        st.session_state['temperature'] = temperature
        st.session_state['max_tokens'] = max_tokens
        
        # Быстрые настройки
        st.subheader("Quick Settings")
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("🎯 Precise (0.0, 200)", help="Детерминированная генерация"):
                st.session_state['temperature'] = 0.0
                st.session_state['max_tokens'] = 200
                st.rerun()
            
            if st.button("⚖️ Balanced (0.3, 400)", help="Сбалансированная генерация"):
                st.session_state['temperature'] = 0.3
                st.session_state['max_tokens'] = 400
                st.rerun()
        
        with col2:
            if st.button("🎨 Creative (0.7, 600)", help="Креативная генерация"):
                st.session_state['temperature'] = 0.7
                st.session_state['max_tokens'] = 600
                st.rerun()
            
            if st.button("🚀 Complex (0.1, 800)", help="Для сложных запросов"):
                st.session_state['temperature'] = 0.1
                st.session_state['max_tokens'] = 800
                st.rerun()
        
        # Настройки загружаются из .env файла
        env_url = os.getenv("LOCAL_BASE_URL")
        env_key = os.getenv("LOCAL_API_KEY")
        
        if not env_url or not env_key:
            st.error("⚠️ Настройки не найдены в .env файле!")
            st.write("Создайте .env файл с настройками:")
            st.code("""LOCAL_API_KEY=your_api_key
LOCAL_BASE_URL=your_api_url""")
            st.stop()
        
        # Показываем текущие настройки из .env
        if model_type == "Llama 4 API (RunPod)":
            st.info(f"🦙 Llama 4 API: {env_url}")
            st.success("Настройки загружены из .env файла")
        else:
            st.info(f"🤖 Fine-tuned Model: {env_url}")
            st.success("Настройки загружены из .env файла")
        
        # Сохраняем настройки из .env
        st.session_state['base_url'] = env_url
        st.session_state['api_key'] = env_key
        
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
    # Получаем настройки из UI или переменных окружения
    api_key = st.session_state.get('api_key') or os.getenv('LOCAL_API_KEY')
    base_url = st.session_state.get('base_url') or os.getenv('LOCAL_BASE_URL')
    
    if not api_key or not base_url:
        st.error("⚠️ API настройки не найдены! Пожалуйста:")
        st.write("1. Заполните поля в боковой панели, ИЛИ")
        st.write("2. Установите переменные окружения LOCAL_API_KEY и LOCAL_BASE_URL, ИЛИ")
        st.write("3. Создайте .env файл из env.example")
        st.stop()
    
    try:
        agent = init_agent(api_key, base_url)
    except Exception as e:
        st.error(f"❌ Ошибка инициализации агента: {e}")
        st.stop()
    
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
            # Получаем параметры модели из session state
            temperature = st.session_state.get('temperature', 0.0)
            max_tokens = st.session_state.get('max_tokens', 400)
            result = agent.process_query(user_query, temperature=temperature, max_tokens=max_tokens)
            processing_time = time.time() - start_time
        
        # Отображение результатов
        if 'error' in result:
            st.error(f"Error: {result['error']}")
            if result.get('sql'):
                st.code(result['sql'], language='sql')
            # Показываем анализ риска даже для ошибок
            if result.get('risk_analysis'):
                display_risk_analysis(result['risk_analysis'])
        else:
            st.success("Query executed successfully")
            
            # Отображаем анализ риска перед вкладками
            if result.get('risk_analysis'):
                display_risk_analysis(result['risk_analysis'])
            
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
                
                # Показываем параметры модели
                col_param1, col_param2 = st.columns(2)
                with col_param1:
                    st.metric("Temperature", f"{temperature:.1f}")
                with col_param2:
                    st.metric("Max Tokens", max_tokens)
                
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
