import streamlit as st
from typing import Optional
from app.config.settings import UIText
from app.infrastructure.db.postgres import PostgresConfig, make_postgres_engine
from app.application.use_cases import GetSchemaOverviewUC

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
st.sidebar.header("Данные подключения")
host = st.sidebar.text_input("Host", value="localhost")
port = st.sidebar.number_input("Port", min_value=1, max_value=65535, value=5432, step=1)
user = st.sidebar.text_input("Username", value="postgres")
password = st.sidebar.text_input("Password", type="password", value="")
database = st.sidebar.text_input("Database Name", value="postgres")
sslmode = st.sidebar.selectbox("SSL Mode", options=["", "require", "disable"], index=0, help="Опционально")

connect_clicked = st.sidebar.button("Подключиться")

if "engine" not in st.session_state:
    st.session_state.engine = None
if "last_error" not in st.session_state:
    st.session_state.last_error = None

if connect_clicked:
    try:
        cfg = PostgresConfig(
            host=host, port=int(port), user=user, password=password,
            database=database, sslmode=(sslmode or None)
        )
        st.session_state.engine = make_postgres_engine(cfg)
        st.session_state.last_error = None
        st.sidebar.success(UIText.connect_success)
    except Exception as e:
        st.session_state.engine = None
        st.session_state.last_error = str(e)
        st.sidebar.error(UIText.connect_fail)
        st.sidebar.code(str(e))

# --------- Основной экран ----------
st.title(UIText.title)

if st.session_state.engine is None:
    st.info("Введите параметры слева и нажмите «Подключиться».")
else:
    uc = GetSchemaOverviewUC(st.session_state.engine, schema="public")
    try:
        overview = uc.execute()
    except Exception as e:
        st.error(f"Не удалось получить схему: {e}")
        st.stop()

    st.subheader(UIText.schema_title)
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

    st.caption("Готово. Можно расширять: фильтры по схемам, поиск по колонкам, экспорт в CSV, NATURAL-LANGUAGE SQL и т.д.")
