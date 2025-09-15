from pathlib import Path
import streamlit as st
import duckdb


@st.cache_resource
def connect_db():
    db_path = Path(__file__).parent.parent / "cleannest" / "db" / "main.db"
    con = duckdb.connect(db_path)
    return con


def retrieve_data(con, table_name):
    return con.sql(f"SELECT * FROM {table_name}").pl()


"# Database Explorer"

# Connect to database
con = connect_db()

# Extract table names fromm database
tables = [res[0] for res in con.sql("SHOW TABLES").fetchall()]

with st.container(horizontal=False, horizontal_alignment="center"):
    table_select = st.selectbox(label="Select table", options=tables, index=0)
    df = retrieve_data(con, table_select)
    st.dataframe(df)
