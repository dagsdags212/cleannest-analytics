from cleannest.database import CleannestDatabase
import polars as pl
import streamlit as st


# Load data
@st.cache_resource
def load_db():
    return CleannestDatabase()

db = load_db()
receipts = db.fetch_receipts().sort("timestamp", descending=True)

receipts = (
    receipts
    .select(
        "timestamp",
        "receipt_id",
        "gross_sales",
        "discounts",
        "total_collected",
        "customer_name",
        "description",
        "payment_type",
        "cashier_name",
    )
)

"# Transactions"

with st.container(horizontal=True, border=True):
    customer_list = receipts['customer_name'].unique()
    selected_customers = st.multiselect("Filter by customer", options=customer_list, default=None)

    _start, _end = st.date_input(
        "Filter by date",
        (
            receipts['timestamp'].min(), 
            receipts['timestamp'].max()
        ),
    )

filters = []
if len(selected_customers) > 0:
    filters.append(
        pl.col("customer_name").is_in(selected_customers)
    )

if _start and _end:
    filters.append(pl.col("timestamp") >= _start)
    filters.append(pl.col("timestamp") <= _end)

st.dataframe(
    receipts.filter(*filters),
    use_container_width=True,
    height=600,
)