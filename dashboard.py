import streamlit as st

st.set_page_config(
    layout="wide",
)

pages = {
    "Overview": [
        st.Page("pages/overview/summary.py", title="Summary"),
        st.Page("pages/overview/pricing.py", title="Pricing"),
        st.Page("pages/overview/receipts.py", title="Transactions"),
        st.Page("pages/overview/clients.py", title="Clients"),
    ],
    "Analytics": [
        st.Page("pages/revenue.py", title="Revenue"),
        st.Page("pages/retention.py", title="Retention"),
        st.Page("pages/expenses.py", title="Expenses"),
    ],
    "Orders": [
        st.Page("pages/order_form.py", title="Order Form"),
    ],
    "Database": [
        st.Page("pages/database.py", title="Database Explorer"),
    ],
}

pg = st.navigation(pages)
pg.run()
