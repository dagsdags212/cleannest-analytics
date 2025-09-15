import streamlit as st

st.set_page_config(
    layout="wide",
)

pages = {
    "Summary": [
        st.Page("pages/overview.py", title="Overview")
    ],
    "Dashboards": [
        st.Page("pages/revenue.py", title="Revenue"),
        st.Page("pages/customers.py", title="Customers"),
        st.Page("pages/expenses.py", title="Expenses"),
    ],
    "Order Tracking": [
        st.Page("pages/orders.py", title="Order Tracking"),
    ],
    "Database": [
        st.Page("pages/database.py", title="Database Explorer"),
    ],
}

pg = st.navigation(pages)
pg.run()
