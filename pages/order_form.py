"""
TODO
- [ ] represent items are classes
- [ ] 
"""

import polars as pl
import streamlit as st
from cleannest.models import Customer, Order
from cleannest.ingestion import item_list
from cleannest.database import CleannestDatabase
from cleannest.components.OrderForm import OrderForm

db = CleannestDatabase()
items = db.fetch_items()
clients = db.fetch_customers()

if "orders" not in st.session_state:
    st.session_state["orders"] = []

def process_order() -> None:    
    item_map = {item.name: item for item in item_list()}
    order = {}
    
    items = []
    if st.session_state.use_titan:
        if "Wash" in st.session_state.services:
            items.append(item_map["TITAN Wash"])
        if "Dry" in st.session_state.services:
            items.append(item_map["TITAN Dry"])
        if "Extra Dry"in st.session_state.extras:
            items.append(item_map["Extra TITAN Dry"])
    else:
        if "Wash" in st.session_state.services:
            items.append(item_map["Regular Wash"])
        if "Dry" in st.session_state.services:
            items.append(item_map["Regular Dry"])
        if "Extra Dry"in st.session_state.extras:
            items.append(item_map["Extra Regular Dry"])
        
    if "Fold" in st.session_state.services:
        items.append(item_map["Fold"])

    for _ in range(st.session_state.n_detergent):
        items.append(item_map["Ariel Detergent"])

    for _ in range(st.session_state.n_fabcon):
        items.append(item_map["Downey Fabcon"])

    for _ in range(st.session_state.n_bleach):
        items.append(item_map["Zonrox Colorsafe Bleach"])

    if "Hand Wash"in st.session_state.extras:
        items.append(item_map["Hand Wash"])

    order["items"] = items

    # Add customer class
    _customer = clients.filter(
        pl.col("customer_name") == st.session_state.customer_name
    ).to_dicts()[0]
    customer = Customer(**_customer)

    order["customer"] = customer

    st.session_state.orders.append(Order(**order))

def tabulate_orders(orders: list[Order]):
    data = [o.to_dict() for o in orders]
    df = pl.DataFrame(data)

    return st.data_editor(
        df,
        num_rows="dynamic",
        column_config={
            "customer": st.column_config.TextColumn(label="Customer"),
            "created_at": st.column_config.DatetimeColumn(label="Timestamp"),
            "items": st.column_config.ListColumn("Items"),
            "total": st.column_config.NumberColumn("Total")
        }
    )

OrderForm(
    clients['customer_name'].sort(),
    process_order
)

tabulate_orders(st.session_state.orders)