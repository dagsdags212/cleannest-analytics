from typing import Callable
import streamlit as st


def OrderForm(customer_selection: list, callback: Callable):
    SERVICE_LIST = ["Wash", "Dry", "Fold"]

    order_form = st.form("order_form")

    order_form.write("## Order Form")
    
    # Customer select
    order_form.selectbox(
        "Customer",
        options=customer_selection,
        key="customer_name"
    )

    # Service multi-select
    order_form.segmented_control(
        "Select services",
        options=SERVICE_LIST,
        selection_mode="multi",
        key="services",
    )

    # Extra service multi-select
    order_form.segmented_control(
        "Extras",
        options=["Extra Dry", "Hand Wash"],
        selection_mode="multi",
        key="extras",
    )

    order_form.toggle(label="Use TITAN", value=False, key="use_titan")

    # Soap multi-select
    with order_form.container(horizontal=True):
        st.number_input("Detergent", min_value=0, value=1, key="n_detergent")
        st.number_input("Fabcon", min_value=0, value=1, key="n_fabcon")
        st.number_input("Bleach", min_value=0, value=0, key="n_bleach")


    # Submit button
    order_form.form_submit_button("Submit", on_click=callback)

    return order_form
