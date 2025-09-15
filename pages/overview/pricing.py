from dataclasses import dataclass
import polars as pl
import streamlit as st


@dataclass(frozen=True)
class Item:
    name: str
    variant: str | None
    price: float
    cost: float | None


pricing_data = [
    dict(item="detergent", variant="Ariel", price=18.0, cost=13.53), 
    dict(item="fabcon", variant="Downy", price=12.0, cost=8.77),
    dict(item="bleach", variant="", price=6.0, cost=0.0),
    dict(item="wash", variant="regular", price=65.0, cost=0.0),
    dict(item="wash", variant="hand wash", price=45.0, cost=0.0),
    dict(item="wash", variant="TITAN", price=80.0, cost=0.0),
    dict(item="dry", variant="regular", price=65.0, cost=0.0),
    dict(item="dry", variant="regular extra 10 mins.", price=17.0, cost=0.0),
    dict(item="dry", variant="TITAN", price=90.0, cost=0.0),
    dict(item="dry", variant="TITAN extra 10 mins.", price=23.0, cost=0.0),
    dict(item="fold", variant=None, price=35.0, cost=0.0),
]

prices_df=  pl.DataFrame(pricing_data)

"# Pricing"



with st.container(horizontal=True):
    with st.container(border=False, horizontal=False):
        "## Soaps"
        st.metric("Ariel Detergent", value=18.00, border=True)
        st.metric("Downey Fabcon", value=12.00, border=True)
        st.metric("Color Safe Bleach", value=6.00, border=True)

    with st.container(border=False, horizontal=False):
        "## Wash"
        st.metric("TITAN Wash", value=80.00, border=True)
        st.metric("Hand Wash", value=45.0, border=True)

    with st.container(border=False, horizontal=False):
        "## Dry"
        st.metric("Regular Dry", value=65.00, border=True)
        st.metric("TITAN Dry", value=90.00, border=True)

"## Extras"
with st.container(border=False, horizontal=True):
    st.metric("Fold", value=35.00, border=True)
    st.metric("Extra Dry (Regular, 10 mins.)", value=17.00, border=True)
    st.metric("Extra Dry (Regular, 10 mins.)", value=23.00, border=True)

