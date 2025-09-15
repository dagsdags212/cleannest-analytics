from enum import Enum
from dataclasses import dataclass
from pathlib import Path
import polars as pl
import duckdb
import streamlit as st

if "orders" not in st.session_state:
    st.session_state["orders"] = pl.DataFrame()


@st.cache_resource
def connect_db():
    db_path = Path(__file__).parent.parent / "cleannest" / "db" / "main.db"
    con = duckdb.connect(db_path)
    return con


class Item(Enum):
    BLEACH = 6
    DETERGENT = 12
    FABCON = 12


@dataclass
class Order:
    id: int
    # customer_id:
    total: float = 0
    items: list[Item] = []

    def __add__(self, item: Item):
        self.total += item.value
        self.items.append(item)


@dataclass
class Customer:
    id: str
    name: str
    phone: str | None = None
    email: str | None = None
    address: str | None = None

    def to_dict(self) -> dict:
        return dict(
            id=self.id,
            name=self.name,
            phone=self.phone,
            email=self.email,
            address=self.address,
        )

    def insert(self, df: pl.DataFrame) -> pl.DataFrame: ...


def submit_handler():
    c = Customer(
        id=st.session_state.customer_id,
        name=st.session_state.customer_name,
        phone=st.session_state.customer_phone,
        email=st.session_state.customer_email,
        address=st.session_state.customer_address,
    )
    st.write(c.to_dict())


def fetch_customer_info(db, name: str) -> dict:
    query = f"SELECT * FROM customers WHERE customer_name = '{name}' LIMIT 1"
    c = db.sql(query).pl()
    return Customer(
        id=c["customer_id"].item(),
        name=c["customer_name"].item(),
        phone=c["phone"].item(),
        email=c["email"].item(),
        address=c["address"].item(),
    )


db = connect_db()
customer_df = db.sql("FROM customers").pl()
customer_list = customer_df["customer_name"].unique().sort()

st.dataframe(customer_df)

st.header("Order Tracker")


with st.container(horizontal=True):
    with st.form("order_form"):
        customer = st.selectbox("Customer", customer_list, key="customer_name")

        sc = fetch_customer_info(db, customer)

        with st.container(horizontal=True):
            st.text_input(
                label="ID",
                value=sc.id,
                disabled=True,
                key="customer_id",
            )

            st.text_input(
                label="Phone",
                value=sc.phone,
                disabled=True,
                key="customer_phone",
            )

        st.text_input(
            label="Email",
            value=sc.email,
            disabled=True,
            key="customer_email",
        )

        st.text_input(
            label="Address",
            value=sc.address,
            disabled=True,
            key="customer_address",
        )

        st.radio(
            label="Status",
            # options=["pickup", "delivery", "pending_payment"],
            options=["Pick-up", "Delivery", "Pending Payment"],
            horizontal=True,
            key="status",
        )

        st.form_submit_button("Submit", on_click=submit_handler)


# st.write(st.session_state)
