import polars as pl
import streamlit as st
from cleannest.database import CleannestDatabase


# Load data
@st.cache_resource
def load_db():
    return CleannestDatabase()

db = load_db()
customers_df = db.fetch_customers()
receipts_df = db.fetch_receipts()


"# Clients"

df = (
    receipts_df
    .group_by("customer_name")
    .agg(
        pl.col("gross_sales").sum().alias("total_spent"),
        pl.col("gross_sales").mean().alias("mean_transaction_value"),
        pl.col("discounts").sum().alias("total_discount"),
        pl.col("n_fold").sum().alias("total_loads"),
        pl.count().alias("n_transactions"),
        pl.col("timestamp").max().alias("last_visit"),
        pl.col("timestamp").min().alias("first_vist"),
        pl.col("timestamp").dt.strftime("%Y-%m-%d").unique().alias("visits"),
    )
    .with_columns(
        (pl.col("last_visit") - pl.col("first_vist")) \
            .dt.total_days().alias("tenure"),
        (pl.col("total_spent") - pl.col("total_discount")) \
            .alias("lifetime_value"),
        (10 - pl.col("total_loads") % 10) \
            .alias("loads_until_promo")
    )
    .join(
        customers_df,
        on="customer_name",
    )
    .select(
        "customer_id",
        "customer_name",
        "phone",
        "address",
        "total_loads",
        "n_transactions",
        "last_visit",
        "tenure",
        "total_spent",
        "total_discount",
        "lifetime_value",
        "visits",
        "loads_until_promo"
    )
    # .sort(
    #     pl.col("last_visit"),
    #     descending=True
    # )
)

event = st.dataframe(
   df,
   use_container_width=True,
   height=600,
   row_height=50,
   key="customers",
   column_config={
        "customer_id": st.column_config.TextColumn(
            label="ID",
            pinned=True,
        ),
        "customer_name": st.column_config.TextColumn(
            label="Name",
            pinned=True,
        ),
        "phone": st.column_config.TextColumn(
            label="Phone",
        ),
        "address": st.column_config.TextColumn(
            label="Address",
        ),
        "total_loads": st.column_config.NumberColumn(
            label="Total Loads",
        ),
        "n_transactions": st.column_config.NumberColumn(
            label="Transaction Count",
        ),
        "last_visit": st.column_config.DatetimeColumn(
            label="Last Visit",
        ),
        "tenure": st.column_config.NumberColumn(
            label="Tenure",
            help="Number of days as client since joining"
        ),
        "total_spent": st.column_config.NumberColumn(
            label="Total Spent",
        ),
        "total_discount": st.column_config.NumberColumn(
            label="Total Discount",
        ),
        "lifetime_value": st.column_config.NumberColumn(
            label="Lifetime Value",
            help="Total Spent - Total Discount"
        ),
        "visits": st.column_config.ListColumn(
            label="Visits",
        ),
        "loads_until_promo": st.column_config.ProgressColumn(
            label="Loads Until Promo",
            min_value=0,
            max_value=10,
            step=1,
            format="plain",
            pinned=True,
        ),
   }
)
