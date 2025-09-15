import datetime
from datetime import date
import polars as pl
import pandas as pd
import altair as alt
import streamlit as st
from streamlit_gsheets import GSheetsConnection

# Setup gsheets connection
con = st.connection("gsheets", type=GSheetsConnection)


@st.cache_data
def load_all_expenses():
    sheets = {
        "capital_expenses": 108,
        "Jan2025": 4,
        "Feb2025": 25,
        "Mar2025": 27,
        "Apr2025": 41,
        "May2025": 30,
        "June2025": 33,
        "July2025": 41,
        "Aug2025": 27,
    }

    _dfs = []
    for worksheet, nrows in sheets.items():
        df = con.read(
            worksheet=worksheet,
            ttl="5m",
            header=0,
            usecols=list(range(8)),
            nrows=nrows,
            dtype=object,
            parse_dates=True,
        )
        colnames = [
            "date",
            "item",
            "note",
            "category",
            "subcategory",
            "quantity",
            "unit",
            "total_cost",
        ]
        df.columns = colnames

        _dfs.append(df)

    return pd.concat(_dfs)


"# Expenses"

df = load_all_expenses()
df["date"] = pd.to_datetime(df["date"])
df["quantity"] = df["quantity"].fillna(1).astype(int)
df["total_cost"] = df["total_cost"].str.replace(",", "").fillna(0.0).astype(float)
df = pl.from_pandas(df)


# Total cost by month since opening
month_selection = alt.selection_point()
total_monthly_costs_chart = (
    alt.Chart(df)
    .transform_filter(alt.FieldGTEPredicate(field="date", gte=date(2025, 1, 1)))
    .mark_bar()
    .encode(
        alt.Y("yearmonth(date):O", sort="-y").title("Month-Year"),
        alt.X("sum(total_cost):Q").title("Total Cost (PHP)"),
        alt.Color("category", legend=None),
        opacity=alt.condition(month_selection, alt.value(1), alt.value(0.2)),
    )
    .add_params(month_selection)
    .properties(height=400)
    .interactive()
)

total_cost_by_category = (
    alt.Chart(df)
    .transform_aggregate(
        total_cost="sum(total_cost)",
        groupby=["category"],
    )
    .mark_arc(innerRadius=50)
    .encode(
        alt.Theta("total_cost:Q"),
        alt.Color("category"),
    )
)

with st.container(
    horizontal=True,
    border=True,
    horizontal_alignment="distribute",
):
    st.metric(
        label="Total Cost (PHP)",
        value=f"{df['total_cost'].sum():,.2f}",
    )

    # Compute total cost from latest month
    latest_month = df["date"].dt.truncate("1mo").max()
    latest_month_total_cost = df.filter(pl.col("date") >= latest_month)[
        "total_cost"
    ].sum()
    st.metric(
        label=f"Total Cost ({latest_month.strftime('%B')})",
        value=f"{latest_month_total_cost:,.2f}",
    )

with st.container(
    horizontal=True,
    border=True,
    horizontal_alignment="center",
    vertical_alignment="center",
    gap="medium",
):
    st.altair_chart(total_monthly_costs_chart)

    with st.container(width=500):
        st.altair_chart(total_cost_by_category)

st.dataframe(df)
