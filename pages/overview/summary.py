from pathlib import Path
from datetime import date
import duckdb
import polars as pl
import streamlit as st
import altair as alt


@st.cache_resource
def connect_db():
    db_path = Path(__file__).parent.parent / "cleannest" / "db" / "main.db"
    con = duckdb.connect(db_path)
    return con


con = connect_db()
customers_df = con.sql("FROM customers").pl()
receipts_df = con.sql("FROM receipts").pl()
expenses_df = con.sql("FROM expenses").pl()


def sort_by_month(df, dt_col, metric_col, metric="sum") -> pl.DataFrame:
    """Aggregate by month and compute a summary metric"""
    if metric == "sum":
        res = (
            df.with_columns(
                pl.col(dt_col).dt.truncate("1mo"),
            )
            .group_by(dt_col)
            .agg(
                pl.col(metric_col).sum(),
            )
            .sort(dt_col)
        )
    elif metric == "mean":
        res = (
            df.with_columns(pl.col(dt_col).dt.truncate("1mo"))
            .group_by(dt_col)
            .agg(
                pl.col(metric_col).mean(),
            )
            .sort(dt_col)
        )
    elif metric == "median":
        res = (
            df.with_columns(pl.col(dt_col).dt.truncate("1mo"))
            .group_by(dt_col)
            .agg(
                pl.col(metric_col).median(),
            )
            .sort(dt_col)
        )

    return res


# Compute revenue metrics
mean_monthly_revenue = sort_by_month(receipts_df, "timestamp", "gross_sales", "sum")
total_revenue = receipts_df["gross_sales"].sum()
daily_revenue = (
    receipts_df.with_columns(
        pl.col("timestamp").dt.truncate("1mo").alias("month"),
        pl.col("timestamp").dt.truncate("1d").alias("day"),
    )
    .group_by(["month", "day"])
    .agg(
        pl.col("gross_sales").sum().alias("daily_revenue"),
    )
    .group_by("month")
    .agg(pl.col("daily_revenue"))
    .sort("month")
)

# Compute expense metrics
mean_monthly_expense = sort_by_month(expenses_df, "date", "total_cost", "sum")
total_cost = expenses_df["total_cost"].sum()


"# Overview"

with st.container(border=True, horizontal=True, horizontal_alignment="center"):
    # Days since opening
    operating_days = (date.today() - date(2025, 1, 18)).days
    st.metric(
        label="Operating Days",
        value=operating_days,
    )

    # Number of unique clients
    st.metric(
        label="Unique Clients",
        value=customers_df.height,
    )

    # Total transactions
    st.metric(
        label="Total Transactions",
        value=f"{receipts_df.height:,}"
    )


"## Cash Flow"

df = (
    mean_monthly_revenue.with_columns(
        pl.col("timestamp").dt.date(),
    )
    .join(mean_monthly_expense, left_on="timestamp", right_on="date")
    .with_columns(
        total_cost_neg=pl.col("total_cost") * -1,
        net_gross_amt=pl.col("gross_sales") - pl.col("total_cost"),
        net_gross_pct=(pl.col("gross_sales") - pl.col("total_cost"))
        / pl.col("total_cost"),
    )
    .join(
        daily_revenue.with_columns(pl.col("month").dt.date()),
        left_on="timestamp",
        right_on="month",
    )
)

base = alt.Chart(df).encode(
    alt.X("yearmonth(timestamp):O").title("Date")
)

sales_chart = base.mark_bar(color="seagreen", opacity=0.9).encode(
    alt.Y("gross_sales").title("")
)
expense_chart = base.mark_bar(color="firebrick", opacity=0.9).encode(
    alt.Y("total_cost_neg").title("")
)
net_gross_chart = base.mark_point(size=100, color="gray", fill="white").encode(
    alt.Y("net_gross_amt").title("")
)

_revenue_expense_chart = alt.layer(
    sales_chart, 
    expense_chart,
    net_gross_chart,
).configure_scale(
    bandPaddingInner=0.15,
)

with st.container(horizontal=True, horizontal_alignment="center", width="stretch"):
    with st.container(width=300):
        st.metric(
            label="Total Revenue (PHP)",
            value=f"{total_revenue:,.2f}",
            border=True,
        )

        st.metric(
            label="Total Expense (PHP)",
            value=f"{total_cost:,.2f}",
            border=True,
        )

        # Compute total gross
        total_net_gross = total_revenue - total_cost
        st.metric(
            label="Total Gross (PHP)",
            value=f"{total_net_gross:,.2f}",
            border=True,
        )

    st.altair_chart(_revenue_expense_chart)

def color_values(val):
    if val > 0: 
        color = "seagreen"
    elif val < 0: 
        color = "firebrick"
    else: 
        color = "gray"
    
    return f"color: white; background-color: {color}"

df_styled = df.sort("timestamp", descending=True).to_pandas().style.applymap(color_values, subset=["net_gross_amt"])

"## Balance Sheet"
st.dataframe(
    df_styled,
    hide_index=True,
    column_order=[
        "timestamp",
        "gross_sales",
        "total_cost_neg",
        "net_gross_amt",
        "daily_revenue",
    ],
    column_config={
        "timestamp": st.column_config.DateColumn(
            "Date", format="MMMM YYYY", pinned=True
        ),
        "gross_sales": st.column_config.NumberColumn("Total Revenue"),
        "total_cost_neg": st.column_config.NumberColumn("Total Cost"),
        "net_gross_amt": st.column_config.NumberColumn("Net Gross"),
        "daily_revenue": st.column_config.AreaChartColumn("Daily Revenue"),
    },
)

# st.dataframe(df)
