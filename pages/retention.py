from datetime import date

import numpy as np
import matplotlib.pyplot as plt
import altair as alt
import polars as pl
import streamlit as st

from cleannest.plotting import Stats
from cleannest.plotting import Charts

"# Customers"

# Churn analysis
subset = ["timestamp", "gross_sales", "customer_name"]
df = Stats.receipts.select(subset).with_columns(
    pl.col("timestamp").dt.truncate("1mo").alias("year_month")
)

## Get active customers per month
active_customers = (
    df.group_by("year_month")
    .agg(pl.col("customer_name").unique().alias("customers"))
    .sort("year_month")
)

## Convert to lists for set operations
months = active_customers["year_month"].to_list()
customers_list = active_customers["customers"].to_list()

## Compute churn metrics
records = []
for i in range(1, len(months)):
    prev_set = set(customers_list[i - 1])
    curr_set = set(customers_list[i])

    lost_customers = prev_set - curr_set
    churn_rate = len(lost_customers) / len(prev_set) if prev_set else 0

    records.append(
        {
            "month": months[i],
            "churn_rate": churn_rate,
            "lost_customers": len(lost_customers),
            "active_customers": len(curr_set),
        }
    )

churn_df = pl.DataFrame(records).with_columns(
    (1 - pl.col("churn_rate")).alias("retention_rate")
)

churn_base = alt.Chart(churn_df).encode(alt.X("month(month):T").title("Month"))

churn_chart = (
    alt.Chart(
        churn_df,
        title=alt.Title(
            "Customer Retention",
            fontSize=24,
        ),
    )
    .transform_fold(["churn_rate", "retention_rate"], as_=["label", "rate"])
    .mark_bar()
    .encode(
        x=alt.X("month(month):T").title("Month"),
        y=alt.Y("rate:Q").title("Rate (%)"),
        color=alt.Color(
            "label:N",
        ).title("Measurement"),
    )
)

with st.container(horizontal=True, horizontal_alignment="distribute"):
    st.metric(label="Unique Customers", value=Stats.total_customer_count(), border=True)

    st.metric(
        label="Returning Customers",
        value=Stats.total_returning_customer_count(),
        border=True,
    )

    global_retention_rate = (
        Stats.total_returning_customer_count() / Stats.total_customer_count()
    )
    global_churn_rate = 1 - global_retention_rate

    st.metric(
        label="Global Retention Rate", value=f"{global_retention_rate:.2%}", border=True
    )

    st.metric(label="Global Churn Rate", value=f"{global_churn_rate:.2%}", border=True)


with st.container(horizontal=True, horizontal_alignment="distribute"):
    with st.container(border=True, vertical_alignment="center"):
        customer_counts = (
            Stats.receipts.with_columns(pl.col("timestamp").dt.truncate("1mo"))
            .group_by("timestamp")
            .agg(pl.col("customer_name").unique().len().alias("unique_customers"))
            .sort("timestamp")
        )

        st.altair_chart(
            alt.Chart(
                customer_counts,
                title=alt.Title(
                    "Monthly Unique Customers",
                    fontSize=24,
                ),
            )
            .mark_bar()
            .encode(
                alt.X("month(timestamp):T").title("Month"),
                alt.Y("unique_customers").title("Unique Customers"),
            )
        )

    with st.container(border=True, vertical_alignment="center"):
        st.altair_chart(churn_chart)

"### Peak Hours"

st.altair_chart(
    Charts.peak_daily_hours(title="")
)

"### Cohort Analysis"

orders = Stats.receipts.with_columns(
    pl.col("timestamp").dt.truncate("1mo").alias("order_period")
)
cohort_df = orders.group_by("customer_name").agg(
    pl.col("timestamp").min().dt.truncate("1mo").alias("cohort_period")
)
cohort_df = orders.join(cohort_df, on="customer_name")

## Calculate cohort idnex (months since cohort start)
orders = cohort_df.with_columns(
    (
        (pl.col("order_period").dt.year() - pl.col("cohort_period").dt.year()) * 12
        + (pl.col("order_period").dt.month() - pl.col("cohort_period").dt.month())
    ).alias("cohort_index")
)

# Count unique customers by cohort_period and cohort_index
_cohort_counts = (
    orders
    .group_by(["cohort_period", "cohort_index"])
    .agg(pl.col("customer_name").n_unique().alias("n_customers"))
    .sort(["cohort_period", "cohort_index"])
    .fill_null(0)
)

cohort_counts = (
    _cohort_counts
    .pivot(
        values="n_customers",
        index="cohort_period",
        on="cohort_index",
    )
)

# Cohort sizes (first column, month 0)
cohort_sizes = cohort_counts.select(pl.col("0")).to_numpy().flatten()

# Convert counts to retention rates
retention_values = cohort_counts.select(pl.exclude("cohort_period")).to_numpy()
retention_rates = (retention_values.T / cohort_sizes).T

# Prepare labels for plotting
cohort_labels = (
    cohort_counts
    .select("cohort_period")
    .to_series()
    .dt.strftime("%b %Y")
    .to_list()
)
month_labels = [str(m) for m in range(retention_rates.shape[1])]


with st.container(horizontal=True, horizontal_alignment="distribute"):
    with st.container(border=True, vertical_alignment="center"):
        "### Retention Matrix"

        # st.altair_chart(
        #     # alt.Chart(_cohort_counts).transform_pivot(
        #     #     groupby="cohort_period",
        #     #     pivot="cohort_index",
        #     #     value="n_customers",
        #     # ).mark_rect().encode(

        #     # )

        #     alt.Chart(_cohort_counts).mark_rect().encode(
        #         alt.X("cohort_index:O").title("Cohort Index"),
        #         alt.Y(
        #             "cohort_period:T", 
        #             axis=alt.Axis(format="%b %Y"),
        #         ).title("Join Date"),
        #         color=alt.Color("n_customers:Q")
        #     )
        # )

        # st.dataframe(_cohort_counts)

        fig, ax = plt.subplots(figsize=(13, 9))
        cax = ax.imshow(retention_rates, aspect="auto", cmap="YlGnBu", vmin=0, vmax=1)
        ax.set_xticks(np.arange(len(month_labels)))
        ax.set_xticklabels(month_labels)
        ax.set_yticks(np.arange(len(cohort_labels)))
        ax.set_yticklabels(cohort_labels)

        plt.xlabel("Cohort Index", loc="center")
        plt.ylabel("Cohort")
        plt.colorbar(cax, label="Retention rate")
        plt.tight_layout(pad=1.5)
        st.pyplot(fig, use_container_width=True)

    with st.container(border=True, vertical_alignment="center"):
        "### Cohort Sizes"

        cohort_sizes_df = pl.DataFrame(
            {"cohort_labels": cohort_labels, "cohort_sizes": cohort_sizes}
        ).with_columns(
            pl.col("cohort_labels").str.to_date("%b %Y")
        )

        st.altair_chart(
            alt.Chart(cohort_sizes_df).mark_line().encode(
                alt.X("cohort_labels:T", 
                    title="Join Date",
                    axis=alt.Axis(
                        format="%b %Y",
                        labelAngle=-45,
                    )
                ),
                alt.Y("cohort_sizes:Q", title="Cohort Size")
            )
        )
