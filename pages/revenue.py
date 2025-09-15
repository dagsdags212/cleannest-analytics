import streamlit as st

from cleannest.plotting import Charts, Stats
from cleannest.database import CleannestDatabase


# Load data
@st.cache_resource
def load_db():
    return CleannestDatabase()


db = load_db()
customers = db.fetch_customers()
receipts = db.fetch_receipts().sort("timestamp", descending=True)

# Set page configuration
st.set_page_config(
    layout="wide", page_title="Dashboard", initial_sidebar_state="expanded"
)

# === APP ===

st.write("# Business Revenue")

# st.dataframe(receipts)

_min_date = receipts["timestamp"].dt.date().min()
_max_date = receipts["timestamp"].dt.date().max()

c1, c2, c3 = st.columns([1, 2, 1])


with st.container(horizontal=True, horizontal_alignment="distribute", border=True):
    METRICS = ["Revenue", "Load Count"]
    selected_metric = st.pills(
        "Metric", METRICS, selection_mode="single", default="Revenue"
    )

    with st.container(
        horizontal=True, vertical_alignment="center", horizontal_alignment="right"
    ):
        date_filter = st.date_input(
            "Select date range",
            (_min_date, _max_date),
            width=500,
        )
        window_size = st.slider(
            label="Window Size",
            min_value=7,
            max_value=180,
            value=60,
            step=1,
            width=100,
        )

filtered_df = Charts.df(date_filter[0], date_filter[1])

with st.container(horizontal=True, horizontal_alignment="center"):
    if selected_metric == "Revenue":

        # Compute delta between today's revenue and mean revenue
        revenue_delta = (Stats.total_revenue_today() - Stats.daily_average_revenue()) / Stats.daily_average_revenue()

        st.metric(
            label="Today's Revenue (PHP)",
            value=f"{Stats.total_revenue_today():,.2f}",
            border=True,
            delta=f"{revenue_delta:.2%}",
        )

        st.metric(
            label="Mean Daily Revenue (PHP)",
            value=f"{Stats.daily_average_revenue():,.2f}",
            # delta="10",
            border=True,
        )

        st.metric(
            label="Total Revenue (PHP)",
            value=f"{Stats.total_revenue():,.2f}",
            # delta="10",
            border=True,
        )

    elif selected_metric == "Load Count":
        # Compute delta between today's load count and mean load count
        load_count_delta = (
            Stats.total_load_count_today() - Stats.daily_average_load_count()
        ) / Stats.daily_average_load_count()
        
        st.metric(
            label="Today's Load Count",
            value=f"{Stats.total_load_count_today()}",
            border=True,
            delta=f"{load_count_delta:.2%}"
        )

        st.metric(
            label="Mean Daily Load Count",
            value=f"{Stats.daily_average_load_count():,.2f}",
            # delta="10",
            border=True,
        )

        st.metric(
            label="Total Load Count",
            value=f"{Stats.total_load_count()}",
            # delta="10",
            border=True,
        )



with st.container(border=True, vertical_alignment="center"):
    subtitle = f"Data from {date_filter[0]} to {date_filter[1]}"
    match selected_metric:
        case "Revenue":
            st.altair_chart(
                Charts.daily_total_revenue(
                    filtered_df,
                    subtitle=subtitle,
                )
                + Charts.daily_rolling_revenue(filtered_df, window_size),
                use_container_width=True,
            )
        case "Load Count":
            st.altair_chart(
                Charts.daily_total_load_count(
                    filtered_df,
                    subtitle=subtitle,
                )
                + Charts.daily_rolling_load_count(filtered_df, window_size)
            )

st.dataframe(
    filtered_df,
    column_order=(
        "timestamp",
        "unique_customers",
        "full_loads",
        "total_gross",
        "receipts",
    ),
    column_config={
        "timestamp": st.column_config.DatetimeColumn(
            label="Date",
            format="MMMM D, YYYY",
        ),
        "unique_customers": st.column_config.NumberColumn(
            help="Number of unique customers that visited", label="Unique Customers"
        ),
        "full_loads": st.column_config.NumberColumn(
            help="Number of full loads",
            label="Load Count",
        ),
        "total_gross": st.column_config.NumberColumn(
            help="Gross revenue for the day",
            label="Total Revenue (PHP)",
            format="accounting",
        ),
        "receipts": st.column_config.ListColumn(
            help="List of transaction identifiers",
            label="Receipt IDs",
        ),
    },
)

with st.container(horizontal_alignment="right"):
    st.badge(
        label=f"Last updated on {_max_date}", icon=":material/database:", color="gray"
    )
