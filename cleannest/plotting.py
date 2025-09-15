from datetime import date, timedelta
from typing import Optional

from streamlit_echarts import st_echarts
import altair as alt
import polars as pl

from .database import CleannestDatabase
from .palettes import Default

DATABASE = CleannestDatabase()


class Stats:
    customers = DATABASE.fetch_customers()
    receipts = DATABASE.fetch_receipts()

    @classmethod
    def total_customer_count(cls):
        return cls.customers["customer_name"].unique().len()

    @classmethod
    def total_returning_customer_count(cls):
        df = (
            cls.receipts.filter(pl.col("gross_sales") > 100)
            .group_by("customer_name")
            .agg(pl.col("receipt_id").len().alias("n_transactions"))
            .filter(pl.col("n_transactions") > 1)
        )
        return df.height

    @classmethod
    def total_revenue(cls):
        return cls.receipts["gross_sales"].sum()

    @classmethod
    def daily_average_revenue(cls):
        return (
            cls.receipts.group_by(pl.col("timestamp").dt.date())
            .agg(pl.col("gross_sales").sum())["gross_sales"]
            .mean()
        )

    @classmethod
    def total_revenue_today(cls):
        return cls.receipts.filter(
            pl.col("timestamp") >= pl.col("timestamp").dt.date().max()
        )["gross_sales"].sum()

    @classmethod
    def total_load_count(cls):
        return cls.receipts["n_fold"].sum()

    @classmethod
    def daily_average_load_count(cls):
        return (
            cls.receipts.group_by(pl.col("timestamp").dt.date())
            .agg(pl.col("n_fold").sum())["n_fold"]
            .mean()
        )

    @classmethod
    def total_load_count_today(cls):
        return cls.receipts.filter(
            pl.col("timestamp") >= pl.col("timestamp").dt.date().max()
        )["n_fold"].sum()


class Charts:
    customers = DATABASE.fetch_customers()
    receipts = DATABASE.fetch_receipts()

    @classmethod
    def df(
        cls,
        start: Optional[date] = date(2025, 1, 18),
        end: Optional[date] = date.today() + timedelta(days=1),
    ):
        return (
            cls.receipts.filter(
                pl.col("timestamp") >= start,
                pl.col("timestamp") < end,
            )
            .group_by(
                pl.col("timestamp").dt.date(),
            )
            .agg(
                total_gross=pl.col("gross_sales").sum(),
                unique_customers=pl.col("customer_name").unique().len(),
                customers=pl.col("customer_name").unique(),
                full_loads=pl.col("is_full_load").sum(),
                titan_runs=pl.col("is_titan").sum(),
                hours_with_customer=pl.col("timestamp").max()
                - pl.col("timestamp").min(),
                n_detergent=pl.col("n_detergent").sum(),
                n_fabcon=pl.col("n_fabcon").sum(),
                n_bleach=pl.col("n_bleach").sum(),
                visits=pl.col("timestamp"),
                receipts=pl.col("receipt_id"),
            )
            .with_columns(
                titan_usage=(pl.col("titan_runs") / pl.col("full_loads")).round(3)
            )
            .sort(
                "timestamp",
                descending=True,
            )
        )

    @classmethod
    def daily_total_revenue(
        cls,
        df: pl.DataFrame,
        subtitle: str = "",
        fontSize: int = 20,
    ):
        return (
            alt.Chart(df)
            .mark_bar(
                color=Default.BLUE,
            )
            .encode(
                alt.X("timestamp:T").title("Date"),
                alt.Y("total_gross:Q").title("Total Gross"),
            )
            .properties(
                title=alt.Title(
                    "Total Daily Revenue",
                    subtitle=subtitle,
                    fontSize=fontSize,
                )
            )
        )

    @classmethod
    def daily_rolling_revenue(
        cls,
        df: pl.DataFrame,
        window_size: int = 14,
    ):
        return (
            alt.Chart(df)
            .mark_line(
                color=Default.ORANGE,
                size=2,
            )
            .transform_window(
                rolling_mean="mean(total_gross)",
                frame=[-window_size // 2, window_size // 2],
            )
            .encode(alt.X("timestamp:T"), alt.Y("rolling_mean:Q"))
        )

    @classmethod
    def daily_total_load_count(
        cls,
        df: pl.DataFrame,
        subtitle: str = "",
        fontSize: int = 20,
    ):
        return (
            alt.Chart(df)
            .mark_bar(
                color=Default.BLUE,
            )
            .encode(
                alt.X("timestamp:T").title("Date"),
                alt.Y("full_loads:Q").title("Load Count"),
            )
            .properties(
                title=alt.Title(
                    "Total Daily Load Count",
                    subtitle=subtitle,
                    fontSize=fontSize,
                )
            )
        )

    @classmethod
    def daily_rolling_load_count(
        cls,
        df: pl.DataFrame,
        window_size: int = 14,
    ):
        return (
            alt.Chart(df)
            .mark_line(
                color=Default.ORANGE,
                size=2,
            )
            .transform_window(
                rolling_mean="mean(full_loads)",
                frame=[-window_size // 2, window_size // 2],
            )
            .encode(alt.X("timestamp:T"), alt.Y("rolling_mean:Q"))
        )

    @classmethod
    def peak_daily_hours(
        cls, 
        title: str = "Peak Hours",
    ) -> alt.Chart:
        data = (
            cls.receipts
            .with_columns(
                pl.col("timestamp").dt.weekday().alias("weekday"),
                pl.col("timestamp").dt.hour().alias("hour"),
            )
            .group_by(["weekday", "hour"])
            .agg(
                pl.col("timestamp").first().dt.truncate("1h"),
                pl.col("n_fold").sum().alias("load_count")
            )
            .sort(["weekday", "hour"])
        )

        return (
            alt.Chart(data, title=title).mark_rect().encode(
                alt.X("hour:O")
                    .title("Hour")
                    .axis(labelAngle=0)
                ,
                alt.Y("weekday:O").title("Weekday"),
                alt.Color("load_count:Q")
                    .title(None)
                    .scale(scheme="oranges"),
                tooltip=[
                    alt.Tooltip("timestamp").title("Time"),
                    alt.Tooltip("load_count").title("Load Count")
                ]
            ).properties(
                height=300,
            )
        )

    @classmethod
    def daily_revenue_heatmap(cls, height: int=200):
        _data = (
            Stats.receipts
            .with_columns(
                pl.col("timestamp").dt.date(),
            )
            .group_by("timestamp")
            .agg(
                pl.col("gross_sales").sum(),
            )
        )

        data = [
            (row[0].strftime("%Y-%m-%d"), row[1])
            for row in _data.iter_rows()
        ]

        options = {
            "tooltip": {"position": "top"},
            "visualMap": {
                "min": _data["gross_sales"].min(),
                "max": _data["gross_sales"].max(),
                "type": "piecewise",
                "calculable": True,
                "orient": "horizontal",
                "left": "center",
                "top": "top",
            },
            "calendar": {
                "range": "2025", 
                "cellSize": ["auto", 15],
                "left": 30,
                "right": 10,
                "itemStyle": {
                    "borderWidth": 1,
                }
            },
            "series": [
                {
                    "type": "heatmap",
                    "coordinateSystem": "calendar",
                    "calendarIndex": 0,
                    "data": data,
                }
            ]
        }

        return st_echarts(options, height=f"{height}px", key="echarts")

    @classmethod
    def daily_load_count_heatmap(cls, height: int=200):
        _data = (
            Stats.receipts
            .with_columns(
                pl.col("timestamp").dt.date(),
            )
            .group_by("timestamp")
            .agg(
                pl.col("n_fold").sum().alias("load_count"),
            )
        )

        data = [
            (row[0].strftime("%Y-%m-%d"), row[1])
            for row in _data.iter_rows()
        ]

        options = {
            "tooltip": {"position": "top"},
            "visualMap": {
                "min": _data["load_count"].min(),
                "max": _data["load_count"].max(),
                "type": "piecewise",
                "calculable": True,
                "orient": "horizontal",
                "left": "center",
                "top": "top",
            },
            "calendar": {
                "range": "2025", 
                "cellSize": ["auto", 15],
                "left": 30,
                "right": 10,
                "itemStyle": {
                    "borderWidth": 1,
                }
            },
            "series": [
                {
                    "type": "heatmap",
                    "coordinateSystem": "calendar",
                    "calendarIndex": 0,
                    "data": data,
                }
            ]
        }

        return st_echarts(options, height=f"{height}px", key="echarts")