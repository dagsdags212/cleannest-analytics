from datetime import date
import polars as pl


def filter_by_date(
    df: pl.DataFrame, date_col: str, start: date | None = None, end: date | None = None
):
    """Subset a dataframe using the date column

    Parameters
    ----------
    df : pl.DataFrame
        the dataframe to subset
    date_col : str
        name of the column pertaining to the date
    start : Optional[date]
        earliest date to include
    end : Optional[date]
        latest date to include, exclusive
    """
    return df.filter(
        pl.col(date_col) >= start,
        pl.col(date_col) < end,
    )
