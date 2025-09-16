from pathlib import Path

import polars as pl
import duckdb

from cleannest.models import Item


def _load_customers_from_csv(fp: Path) -> pl.DataFrame:
    _colnames = [
        "customer_id",
        "customer_name",
        "email",
        "phone",
        "address",
        "city",
        "province",
        "postal_code",
        "country",
        "customer_code",
        "points_balance",
        "note",
        "first_visit",
        "last_visit",
        "total_visits",
        "total_spent",
    ]

    customers_df = pl.read_csv(fp, has_header=True)
    customers_df.columns = _colnames
        
    return customers_df


def load_customer_data() -> pl.DataFrame:
    """Load customer data from a CSV file

    Columns
    -------
    customer_id : str
        a unique identifier for each customer
    customer_name : str
        full name of the customer
    email : str
        active email address of the customer
    phone : str
        contact number of customer
    address : str
        delivery address of the customer
    """
    customer_data_dir = Path("data/customers")

    _colnames = [
        "customer_id",
        "customer_name",
        "email",
        "phone",
        "address",
        "city",
        "province",
        "postal_code",
        "country",
        "customer_code",
        "points_balance",
        "note",
        "first_visit",
        "last_visit",
        "total_visits",
        "total_spent",
    ]

    customers_dfs = []
    for csv in customer_data_dir.glob("*.csv"):
        customers_dfs.append(
            _load_customers_from_csv(csv)
        )

    customers_df = pl.concat(customers_dfs)
    subset = ["customer_id", "customer_name", "email", "phone", "address"]

    # Set column datatypes
    customers_df = customers_df.select(subset).with_columns(
        pl.col("customer_id").cast(pl.String),
        pl.col("phone").cast(pl.String),
        # # Standardize input formats
        pl.col("customer_name").str.to_titlecase(),
    ).unique(subset=pl.col("customer_id"))

    return customers_df


def _load_receipts_from_csv(fp: Path) -> pl.DataFrame:
    _colnames = [
        "Date",
        "Receipt number",
        "Receipt type",
        "Gross sales",
        "Discounts",
        "Total collected",
        "Payment type",
        "Description",
        "Cashier name",
        "Customer name",
        "Status",
    ]
    df = pl.read_csv(fp, has_header=True, columns=_colnames)
    df.columns = [
        "timestamp",
        "receipt_id",
        "receipt_type",
        "gross_sales",
        "discounts",
        "total_collected",
        "payment_type",
        "description",
        "cashier_name",
        "customer_name",
        "status",
    ]

    df = (
        df.filter(pl.col("status") != "Cancelled")
        .with_columns(
            pl.col("timestamp").str.to_datetime(format="%-m/%-d/%y %I:%-M %p"),
            pl.col("receipt_type").cast(pl.Categorical),
            pl.col("payment_type").cast(pl.Enum(["Cash", "Gcash", "Card"])),
            pl.col("cashier_name").cast(pl.Enum(["Hannah", "Matet"])),
            pl.col("status").cast(pl.Categorical),
        )
        .with_columns(
            # parse order items
            pl.col("description")
            .str.extract(r"(\d) x (TITAN )?Wash")
            .fill_null(0)
            .cast(pl.Int8)
            .alias("n_wash"),
            pl.col("description")
            .str.extract(r"(\d) x (TITAN )?Dry")
            .fill_null(0)
            .cast(pl.Int8)
            .alias("n_dry"),
            pl.col("description")
            .str.extract(r"(\d) x Fold")
            .fill_null(0)
            .cast(pl.Int8)
            .alias("n_fold"),
            pl.col("description")
            .str.extract(r"(\d) x Ariel Liquid Detergent")
            .fill_null(0)
            .cast(pl.Int8)
            .alias("n_detergent"),
            pl.col("description")
            .str.extract(r"(\d) x Downy Fabcon")
            .fill_null(0)
            .cast(pl.Int8)
            .alias("n_fabcon"),
            pl.col("description")
            .str.extract(r"(\d) x Zonrox Colorsafe Bleach")
            .fill_null(0)
            .cast(pl.Int8)
            .alias("n_bleach"),
        )
        .with_columns(
            # create additional features
            pl.col("description").str.contains(r".*TITAN.*").alias("is_titan"),
            pl.when(
                pl.col("n_wash") > 0,
                pl.col("n_dry") > 0,
                pl.col("n_fold") > 0,
                pl.col("n_detergent") > 0,
                pl.col("n_fabcon") > 0,
            )
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .alias("is_full_load"),
            pl.when(
                pl.any_horizontal(
                    pl.col("n_bleach") > 0,
                    pl.col("n_detergent") > 1,
                    pl.col("n_fabcon") > 1,
                )
            )
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .alias("has_extra"),
        )
    )

    return df


def load_receipt_data():
    receipt_dir = Path("data/receipts")
    df_list = []
    for fp in receipt_dir.glob("*.csv"):
        df_list.append(_load_receipts_from_csv(fp))

    return (
        pl.concat(df_list)
        .unique(pl.col("receipt_id"))
        .sort("timestamp", descending=True)   
    )


def df2db(df: pl.DataFrame, db: Path, tablename: str) -> None:
    db = Path(db)
    if not db.parent.exists():
        db.parent.mkdir(exist_ok=True)

    with duckdb.connect(database=db, read_only=False) as con:
        con.execute(f"""
            CREATE OR REPLACE TABLE '{tablename}' AS
            SELECT * FROM df
        """)
    print(f"{tablename} table updated!")


def load_holidays(year: int = 2025):
    holidays_csv = Path(f"data/holidays/{year}.csv")
    return pl.read_csv(holidays_csv, has_header=True, try_parse_dates=True)


def install_duckdb_gsheets_ext():
    duckdb.sql("""
    INSTALL gsheets FROM community;
    LOAD gsheets;
    """)


def authenticate_sheets(credentials: Path) -> bool:
    credentials = Path(credentials)
    if not credentials.exists():
        raise FileNotFoundError

    install_duckdb_gsheets_ext()
    success = duckdb.sql(f"""
    CREATE OR REPLACE PERSISTENT SECRET (
        TYPE gsheet,
        PROVIDER key_file,
        FILEPATH '{credentials}' 
    )
    """).fetchone()[0]

    if not success:
        print("Failed to authenticate with Google Sheets")

    return success


def gsheets2df(
    url: str,
    credentials: Path,
    sheet_name: str,
    sheet_range: str,
    has_header: bool = True,
):
    authenticate_sheets(credentials)
    query = f"""
    FROM read_gsheet(
        '{url}',
        header = {"true" if has_header else "false"},
        sheet = '{sheet_name}',
        range = '{sheet_range}'
    )
    """
    result = duckdb.sql(query)
    df = result.fetchdf()

    if len(df) == 0:
        print("Returned a dataframe of length 0, exiting...")
        return

    df = pl.from_pandas(df).with_columns(
        pl.col("date").str.to_date("%m/%d/%Y"),
        pl.col("quantity").cast(pl.Int32),
        pl.col("total_cost").cast(pl.Float32),
    )

    return df


def load_expense_data() -> pl.DataFrame:
    sheets = {
        "capital_expenses": "A1:H108",
        "Jan2025": "A1:H4",
        "Feb2025": "A1:H25",
        "Mar2025": "A1:H27",
        "Apr2025": "A1:H41",
        "May2025": "A1:H30",
        "June2025": "A1:H33",
        "July2025": "A1:H41",
        "Aug2025": "A1:H38",
    }
    credentials = "credentials.json"
    url = "https://docs.google.com/spreadsheets/d/1PmYbcvwLeMfUiV9WDSWSfo_J45qzOXHEgYvvFOQzygA/edit"

    _expenses = []
    for sheet_name, sheet_range in sheets.items():
        _expenses.append(gsheets2df(url, credentials, sheet_name, sheet_range, True))

    return pl.concat(_expenses)

def item_list():
    return [
        Item(name="Ariel Detergent", category="soap", cost=18.0),
        Item(name="Downey Fabcon", category="soap", cost=12.0),
        Item(name="Zonrox Colorsafe Bleach", category="soap", cost=6.0),
        Item(name="Regular Wash", category="service", cost=65.0),
        Item(name="TITAN Wash", category="service", cost=80.0),
        Item(name="Hand Wash", category="service", cost=45.0),
        Item(name="Regular Dry", category="service", cost=65.0),
        Item(name="TITAN Dry", category="service", cost=90.0),
        Item(name="Extra Regular Dry", category="service", cost=17.0),
        Item(name="Extra TITAN Dry", category="service", cost=17.0),
        Item(name="Fold", category="service", cost=35.0),
    ]

def build_items_table(db) -> None:
    query = """
    CREATE OR REPLACE TABLE items (
        id UUID PRIMARY KEY DEFAULT uuidv4(),
        name VARCHAR(255) NOT NULL,
        category VARCHAR(255) NOT NULL,
        cost DOUBLE NOT NULL,
        created_at TIMESTAMP NOT NULL,
        updated_at TIMESTAMP NOT NULL,
        deleted_at TIMESTAMP
    );
    """
    with duckdb.connect(database=db, read_only=False) as con:
        # Initialize item tables
        con.execute(query)

        # Insert individual items
        for item in item_list():
            con.execute(f"""
            INSERT INTO items (name, category, cost, created_at, updated_at) 
            VALUES ('{item.name}', '{item.category}', {item.cost}, '{item.created_at}', '{item.updated_at}');
            """)


if __name__ == "__main__":
    print("Loading customer data...")
    customers_df = load_customer_data()

    print("Loading receipt data...")
    receipts_df = load_receipt_data()

    print("Loading expense data...")
    expenses_df = load_expense_data()

    db = "cleannest/db/main.db"

    print("Generating `customer` table...")
    df2db(customers_df, db, "customers")

    print("Generating `receipt` table...")
    df2db(receipts_df, db, "receipts")

    print("Generating `expense` table...")
    df2db(expenses_df, db, "expenses")

    print("Generating `items` table...")
    build_items_table(db)
