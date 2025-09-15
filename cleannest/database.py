from pathlib import Path
import polars as pl
import duckdb


class CleannestDatabase:
    def __init__(self):
        self.db = Path(__file__).parent / "db" / "main.db"
        self.con = duckdb.connect(self.db)

    def fetch_customers(self) -> pl.DataFrame:
        query = "SELECT * FROM customers"
        return self.con.execute(query).pl()

    def fetch_receipts(self):
        query = "SELECT * FROM receipts"
        return self.con.execute(query).pl()

    def fetch_expenses(self):
        query = "SELECT * FROM expenses"
        return self.con.execute(query).pl()
