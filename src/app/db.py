from logging import Logger
from pathlib import Path

import pandas as pd
from datetime import datetime

from app.source import KagglehubSource, CsvReader


class DbImporter:
    def __init__(self, conn):
        self.conn = conn

    def execute(self, df) -> int:
        rows = df.to_sql("ev_specifications", self.conn, if_exists="replace", index=False)

        return rows


class DbSearch:
    def __init__(self, conn):
        self.conn = conn

    def search(self, brand: str = None) -> pd.DataFrame:
        query = "SELECT * FROM ev_specifications"
        params = {}

        if brand is not None:
            query = "SELECT * FROM ev_specifications WHERE brand = :brand"
            params = {"brand": brand}

        return pd.read_sql_query(query, self.conn, params=params)

    def list_brands(self) -> pd.Series:
        query = "SELECT DISTINCT brand FROM ev_specifications"

        df = pd.read_sql_query(query, self.conn)

        return df['brand']


class DailyImporter:
    def __init__(self, db_importer: DbImporter, source: KagglehubSource, logger: Logger):
        self.importer = db_importer
        self.source = source
        self.logger = logger
        self.file = Path("last_import_date.txt")

    def execute(self) -> int | None:
        if not self.should_import_today():
            self.logger.info("Skipping import.")
            return None

        self.logger.info("Starting daily import...")

        result = self.source()

        data = CsvReader(result).execute()
        rows = self.importer.execute(data)

        self.set_last_import_date(datetime.now())

        return rows

    def get_last_import_date(self) -> datetime:
        if not self.file.exists():
            return datetime.min

        data = self.file.read_text().strip()
        if not data:
            return datetime.min
        try:
            return datetime.fromisoformat(data)
        except ValueError:
            # If the date is not in the correct format, return a default value
            self.file.unlink()
            self.file.write_text(datetime.min.isoformat())
            return datetime.min

    def set_last_import_date(self, date: datetime):
        self.file.write_text(date.isoformat())

    def should_import_today(self) -> bool:
        last_import_date = self.get_last_import_date()
        today = datetime.now().date()

        return last_import_date.date() < today
