import logging
import sqlite3

from app.db import DbImporter, DbSearch, DailyImporter
from app.source import KagglehubSource


def main():
    conn = sqlite3.connect('ev_specifications.db')

    logger = logging.getLogger(__name__)

    source = KagglehubSource(logger.getChild("kagglehub"))
    importer = DbImporter(conn)

    daily_importer = DailyImporter(importer, source, logger.getChild('daily_importer'))
    daily_importer.execute()

    search = DbSearch(conn)
    brands = search.list_brands()

    print(brands.head(100))

    hyundai_data = search.search(brand="Hyundai")
    print(hyundai_data[["brand", "model"]].head(100))


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
