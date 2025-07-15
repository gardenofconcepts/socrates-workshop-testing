import sqlite3
from logging import Logger
from pathlib import Path
from textwrap import dedent
from unittest.mock import Mock

import pandas as pd

from app.db import DbImporter, DbSearch, DailyImporter
from app.source import KagglehubSource


class TestDbImporter:
    def test_import(self):
        # arrange
        expected_data = pd.DataFrame({'id': [1], 'brand': ['Hyundai'], 'model': ['Ionic 5']})
        import_data = pd.DataFrame({'id': [1, 2], 'brand': ['Hyundai', 'Nissan'], 'model': ['Ionic 5', 'Leaf']})

        conn = sqlite3.connect(':memory:')

        sut = DbImporter(conn)

        # act
        rows = sut.execute(import_data)

        # assert
        assert rows == 2

        # TODO: implementation detail
        df_hyundai = pd.read_sql_query("SELECT * FROM ev_specifications WHERE brand = 'Hyundai'", conn)

        pd.testing.assert_frame_equal(expected_data, df_hyundai)


class TestDbSearch:
    def test_search(self):
        # arrange
        conn = sqlite3.connect(':memory:')
        conn.execute("CREATE TABLE ev_specifications (id INTEGER, brand TEXT, model TEXT)")
        conn.execute("INSERT INTO ev_specifications (id, brand, model) VALUES (1, 'Hyundai', 'Ionic 5')")
        conn.execute("INSERT INTO ev_specifications (id, brand, model) VALUES (2, 'Nissan', 'Leaf')")

        sut = DbSearch(conn)

        # act
        result = sut.search("Hyundai")

        # assert
        expected = pd.DataFrame({'id': [1], 'brand': ['Hyundai'], 'model': ['Ionic 5']})
        pd.testing.assert_frame_equal(expected, result)

    def test_list_brands(self):
        # arrange
        expected = pd.Series(['Hyundai', 'Nissan'], name='brand')

        conn = sqlite3.connect(':memory:')
        conn.execute("CREATE TABLE ev_specifications (id INTEGER, brand TEXT, model TEXT)")
        conn.execute("INSERT INTO ev_specifications (id, brand, model) VALUES (1, 'Hyundai', 'Ionic 5')")
        conn.execute("INSERT INTO ev_specifications (id, brand, model) VALUES (2, 'Nissan', 'Leaf')")

        sut = DbSearch(conn)

        # act
        result = sut.list_brands()

        # assert
        pd.testing.assert_series_equal(expected, result)


class TestDailyImporter:
    def test_execute(self, tmp_path: Path):
        csv_data = dedent("""
        id,brand,model
        1,Hyundai,Ionic 5
        """)

        csv_file = tmp_path / "data.csv"
        csv_file.write_text(csv_data)

        importer = Mock(spec=DailyImporter)
        importer.execute.return_value = 0

        source = Mock(spec=KagglehubSource)
        source.return_value = csv_file

        logger = Mock(spec=Logger)

        sut = DailyImporter(importer, source, logger)
        result = sut.execute()

        assert result == 0
