import sqlite3
from datetime import datetime
from logging import Logger
from pathlib import Path
from textwrap import dedent
from unittest.mock import Mock

import pandas as pd
import pytest
# from freezegun import freeze_time

from app.db import DailyImporter, DbImporter, DbSearch
from app.source import KagglehubSource


@pytest.fixture(scope="function", name="db_conn")
def fixture_test_db():
    conn = sqlite3.connect(':memory:')
    conn.execute("CREATE TABLE ev_specifications (id INTEGER, brand TEXT, model TEXT)")
    conn.execute("INSERT INTO ev_specifications (id, brand, model) VALUES (1, 'Hyundai', 'Ionic 5')")
    conn.execute("INSERT INTO ev_specifications (id, brand, model) VALUES (2, 'Nissan', 'Leaf')")

    return conn


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

        df_hyundai = pd.read_sql_query("SELECT * FROM ev_specifications WHERE brand = 'Hyundai'", conn)

        pd.testing.assert_frame_equal(expected_data, df_hyundai)


class TestDbSearch:
    def test_search(self, db_conn):
        # arrange
        sut = DbSearch(db_conn)

        # act
        result = sut.search("Hyundai")

        # assert
        expected = pd.DataFrame({'id': [1], 'brand': ['Hyundai'], 'model': ['Ionic 5']})
        pd.testing.assert_frame_equal(expected, result)

    def test_list_brands(self, db_conn):
        # arrange
        expected = pd.Series(['Hyundai', 'Nissan'], name='brand')

        sut = DbSearch(db_conn)

        # act
        result = sut.list_brands()

        # assert
        pd.testing.assert_series_equal(expected, result)


class TestDailyImporter:
    def test_execute(self, tmp_path: Path):
        # arrange
        csv_data = dedent("""
        id,brand,model
        1,Hyundai,Ionic 5
        """)

        logger = Mock(spec=Logger)

        csv_file = tmp_path / "data.csv"
        csv_file.write_text(csv_data)

        source = Mock(spec=KagglehubSource)
        source.return_value = csv_file

        importer = Mock(spec=DailyImporter)
        importer.execute.return_value = 1

        sut = DailyImporter(importer, source, logger)

        # act
        result = sut.execute()

        # assert
        assert result == 1
        assert source.called
        assert importer.execute.called
        assert logger.info.called

    def test_get_last_import_date(self, tmp_path: Path):
        # arrange
        csv_data = dedent("""
        id,brand,model
        1,Hyundai,Ionic 5
        """)

        logger = Mock(spec=Logger)

        csv_file = tmp_path / "data.csv"
        csv_file.write_text(csv_data)

        last_import_file = tmp_path / "last_import_date.txt"
        last_import_file.write_text(datetime.now().isoformat())

        source = Mock(spec=KagglehubSource)
        source.return_value = csv_file

        importer = Mock(spec=DailyImporter)
        importer.execute.return_value = 1

        sut = DailyImporter(importer, source, logger)

        # act
        result = sut.execute()

        # assert
        assert result is None
        assert not source.called
        assert not importer.execute.called
        assert logger.info.called

# def test_is_today_a_socrates_conf(self):
#     with freeze_time("2025-07-19"):
#         now = datetime.now()
#
#     assert now.year == 2025
#     assert now.month == 7
#     assert now.day == 19
