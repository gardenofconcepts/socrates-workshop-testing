from logging import Logger
from pathlib import Path
from textwrap import dedent
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from app.source import KagglehubSource, CsvReader


class TestKagglehubSource:

    @pytest.mark.e2e
    def test_real_download_behaviour(self):
        logger = Mock(spec=Logger)

        sut = KagglehubSource(logger)

        result = sut()

        assert result is not None
        assert result.is_file()

    def test_download_with_mock(self, tmp_path: Path):
        # arrange
        logger = Mock(spec=Logger)

        result_file = tmp_path / "data.csv"
        result_file.write_text("hi socrates")

        with patch("kagglehub.dataset_download", return_value=tmp_path) as mock_fn:
            sut = KagglehubSource(logger)

            result = sut()

        # assert
        mock_fn.assert_called_once_with(handle="urvishahir/electric-vehicle-specifications-dataset-2025")
        mock_fn.assert_called_once()

        assert result is not None
        assert result.is_file()
        assert result == result_file
        assert result.read_text() == "hi socrates"


class TestCsvReader:

    def test_read_csv(self, tmp_path: Path):
        # arrange
        csv_data = dedent("""
        id,brand,model
        1,Hyundai,Ionic 5
        """)

        csv_file = tmp_path / "data.csv"
        csv_file.write_text(csv_data)

        expected = pd.DataFrame({'id': [1], 'brand': ['Hyundai'], 'model': ['Ionic 5']})

        sut = CsvReader(csv_file)

        # act
        result = sut.execute()

        pd.testing.assert_frame_equal(expected, result)
