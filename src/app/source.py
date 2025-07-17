from logging import Logger
from pathlib import Path

import kagglehub
import pandas as pd
from kagglehub.exceptions import KaggleApiHTTPError
from pandas import DataFrame


class SourceError(Exception):
    pass


class KagglehubSource:
    def __init__(self, logger: Logger):
        self.logger = logger

    def __call__(self) -> Path | None:
        try:
            csv_path = kagglehub.dataset_download(
                handle="urvishahir/electric-vehicle-specifications-dataset-2025")
            self.logger.info(f"Dataset downloaded to: {csv_path}")
        except KaggleApiHTTPError as e:
            self.logger.error(f"Error downloading dataset: {e}")

            raise SourceError(f"Error downloading dataset: {e}") from e

        csv_files = Path(csv_path).glob("*.csv")

        if not csv_files:
            raise SourceError(f"No csv files found in {csv_path}")

        csv_file = next(csv_files, None)

        return csv_file


class CsvReader:
    def __init__(self, csv_path: Path):
        self.csv_path = csv_path

    def execute(self) -> DataFrame:
        return pd.read_csv(self.csv_path)
