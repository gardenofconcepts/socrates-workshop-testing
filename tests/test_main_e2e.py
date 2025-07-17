import logging
import sqlite3

import pandas as pd
from pytest_bdd import parsers, scenario, given, when, then

from app.db import DbImporter, DailyImporter, DbSearch
from app.source import KagglehubSource

logger = logging.getLogger(__name__)


@scenario("e2e/features/App.feature", "List brands")
def test_main():
    pass


@given("I have the latest data loaded", target_fixture="conn")
def load():
    conn = sqlite3.connect('ev_specifications.db')
    source = KagglehubSource(logger.getChild("kagglehub"))
    importer = DbImporter(conn)

    daily_importer = DailyImporter(importer, source, logger.getChild('daily_importer'))
    daily_importer.execute()

    return conn


@when(
    parsers.cfparse("I search for brand \"{brand}\"", extra_types={"brand": str}),
    target_fixture="brand_search_result"
)
def when_search_for_brand(brand: str, conn):
    search = DbSearch(conn)
    brands = search.search(brand=brand)

    return brands


@then(
    parsers.cfparse("I should see \"{brand}\" in the brand list", extra_types={"brand": str}),
)
def then_should_see_brand(brand: str, brand_search_result: pd.DataFrame):
    expected_df = pd.DataFrame({'brand': [brand]})

    brands_df = brand_search_result[['brand']].drop_duplicates().reset_index(drop=True)

    pd.testing.assert_frame_equal(brands_df, expected_df)
