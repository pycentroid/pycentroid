import pytest
from os.path import abspath, join, dirname
from centroid.data.application import DataApplication
from centroid.data.context import DataContext
from unittest import TestCase


APP_PATH = abspath(join(dirname(__file__), '..'))


@pytest.fixture()
def context() -> DataContext:
    app = DataApplication(cwd=APP_PATH)
    return app.create_context()


async def test_count(context: DataContext):
    length = await context.model('Product').as_queryable().count()
    TestCase().assertGreater(length, 0)


async def test_select(context: DataContext):
    results = await context.model('Product').as_queryable().select(
        lambda x: (x.id, x.category, x.price,)
    ).get_items()
    TestCase().assertGreater(len(results), 0)
