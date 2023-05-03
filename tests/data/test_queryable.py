import pytest
from os.path import abspath, join, dirname
from centroid.data.application import DataApplication
from centroid.data.context import DataContext
from unittest import TestCase
from types import SimpleNamespace


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


async def test_find(context: DataContext):
    results = await context.model('Product').as_queryable().find({
        'category': 'Desktops'
    }).get_items()
    TestCase().assertGreater(len(results), 0)
    for result in results:
        TestCase().assertEqual(result.category, 'Desktops')
    
    results = await context.model('Product').as_queryable().find(SimpleNamespace(category='Laptops')).get_items()
    TestCase().assertGreater(len(results), 0)
    for result in results:
        TestCase().assertEqual(result.category, 'Laptops')


async def test_query_association(context: DataContext):
    results = await context.model('Order').where(
        lambda x: x.orderedItem.category == 'Desktops'
        ).get_items()
    TestCase().assertGreater(len(results), 0)


async def test_find_by_obj(context: DataContext):
    results = await context.model('Order').find(
        {
            'orderedItem': 84,
            'orderStatus': {
                'name': 'Processing'
            }
        }
    ).get_items()
    TestCase().assertGreater(len(results), 0)
    for result in results:
        TestCase().assertEqual(result.orderedItem, 84)
        TestCase().assertEqual(result.orderStatus, 7)


async def test_expand(context: DataContext):
    results = await context.model('Order').where(
        lambda x: x.orderStatus.alternateName == 'OrderProcessing'
    ).expand(
        lambda x: (x.customer,)
        ).take(25).get_items()
    TestCase().assertGreater(len(results), 0)
    for result in results:
        TestCase().assertIsNotNone(result.orderedItem.id)
        TestCase().assertIsNotNone(result.customer.id)
