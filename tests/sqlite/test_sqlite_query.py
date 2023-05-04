import pytest
from centroid.sqlite import SqliteAdapter
from centroid.common import AnyObject
from centroid.query import QueryEntity, QueryExpression
from unittest import TestCase
from os.path import abspath, join, dirname

Products = QueryEntity('ProductData')

db = None


@pytest.fixture()
def db() -> SqliteAdapter:
    return SqliteAdapter(AnyObject(database=abspath(join(dirname(__file__), '../db/local.db'))))


# noinspection PyShadowingNames
async def test_select(db):
    items = await db.execute(QueryExpression(Products).select(
        lambda x: (x.id, x.name, x.category)
    ))
    TestCase().assertGreater(len(items), 0)
    properties = list(items[0].__dict__.keys())
    TestCase().assertEqual(properties, [
        'id',
        'name',
        'category'
    ])


# noinspection PyShadowingNames
async def test_take(db):
    query = QueryExpression(Products).select(
        lambda x: (x.id, x.name, x.category)
    ).where(
        lambda x: x.category == 'Laptops'
    ).take(10)
    items = await db.execute(query)
    TestCase().assertEqual(len(items), 10)
    for item in items:
        TestCase().assertEqual(item.category, 'Laptops')


# noinspection PyShadowingNames
async def test_and(db):
    query = QueryExpression(Products).select(
        lambda x: (x.id, x.name, x.category)
    ).where(
        lambda x: x.category == 'Laptops' and x.price > 500
    )
    items = await db.execute(query)
    for item in items:
        TestCase().assertEqual(item.category, 'Laptops')


# noinspection PyShadowingNames
async def test_or(db):
    query = QueryExpression(Products).select(
        lambda x: (x.id, x.name, x.category)
    ).where(
        lambda x: x.category == 'Laptops' or x.category == 'Desktops'
    )
    items = await db.execute(query)
    for item in items:
        TestCase().assertEqual(item.category in ['Laptops', 'Desktops'], True)


# noinspection PyShadowingNames
async def test_complex_logical(db):
    query = QueryExpression(Products).select(
        lambda x: (x.id, x.name, x.category, x.price)
    ).where(
        lambda x: (x.category == 'Laptops' or x.category == 'Desktops') and x.price <= 800
    )
    items = await db.execute(query)
    TestCase().assertGreater(len(items), 0)
    for item in items:
        TestCase().assertEqual(item.category in ['Laptops', 'Desktops'], True)
        TestCase().assertLessEqual(item.price, 800)


# noinspection PyShadowingNames
async def test_complex_logical_or(db):
    query = QueryExpression(Products).select(
        lambda x: [x.id, x.name, x.category, x.price]
    ).where(
        lambda x: (x.category == 'Laptops' or x.category == 'Desktops') and x.price <= 800
    )
    items = await db.execute(query)
    TestCase().assertGreater(len(items), 0)
    for item in items:
        TestCase().assertEqual(item.category in ['Laptops', 'Desktops'], True)
        TestCase().assertLessEqual(item.price, 800)


# noinspection PyShadowingNames
async def test_order_by(db):
    query = QueryExpression(Products).select(lambda x: (x.id, x.name, x.category, x.price)).where(
        lambda x: x.category == 'Laptops'
    ).order_by(
        lambda x: (x.price,)
    )
    items = await db.execute(query)
    TestCase().assertGreater(len(items), 0)
    for index, item in enumerate(items):
        if index > 0:
            TestCase().assertGreaterEqual(item.price, items[index - 1].price)


# noinspection PyShadowingNames
async def test_order_by_descending(db):
    query = QueryExpression(Products).select(lambda x: (x.id, x.name, x.category, x.price)).where(
        lambda x: x.category == 'Laptops'
    ).order_by_descending(
        lambda x: (x.price,)
    )
    items = await db.execute(query)
    TestCase().assertGreater(len(items), 0)
    for index, item in enumerate(items):
        if index > 0:
            TestCase().assertLessEqual(item.price, items[index - 1].price)


# noinspection PyShadowingNames
async def test_where_with_args(db):
    query = QueryExpression(Products).select(
        lambda x: (x.id, x.name, x.category, x.price)
    ).where((
        lambda x, category: x.category == category
    ), category='Laptops')
    items = await db.execute(query)
    TestCase().assertGreater(len(items), 0)
    for item in items:
        TestCase().assertEqual(item.category, 'Laptops')
