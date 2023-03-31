import pytest
from themost_framework.sqlite import SqliteAdapter
from themost_framework.common import object
from themost_framework.query import DataColumn, QueryEntity, QueryExpression, select, TestUtils
from unittest import TestCase
import re

connection_options = object(database='tests/db/local.db')
Products = QueryEntity('ProductData')

def test_select():
    db = SqliteAdapter(connection_options)
    items = db.execute(QueryExpression(Products).select(
        lambda x: [ x.id, x.name, x.category ]
    ))
    TestCase().assertGreater(len(items), 0)
    properties = list(items[0].__dict__.keys())
    TestCase().assertEqual(properties, [
        'id',
        'name',
        'category'
    ])
    db.close()

def test_take():
    db = SqliteAdapter(connection_options)
    query = QueryExpression(Products).select(
        lambda x: [ x.id, x.name, x.category ]
    ).where(
        lambda x: x.category == 'Laptops'
        ).take(10)
    items = db.execute(query)
    TestCase().assertEqual(len(items), 10)
    for item in items:
        TestCase().assertEqual(item.category, 'Laptops')
    db.close()

def test_and():
    db = SqliteAdapter(connection_options)
    query = QueryExpression(Products).select(
        lambda x: [ x.id, x.name, x.category ]
        ).where(
            lambda x: x.category == 'Laptops' and x.price > 500
        )
    items = db.execute(query)
    for item in items:
        TestCase().assertEqual(item.category, 'Laptops')
    db.close()

def test_or():
    db = SqliteAdapter(connection_options)
    query = QueryExpression(Products).select(
        lambda x: [ x.id, x.name, x.category ]
        ).where(
            lambda x: x.category == 'Laptops' or x.category == 'Tablets'
        )
    items = db.execute(query)
    for item in items:
        TestCase().assertEqual(item.category, 'Laptops')
    db.close()