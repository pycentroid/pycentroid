from unittest import TestCase

from centroid.common import AnyObject
from centroid.query import DataColumn, QueryEntity, QueryExpression, select, TestUtils
from centroid.sqlite import SqliteAdapter, SqliteFormatter
from os.path import abspath, join, dirname
import logging

connection_options = AnyObject(database=abspath(join(dirname(__file__), '../db/local.db')))


async def test_create_connection():
    db = SqliteAdapter(connection_options)
    await db.open()
    TestCase().assertIsNotNone(db.__raw_connection__)
    await db.close()
    TestCase().assertIsNone(db.__raw_connection__)


async def test_table_exists():
    db = SqliteAdapter(connection_options)
    exists = await db.table('ThingBase').exists()
    TestCase().assertTrue(exists)
    await db.close()


async def test_get_table_columns():
    db = SqliteAdapter(connection_options)
    columns = db.table('ThingBase').columns()
    TestCase().assertIsNotNone(columns)
    await db.close()


async def test_create_table():
    db = SqliteAdapter(connection_options)

    async def execute():
        await db.table('Table1').create([
            DataColumn(name='id', type='Counter'),
            DataColumn(name='name', type='Text', nullable=False, size=255)
        ])
        exists = await db.table('Table1').exists()
        TestCase().assertEqual(exists, True)
        version = await db.table('Table1').version()
        TestCase().assertEqual(version, None)
        await db.table('Table1').drop()
        exists = await db.table('Table1').exists()
        TestCase().assertEqual(exists, False)

    await TestUtils(db).execute_in_transaction(execute)
    await db.close()


async def test_change_table():
    db = SqliteAdapter(connection_options)

    async def execute():
        await db.table('Table1').create([
            DataColumn(name='id', type='Counter'),
            DataColumn(name='name', type='Text', nullable=False, size=255)
        ])
        await db.table('Table1').change([
            DataColumn(name='id', type='Counter'),
            DataColumn(name='name', type='Text', nullable=False, size=512)
        ])
        columns = await db.table('Table1').columns()
        column = next(filter(lambda x: x.name == 'name', columns), None)
        TestCase().assertIsNotNone(column)
        TestCase().assertEqual(column.size, 512)
        await db.table('Table1').drop()

    await TestUtils(db).execute_in_transaction(execute)
    await db.close()


async def test_get_columns():
    db = SqliteAdapter(connection_options)

    async def execute():
        await db.table('Table1').create([
            DataColumn(name='id', type='Counter'),
            DataColumn(name='name', type='Text', nullable=False, size=255)
        ])
        columns = await db.table('Table1').columns()
        TestCase().assertGreater(len(columns), 0)
        await db.table('Table1').drop()

    await TestUtils(db).execute_in_transaction(execute)
    await db.close()


async def test_create_view():
    db = SqliteAdapter(connection_options)

    async def execute():
        await db.table('Table1').create([
            DataColumn(name='id', type='Counter'),
            DataColumn(name='name', type='Text', nullable=False, size=255),
            DataColumn(name='description', type='Text', size=512),
            DataColumn(name='dateCreated', type='DateTime'),
            DataColumn(name='dateModified', type='DateTime')
        ])
        exists = await db.table('Table1').exists()
        TestCase().assertEqual(exists, True)

        query = QueryExpression().select(
            lambda x: select(id=x.id, name=x.name)
        ).from_collection('Table1')
        await db.view('Table1View').create(query)
        exists = await db.view('Table1View').exists()
        TestCase().assertEqual(exists, True)
        await db.view('Table1View').drop()
        exists = await db.view('Table1View').exists()
        TestCase().assertEqual(exists, False)

    await TestUtils(db).execute_in_transaction(execute)
    await db.close()


async def test_create_index():
    db = SqliteAdapter(connection_options)

    async def execute():
        await db.table('Table1').create([
            DataColumn(name='id', type='Counter'),
            DataColumn(name='name', type='Text', nullable=False, size=255),
            DataColumn(name='description', type='Text', size=512),
            DataColumn(name='dateCreated', type='DateTime'),
            DataColumn(name='dateModified', type='DateTime')
        ])
        indexes = await db.indexes('Table1').list()
        TestCase().assertEqual(len(indexes), 0)
        await db.indexes('Table1').create('INDEX_NAME', [
            DataColumn(name='name')
        ])
        indexes = await db.indexes('Table1').list()
        TestCase().assertEqual(len(indexes), 1)
        await db.indexes('Table1').drop('INDEX_NAME')
        indexes = await db.indexes('Table1').list()
        TestCase().assertEqual(len(indexes), 0)
        await db.table('Table1').drop()

    await TestUtils(db).execute_in_transaction(execute)
    await db.close()


async def test_sqlite_regex():
    db = SqliteAdapter(connection_options)
    await db.open()
    # noinspection PyPep8Naming
    Products = QueryEntity('ProductData')
    query = QueryExpression(Products).where(lambda x: x.name.__contains__('Apple') is True)
    formatter = SqliteFormatter()
    sql = formatter.format(query)
    items = await db.execute(
        QueryExpression(Products).where(lambda x: x.name.__contains__('Apple') is True)
    )
    TestCase().assertGreater(len(items), 0)
    for item in items:
        TestCase().assertEqual(item.name.__contains__('Apple'), True)
