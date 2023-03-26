import pytest
from themost_framework.sqlite import SqliteAdapter
from themost_framework.query import DataColumn, QueryExpression, select
from themost_framework.common import TestUtils
from unittest import TestCase

__author__ = "Kyriakos Barbounakis"
__copyright__ = "Kyriakos Barbounakis"
__license__ = "BSD-3-Clause"

connection_options = lambda: None
connection_options.database = 'tests/db/local.db'


def test_create_connection():
    db = SqliteAdapter(connection_options)
    db.open()
    TestCase().assertIsNotNone(db.__raw_connection__)
    db.close()
    TestCase().assertIsNone(db.__raw_connection__)

def test_table_exists():
    db = SqliteAdapter(connection_options)
    exists = db.table('ThingBase').exists()
    TestCase().assertTrue(exists)
    db.close()

def test_get_table_columns():
    db = SqliteAdapter(connection_options)
    columns = db.table('ThingBase').columns()
    db.close()

def test_create_table():
    db = SqliteAdapter(connection_options)
    def execute():
        db.table('Table1').create([
            DataColumn(name='id', type='Counter'),
            DataColumn(name='name', type='Text', nullable=False, size=255)
        ])
        exists = db.table('Table1').exists()
        TestCase().assertEqual(exists, True)
        version = db.table('Table1').version()
        TestCase().assertEqual(version, None)
        db.table('Table1').drop()
        exists = db.table('Table1').exists()
        TestCase().assertEqual(exists, False)

    TestUtils(db).execute_in_transaction(execute)
    db.close()

def test_change_table():
    db = SqliteAdapter(connection_options)
    def execute():
        db.table('Table1').create([
            DataColumn(name='id', type='Counter'),
            DataColumn(name='name', type='Text', nullable=False, size=255)
        ])
        db.table('Table1').change([
            DataColumn(name='id', type='Counter'),
            DataColumn(name='name', type='Text', nullable=False, size=512)
        ])
        columns = db.table('Table1').columns()
        column = next(filter(lambda x:x.name== 'name', columns),None)
        TestCase().assertIsNotNone(column)
        TestCase().assertEqual(column.size, 512)
        db.table('Table1').drop()

    TestUtils(db).execute_in_transaction(execute)
    db.close()

def test_get_columns():
    db = SqliteAdapter(connection_options)
    def execute():
        db.table('Table1').create([
            DataColumn(name='id', type='Counter'),
            DataColumn(name='name', type='Text', nullable=False, size=255)
        ])
        columns = db.table('Table1').columns()
        TestCase().assertGreater(len(columns), 0)
        db.table('Table1').drop()

    TestUtils(db).execute_in_transaction(execute)
    db.close()

def test_create_view():
    db = SqliteAdapter(connection_options)
    def execute():
        db.table('Table1').create([
            DataColumn(name='id', type='Counter'),
            DataColumn(name='name', type='Text', nullable=False, size=255),
            DataColumn(name='description', type='Text', size=512),
            DataColumn(name='dateCreated', type='DateTime'),
            DataColumn(name='dateModified', type='DateTime')
        ])
        exists = db.table('Table1').exists()
        TestCase().assertEqual(exists, True)

        query = QueryExpression().select(
            lambda x: select(id=x.id, name=x.name)
        ).from_collection('Table1')
        db.view('Table1View').create(query)
        exists = db.view('Table1View').exists()
        TestCase().assertEqual(exists, True)
        db.view('Table1View').drop()
        exists = db.view('Table1View').exists()
        TestCase().assertEqual(exists, False)

    TestUtils(db).execute_in_transaction(execute)
    db.close()

def test_create_index():
    db = SqliteAdapter(connection_options)
    def execute():
        db.table('Table1').create([
            DataColumn(name='id', type='Counter'),
            DataColumn(name='name', type='Text', nullable=False, size=255),
            DataColumn(name='description', type='Text', size=512),
            DataColumn(name='dateCreated', type='DateTime'),
            DataColumn(name='dateModified', type='DateTime')
        ])
        indexes = db.indexes('Table1').list()
        TestCase().assertEqual(len(indexes), 0)
        db.indexes('Table1').create('INDEX_NAME', [
            DataColumn(name='name')
        ])
        indexes = db.indexes('Table1').list()
        TestCase().assertEqual(len(indexes), 1)
        db.indexes('Table1').drop('INDEX_NAME')
        indexes = db.indexes('Table1').list()
        TestCase().assertEqual(len(indexes), 0)
        db.table('Table1').drop()

    TestUtils(db).execute_in_transaction(execute)
    db.close()

