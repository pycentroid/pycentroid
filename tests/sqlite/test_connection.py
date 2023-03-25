import pytest
from themost_framework.sqlite import SqliteAdapter
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

