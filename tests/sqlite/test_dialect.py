from atmost.sqlite import SqliteDialect
from unittest import TestCase


def test_format_type():
    dialect = SqliteDialect()
    type_str = dialect.format_type(name='description', type='Text')
    TestCase().assertEqual(type_str, '"description" TEXT NULL')
    
    type_str = dialect.format_type(name='description', type='Text', size=255)
    TestCase().assertEqual(type_str, '"description" TEXT(255) NULL')

    type_str = dialect.format_type(name='id', type='Counter', nullable=False)
    TestCase().assertEqual(type_str, '"id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL')

    type_str = dialect.format_type(name='price', type='Decimal', nullable=False, size=19, scale=4)
    TestCase().assertEqual(type_str, '"price" NUMERIC(19,4) NOT NULL')
