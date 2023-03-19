import pytest
from unittest import TestCase
from themost_framework.query import SqlFormatter, QueryExpression

def test_format_where():
    
    query = QueryExpression().where(lambda x: x.category == 'Laptops')
    sql = SqlFormatter().format_where(query.__where__)
    TestCase().assertEqual(sql, 'category=\'Laptops\'')

    query = QueryExpression('Product').where(lambda x: x.category == 'Laptops' \
        or x.category == 'Desktops')
    sql = SqlFormatter().format_where(query.__where__)
    TestCase().assertEqual(sql, '(category=\'Laptops\' OR category=\'Desktops\')')

def test_format_select():
    
    query = QueryExpression('Product').where(lambda x: x.category == 'Laptops' \
        or x.category == 'Desktops')
    sql = SqlFormatter().format_select(query)
    TestCase().assertEqual(sql, 'SELECT * FROM Product WHERE (category=\'Laptops\' OR category=\'Desktops\')')

    query = QueryExpression('Product').select(
        lambda x: [ x.id, x.name, x.category, x.price ]
        ).where(
            lambda x: x.category == 'Laptops' or x.category == 'Desktops'
            )
    sql = SqlFormatter().format_select(query)
    TestCase().assertEqual(sql, 'SELECT id,name,category,price FROM Product WHERE (category=\'Laptops\' OR category=\'Desktops\')')

def test_format_update():
    
    query = QueryExpression().update('Product').set(
        { 'status': 'updated' }
        ).where(
            lambda x: x.category == 'Laptops'
            )
    sql = SqlFormatter().format_update(query)
    TestCase().assertEqual(sql, 'UPDATE Product SET status=\'updated\' WHERE category=\'Laptops\'')