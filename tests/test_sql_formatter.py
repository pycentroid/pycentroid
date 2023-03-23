import pytest
from unittest import TestCase
from themost_framework.query import SqlFormatter, QueryExpression, QueryEntity, QueryField

class Product:
    def __init__(self, name):
        self.name = name
    def set_offer(self, offer):
        # do something here
        return true

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
        { 'name': 'Macbook Pro 13.3' }
        ).where(
            lambda x: x.id == 121
            )
    sql = SqlFormatter().format_update(query)
    TestCase().assertEqual(sql, 'UPDATE Product SET name=\'Macbook Pro 13.3\' WHERE id=121')

    query = QueryExpression().update('Product').set(
        Product('Macbook Pro 13.3')
        ).where(
            lambda x: x.id == 121
            )
    sql = SqlFormatter().format_update(query)
    TestCase().assertEqual(sql, 'UPDATE Product SET name=\'Macbook Pro 13.3\' WHERE id=121')

def test_format_insert():
    
    object = lambda: None
    object.name = 'Lenovo Yoga 2'
    products = QueryEntity('ProductData')
    query = QueryExpression().insert(
        object
    ).into(products)
    sql = SqlFormatter().format_insert(query)
    TestCase().assertEqual(sql, 'INSERT INTO ProductData(name) VALUES (\'Lenovo Yoga 2\')')

def test_format_join():
    
    object = lambda: None
    object.name = 'Lenovo Yoga 2'
    query = QueryExpression('OrderData').select(
        'id', 'customer', 'orderDate', 'orderedItem'
    ).join( collection= 'ProductData').on(
        QueryExpression().where(
            'orderedItem'
        ).equal(
            QueryField('id').from_collection('ProductData')
            )
    )
    sql = SqlFormatter().format_select(query)
    TestCase().assertEqual(sql, 'SELECT id,customer,orderDate,orderedItem FROM OrderData INNER JOIN ProductData ON orderedItem=ProductData.id')
