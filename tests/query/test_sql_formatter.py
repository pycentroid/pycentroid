import pytest
from unittest import TestCase
from themost_framework.query import SqlFormatter, QueryExpression, QueryEntity, QueryField, select
from datetime import datetime

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

def test_format_select_with_lambda():
    
    query = QueryExpression('ProductData').select(
        lambda x: [ x.id, x.name, x.category, x.price ]
        ).where(
            lambda x: x.category == 'Laptops' or x.category == 'Desktops'
            )
    sql = SqlFormatter().format_select(query)
    TestCase().assertEqual(sql, 'SELECT id,name,category,price FROM ProductData WHERE (category=\'Laptops\' OR category=\'Desktops\')')

def test_format_select_with_map():
    
    query = QueryExpression('Product').select(
            lambda x: select(id = x.id, productName = x.name, category = x.category)
        ).where(
            lambda x: x.category == 'Laptops' or x.category == 'Desktops'
            )
    sql = SqlFormatter().format_select(query)
    TestCase().assertEqual(sql, 'SELECT id,name AS productName,category FROM Product WHERE (category=\'Laptops\' OR category=\'Desktops\')')


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

def test_format_order_by():
    
    query = QueryExpression().select('id', 'name', 'category', 'releaseDate', 'price')\
        .from_collection('ProductData').order_by('name').then_by('category')
    sql = SqlFormatter().format_select(query)
    TestCase().assertEqual(sql, 'SELECT id,name,category,releaseDate,price FROM ProductData ORDER BY name ASC,category ASC')

    query = QueryExpression().select(
        lambda x: [x.id, x.name, x.category, x.releaseDate, x.price]
    ).from_collection('ProductData').order_by(
            lambda x: [x.name, x.category]
        )
    sql = SqlFormatter().format_select(query)
    TestCase().assertEqual(sql, 'SELECT id,name,category,releaseDate,price FROM ProductData ORDER BY name ASC,category ASC')


def test_format_limit_select():
    
    query = QueryExpression().select('id', 'name', 'category', 'releaseDate', 'price')\
        .from_collection('ProductData').take(5).skip(5).order_by('name').then_by('category')
    sql = SqlFormatter().format_limit_select(query)
    TestCase().assertEqual(sql, 'SELECT id,name,category,releaseDate,price FROM ProductData ORDER BY name ASC,category ASC LIMIT 5,5')

def test_format_group_by():
    
    query = QueryExpression().select(
        QueryField('id').get_count().as_('total'),
        QueryField('category')
    ).from_collection('ProductData').group_by(QueryField('category'))
    sql = SqlFormatter().format_select(query)
    TestCase().assertEqual(sql, 'SELECT COUNT(id) AS total,category FROM ProductData GROUP BY category')

def test_use_round():
    query = QueryExpression('ProductData').select(
        lambda x:select(id=x.id,name=x.name,price=round(x.price,2))
    )
    sql = SqlFormatter().format_select(query)
    TestCase().assertEqual(sql, 'SELECT id,name,ROUND(price,2) AS price FROM ProductData')

def test_use_floor():
    query = QueryExpression('ProductData').select(
        lambda x:select(id=x.id,name=x.name,price=floor(x.price))
    )
    sql = SqlFormatter().format_select(query)
    TestCase().assertEqual(sql, 'SELECT id,name,FLOOR(price) AS price FROM ProductData')

def test_use_trim():
    query = QueryExpression('ProductData').select(
        lambda x:select(id=x.id,name=x.name,category=x.category.upper())
    )
    sql = SqlFormatter().format_select(query)
    TestCase().assertEqual(sql, 'SELECT id,name,UPPER(category) AS category FROM ProductData')

def test_use_trim():
    query = QueryExpression('ProductData').select(
        lambda x:select(id=x.id,name=x.name.strip(),category=x.category.upper())
    )
    sql = SqlFormatter().format_select(query)
    TestCase().assertEqual(sql, 'SELECT id,TRIM(name) AS name,UPPER(category) AS category FROM ProductData')

def test_use_datetime_year():
    query = QueryExpression('ProductData').select(
        lambda x:select(id=x.id,name=x.name,releaseYear=x.releaseDate.year)
    )
    sql = SqlFormatter().format_select(query)
    TestCase().assertEqual(sql, 'SELECT id,name,YEAR(releaseDate) AS releaseYear FROM ProductData')

