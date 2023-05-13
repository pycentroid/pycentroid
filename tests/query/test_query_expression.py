from unittest import TestCase
from pycentroid.query import QueryExpression, QueryField, select, QueryEntity, SqlFormatter, SelectExpressionEncoder
import json


def test_create_expr():
    q = QueryExpression('Person')
    TestCase().assertEqual(q.__collection__, {
        'Person': 1
    })
    q.select('id', 'familyName', {
        'givenName': 1
    }, QueryField('dateCreated'))
    TestCase().assertEqual(q.__select__, {
        'id': 1,
        'familyName': 1,
        'givenName': 1,
        'dateCreated': 1
    })


def test_use_equal():
    q = QueryExpression('Person').select(
        'id', 'familyName', 'givenName'
    ).where('familyName').equal('Rees')

    TestCase().assertEqual(q.__where__, {
        '$eq': [
            '$familyName',
            'Rees'
        ]
    })


def test_use_and():
    q = QueryExpression('Person').select(
        'id', 'familyName', 'givenName'
    ).where('familyName').equal('Rees').and_also('givenName').equal('Alexis')

    TestCase().assertEqual(q.__where__, {
        '$and': [
            {
                '$eq': [
                    '$familyName',
                    'Rees'
                ]
            },
            {
                '$eq': [
                    '$givenName',
                    'Alexis'
                ]
            }
        ]
    })


def test_use_or():
    q = QueryExpression('Product').select(
        'name', 'releaseDate', 'price', 'category'
    ).where('category').equal('Laptops').or_else('category').equal('Desktops')

    TestCase().assertEqual(q.__where__, {
        '$or': [
            {
                '$eq': [
                    '$category',
                    'Laptops'
                ]
            },
            {
                '$eq': [
                    '$category',
                    'Desktops'
                ]
            }
        ]
    })


def test_use_not_equal():
    q = QueryExpression('Product').select(
        'name', 'releaseDate', 'price', 'category'
    ).where('category').not_equal('Laptops')

    TestCase().assertEqual(q.__where__, {
        '$ne': [
            '$category',
            'Laptops'
        ]
    })


def test_select_map():
    q = QueryExpression('Product').select(
        lambda x: select(id=x.id, name=x.name, category=x.category, price=x.price)
    ).where('category').not_equal('Laptops')

    TestCase().assertEqual(q.__select__, {
        'id': 1,
        'name': 1,
        'category': 1,
        'price': 1
    })


def test_where_with_query_field():

    products = QueryEntity('Product')
    q = QueryExpression(products).where(
        QueryField('category').from_collection(products.collection)
    ).equal('Desktops')
    TestCase().assertEqual(q.__where__, {
        '$eq': [
            '$Product.category',
            'Desktops'
        ]
    })
    formatter = SqlFormatter()

    sql = formatter.format_where(q.__where__)
    TestCase().assertEqual(sql, '(Product.category=\'Desktops\')')

    orders = QueryEntity('Order')
    q = QueryExpression(orders).where(
        QueryField('orderedItem').from_collection(orders.collection)
    ).equal(
        QueryField('id').from_collection(products.collection)
    )
    TestCase().assertEqual(q.__where__, {
        '$eq': [
            '$Order.orderedItem',
            '$Product.id'
        ]
    })
    sql = formatter.format_where(q.__where__)
    TestCase().assertEqual(sql, '(Order.orderedItem=Product.id)')
    

def test_encode_query():
    q = QueryExpression('Product').select(
        lambda x: select(id=x.id, name=x.name, category=x.category, price=x.price)
    ).where('category').not_equal('Laptops')
    expr = json.dumps(q, cls=SelectExpressionEncoder)
    TestCase().assertEqual(expr, json.dumps({
        '$project': {
            'id': 1,
            'name': 1,
            'category': 1,
            'price': 1
        },
        '$match': {
            '$ne': [
                '$category',
                'Laptops'
            ]
        }
    }))


def test_join_expression():
    q = QueryExpression('Order').select(
        lambda x: (x.id, x.customer,)
    ).join(
        QueryExpression('Person').select(
            lambda x: (x.id,)
        ).as_('customer')
    ).on(
        QueryExpression().where(
            QueryField('customer').from_collection('Order')
        ).equal(
            QueryField('id').from_collection('customer')
        )
    )
    sql = SqlFormatter().format(q)
    TestCase().assertEqual(sql, 'SELECT Order.id,Order.customer FROM Order '
                                'INNER JOIN (SELECT id FROM Person) customer ON (Order.customer=customer.id)')
    
    Person = QueryEntity('Person')
    Order = QueryEntity('Order')
    q1 = QueryExpression(Person, alias='customer').select(
        lambda x: (x.id,)
        ).take(10)
    q = QueryExpression(Order).select(
        lambda x: (x.id, x.customer,)
    ).join(q1).on(
        lambda x, customer: x.customer == customer.id
        )
    sql = SqlFormatter().format(q)
    TestCase().assertEqual(sql, 'SELECT Order.id,Order.customer FROM Order '
                                'INNER JOIN (SELECT id FROM Person LIMIT 10) customer ON (Order.customer=customer.id)')
