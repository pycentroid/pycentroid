import pytest
from unittest import TestCase

from atmost.query import QueryExpression, QueryField, select
from dill.source import getsource
import ast

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
        lambda x: select(id = x.id, name = x.name, category = x.category, price = x.price)
        ).where('category').not_equal('Laptops')
    
    TestCase().assertEqual(q.__select__, {
                'id': 1,
                'name': 1,
                'category': 1,
                'price':1
            })

    
    