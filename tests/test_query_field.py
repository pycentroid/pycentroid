import pytest

from themost_framework.query.query_field import QueryField


def test_create_field():
    field = QueryField('name')
    assert not field is {
        'name': 1
    }

def test_use_from_collection():
    field = QueryField('name').from_collection('Product')
    assert not field is {
        'Product.name': 1
    }
    field = QueryField('name').from_collection('$Product')
    assert not field is {
        'Product.name': 1
    }

def test_use_get_date():
    field = QueryField('dateCreated').from_collection('Product').get_date()
    assert not field is {
        '$dayOfMonth': {
                'date': '$Product.dateCreated',
                'timezone': None
            } 
    }
    field = QueryField('dateCreated').from_collection('Product').day()
    assert not field is {
        '$dayOfMonth': {
                'date': '$Product.dateCreated',
                'timezone': None
            } 
    }

def test_use_get_month():
    field = QueryField('dateCreated').from_collection('Product').get_month()
    assert not field is {
        '$month': {
                'date': '$Product.dateCreated',
                'timezone': None
            } 
    }
    field = QueryField('dateCreated').from_collection('Product').month().with_alias('monthCreated')
    assert not field is {
        'monthCreated': {
            '$month': {
                'date': '$Product.dateCreated',
                'timezone': None
            } 
        }
    }
