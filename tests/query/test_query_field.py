# noinspection PyUnresolvedReferences
from unittest import TestCase
from pycentroid.query.query_field import QueryField


def test_create_field():
    field = QueryField('name')
    TestCase().assertEqual(field, {
        'name': 1
    })


def test_use_from_collection():
    field = QueryField('name').from_collection('Product')
    TestCase().assertEqual(field, {
        'Product.name': 1
    })
    field = QueryField('name').from_collection('$Product')
    TestCase().assertEqual(field, {
        'Product.name': 1
    })


def test_use_get_date():
    field = QueryField('dateCreated').from_collection('Product').get_date()
    TestCase().assertEqual(field, {
        '$dayOfMonth': {
            'date': '$Product.dateCreated',
            'timezone': None
        }
    })
    field = QueryField('dateCreated').from_collection('Product').day()
    TestCase().assertEqual(field, {
        '$dayOfMonth': {
            'date': '$Product.dateCreated',
            'timezone': None
        }
    })


def test_use_get_month():
    field = QueryField('dateCreated').from_collection('Product').get_month()
    TestCase().assertEqual(field, {
        '$month': {
            'date': '$Product.dateCreated',
            'timezone': None
        }
    })
    
    field = QueryField('dateCreated').from_collection('Product').month()._as('monthCreated')
    TestCase().assertEqual(field, {
        'monthCreated': {
            '$month': {
                'date': '$Product.dateCreated',
                'timezone': None
            }
        }
    })
    

def test_use_get_year():
    field = QueryField('dateCreated').from_collection('Product').get_year()
    TestCase().assertDictEqual(field, {
        '$year': {
            'date': '$Product.dateCreated',
            'timezone': None
        }
    })
    field = QueryField('dateCreated').from_collection('Product').year()._as('yearCreated')
    TestCase().assertDictEqual(field, {
        'yearCreated': {
            '$year': {
                'date': '$Product.dateCreated',
                'timezone': None
            }
        }
    })


def test_use_get_hours():
    field = QueryField('dateCreated').from_collection('Product').get_hours()
    TestCase().assertDictEqual(field, {
        '$hour': {
            'date': '$Product.dateCreated',
            'timezone': None
        }
    })
    field = QueryField('dateCreated').from_collection('Product').hour()._as('hourCreated')
    TestCase().assertDictEqual(field, {
        'hourCreated': {
            '$hour': {
                'date': '$Product.dateCreated',
                'timezone': None
            }
        }
    })


def test_use_get_minutes():
    field = QueryField('dateCreated').from_collection('Product').get_minutes()
    TestCase().assertDictEqual(field, {
        '$minute': {
            'date': '$Product.dateCreated',
            'timezone': None
        }
    })
    field = QueryField('dateCreated').from_collection('Product').minute()._as('minuteCreated')
    TestCase().assertDictEqual(field, {
        'minuteCreated': {
            '$minute': {
                'date': '$Product.dateCreated',
                'timezone': None
            }
        }
    })


def test_use_get_seconds():
    field = QueryField('dateCreated').from_collection('Product').get_seconds()
    TestCase().assertDictEqual(field, {
        '$second': {
            'date': '$Product.dateCreated',
            'timezone': None
        }
    })
    field = QueryField('dateCreated').from_collection('Product').second()._as('secondCreated')
    TestCase().assertDictEqual(field, {
        'secondCreated': {
            '$second': {
                'date': '$Product.dateCreated',
                'timezone': None
            }
        }
    })


def test_use_add():
    field = QueryField('price').from_collection('Product').add(100)._as('discountPrice')
    TestCase().assertDictEqual(field, {
        'discountPrice': {
            '$add': [
                '$Product.price',
                100
            ]
        }
    })


def test_use_multiply():
    field = QueryField('price').from_collection('Product').multiply(0.75).as_('discountPrice')
    TestCase().assertDictEqual(field, {
        'discountPrice': {
            '$multiply': [
                '$Product.price',
                0.75
            ]
        }
    })


def test_use_concat():
    field = QueryField('familyName').from_collection('Person').concat(
        ' ',
        QueryField('givenName').from_collection('Person')
    ).asattr('name')
    TestCase().assertDictEqual(field, {
        'name': {
            '$concat': [
                '$Person.familyName',
                ' ',
                '$Person.givenName'
            ]
        }
    })
