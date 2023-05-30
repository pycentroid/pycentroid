# noinspection PyUnresolvedReferences
from pycentroid.query.query_field import QueryField


def test_create_field():
    field = QueryField('name')
    assert field == {
        'name': 1
    }


def test_use_from_collection():
    field = QueryField('name').from_collection('Product')
    assert field == {
        'Product.name': 1
    }
    field = QueryField('name').from_collection('$Product')
    assert field == {
        'Product.name': 1
    }


def test_use_get_date():
    field = QueryField('dateCreated').from_collection('Product').get_date()
    assert field == {
        '$dayOfMonth': {
            'date': '$Product.dateCreated',
            'timezone': None
        }
    }
    field = QueryField('dateCreated').from_collection('Product').day()
    assert field == {
        '$dayOfMonth': {
            'date': '$Product.dateCreated',
            'timezone': None
        }
    }


def test_use_get_month():
    field = QueryField('dateCreated').from_collection('Product').get_month()
    assert field == {
        '$month': {
            'date': '$Product.dateCreated',
            'timezone': None
        }
    }

    field = QueryField('dateCreated').from_collection('Product').month()._as('monthCreated')
    assert field == {
        'monthCreated': {
            '$month': {
                'date': '$Product.dateCreated',
                'timezone': None
            }
        }
    }


def test_use_get_year():
    field = QueryField('dateCreated').from_collection('Product').get_year()
    assert field == {
        '$year': {
            'date': '$Product.dateCreated',
            'timezone': None
        }
    }
    field = QueryField('dateCreated').from_collection('Product').year()._as('yearCreated')
    assert field == {
        'yearCreated': {
            '$year': {
                'date': '$Product.dateCreated',
                'timezone': None
            }
        }
    }


def test_use_get_hours():
    field = QueryField('dateCreated').from_collection('Product').get_hours()
    assert field == {
        '$hour': {
            'date': '$Product.dateCreated',
            'timezone': None
        }
    }
    field = QueryField('dateCreated').from_collection('Product').hour()._as('hourCreated')
    assert field == {
        'hourCreated': {
            '$hour': {
                'date': '$Product.dateCreated',
                'timezone': None
            }
        }
    }


def test_use_get_minutes():
    field = QueryField('dateCreated').from_collection('Product').get_minutes()
    assert field == {
        '$minute': {
            'date': '$Product.dateCreated',
            'timezone': None
        }
    }
    field = QueryField('dateCreated').from_collection('Product').minute()._as('minuteCreated')
    assert field == {
        'minuteCreated': {
            '$minute': {
                'date': '$Product.dateCreated',
                'timezone': None
            }
        }
    }


def test_use_get_seconds():
    field = QueryField('dateCreated').from_collection('Product').get_seconds()
    assert field == {
        '$second': {
            'date': '$Product.dateCreated',
            'timezone': None
        }
    }
    field = QueryField('dateCreated').from_collection('Product').second()._as('secondCreated')
    assert field == {
        'secondCreated': {
            '$second': {
                'date': '$Product.dateCreated',
                'timezone': None
            }
        }
    }


def test_use_add():
    field = QueryField('price').from_collection('Product').add(100)._as('discountPrice')
    assert field == {
        'discountPrice': {
            '$add': [
                '$Product.price',
                100
            ]
        }
    }


def test_use_multiply():
    field = QueryField('price').from_collection('Product').multiply(0.75).asattr('discountPrice')
    assert field == {
        'discountPrice': {
            '$multiply': [
                '$Product.price',
                0.75
            ]
        }
    }


def test_use_concat():
    field = QueryField('familyName').from_collection('Person').concat(
        ' ',
        QueryField('givenName').from_collection('Person')
    ).asattr('name')
    assert field == {
        'name': {
            '$concat': [
                '$Person.familyName',
                ' ',
                '$Person.givenName'
            ]
        }
    }
