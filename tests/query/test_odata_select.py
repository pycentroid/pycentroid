# noinspection PyUnresolvedReferences
from unittest import TestCase
from pycentroid.query.open_data_parser import OpenDataParser


def test_parse_select():
    parser = OpenDataParser()
    expr = parser.parse_select_sequence('id,name,category,price')
    TestCase().assertEqual(expr, [
        {'$id': 1},
        {'$name': 1},
        {'$category': 1},
        {'$price': 1}
    ])


def test_parse_select_with_alias():
    parser = OpenDataParser()
    expr = parser.parse_select_sequence('id,name,category as productCategory,price')
    TestCase().assertEqual(expr, [
        {'$id': 1},
        {'$name': 1},
        {'productCategory': '$category'},
        {'$price': 1}
    ])


def test_parse_select_with_func():
    parser = OpenDataParser()
    expr = parser.parse_select_sequence('id,name,category,round(price,2) as price,year(dateReleased) as yearReleased')
    TestCase().assertEqual(expr, [
        {'$id': 1},
        {'$name': 1},
        {'$category': 1},
        {'price': {
            '$round': [
                '$price',
                2
            ]
        }
        },
        {'yearReleased': {
            '$year': [
                '$dateReleased'
            ]
        }
        }
    ])


def test_parse_order_by():
    parser = OpenDataParser()
    expr = parser.parse_order_by_sequence('price asc,name desc')
    TestCase().assertEqual(expr, [
        {'$expr': '$price', 'direction': 'asc'},
        {'$expr': '$name', 'direction': 'desc'}
    ])


def test_parse_select_concat():
    expr = OpenDataParser().parse_select_sequence('concat(concat(person/givenName,\' \'),person/familyName) as name')
    TestCase().assertEqual(expr,
                           [
                               {
                                   'name': {
                                       '$concat': [
                                           {
                                               '$concat': [
                                                   '$person.givenName',
                                                   ' '
                                               ]
                                           },
                                           '$person.familyName'
                                       ]
                                   }
                               }

                           ]
                           )
