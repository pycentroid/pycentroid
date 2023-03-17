import pytest
from unittest import TestCase
from themost_framework.query import LamdaParser, QueryExpression, QueryField
from dill.source import getsource
import ast

def test_func():

    func = lambda x,category: x.category == category and x.price + 100 > 900
    # get module
    module:ast.Module = ast.parse(getsource(func).strip())
    # get function
    body: ast.Assign = module.body[0]
    # get arguments
    args = body.value.args.args;
    params = {
        'category': 'Laptops'
    }
    TestCase().assertEqual(params.get('category'), 'Laptops')
    TestCase().assertEqual('category' in params, True)
    TestCase().assertEqual('otherCategory' in params, False)

def test_parse_filter():
    parser = LamdaParser()
    result = parser.parse_filter(lambda x: x.category == 'Laptops' and x.price > 900)
    TestCase().assertEqual(result, {
        '$and': [
            {
                '$eq': [
                    '$category', 'Laptops'
                ]
            },
            {
                '$gt': [
                    '$price', 900
                ]
            }
        ]
    })

def test_parse_or():
    parser = LamdaParser()
    result = parser.parse_filter(lambda x: x.category == 'Laptops' or x.category == 'Desktops')
    TestCase().assertEqual(result, {
        '$or': [
            {
                '$eq': [
                    '$category', 'Laptops'
                ]
            },
            {
                '$eq': [
                    '$category', 'Desktops'
                ]
            }
        ]
    })

def test_parse_with_params():
    parser = LamdaParser()
    result = parser.parse_filter(lambda x, category, otherCategory: x.category == category or x.category == otherCategory, {
        'category': 'Laptops',
        'otherCategory': 'Desktops'
    })
    TestCase().assertEqual(result, {
        '$or': [
            {
                '$eq': [
                    '$category', 'Laptops'
                ]
            },
            {
                '$eq': [
                    '$category', 'Desktops'
                ]
            }
        ]
    })