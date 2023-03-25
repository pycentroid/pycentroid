import pytest
from unittest import TestCase
from themost_framework.query import LamdaParser, QueryExpression, QueryField, select
from dill.source import getsource
import ast

def test_select_func():

    func = lambda x: select( productName = x.name, productCategory = x.category)
    # get module
    module:ast.Module = ast.parse(getsource(func).strip())
    # get function
    body: ast.Assign = module.body[0]
    TestCase().assertEqual(type(body.value.body), ast.Call)
    # get arguments
    selectFunc = body.value.body.func;
    TestCase().assertEqual(type(selectFunc), ast.Name)
    TestCase().assertEqual(selectFunc.id, 'select')

    selectArgs = body.value.body.keywords;
    TestCase().assertEqual(type(selectArgs), list)

    arg0 = selectArgs[0]
    TestCase().assertEqual(type(arg0), ast.keyword)
    TestCase().assertEqual(arg0.arg, 'productName')
    TestCase().assertEqual(type(arg0.value), ast.Attribute)
    
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

def test_sequence_list():

    func = lambda x: [ x.id, x.name, x.category, x.releaseDate, x.price ]
    # get module
    module:ast.Module = ast.parse(getsource(func).strip())
    # get function
    body: ast.Assign = module.body[0]
    # get attribute
    select: ast.List = body.value.body;
    TestCase().assertEqual(type(select), ast.List)
    for attribute in select.elts:
        TestCase().assertEqual(type(attribute), ast.Attribute)

def test_sequence_dict():

    func = lambda x: { 'id': x.id, 'name': x.name, 'price': x.price }
    # get module
    module:ast.Module = ast.parse(getsource(func).strip())
    # get function
    body: ast.Assign = module.body[0]
    # get attribute
    select: ast.List = body.value.body;
    TestCase().assertEqual(type(select), ast.Dict)
    for attribute in select.keys:
        TestCase().assertEqual(type(attribute), ast.Constant)
    for attribute in select.values:
        TestCase().assertEqual(type(attribute), ast.Attribute)

def test_parse_filter():
    parser = LamdaParser()
    result = parser.parse_filter(lambda x: x.category == 'Laptops' 
        and x.price > 900)
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
    result = parser.parse_filter(lambda x,category,otherCategory: x.category == category or x.category == otherCategory, {
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

def test_parse_sequence():
    parser = LamdaParser()
    result = parser.parse_select(lambda x: [ x.id, x.name, x.price ])
    TestCase().assertEqual(result, {
        'id': 1,
        'name': 1,
        'price': 1
    })

    result = parser.parse_select(lambda x: [ x.id, x.givenName, x.familyName, x.address.streetAddress ])
    TestCase().assertEqual(result, {
        'id': 1,
        'givenName': 1,
        'familyName': 1,
        'address.streetAddress': 1
    })