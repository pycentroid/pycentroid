import pytest
from unittest import TestCase
from themost_framework.query.query_entity import QueryEntity
from themost_framework.query.open_data_query import any, OpenDataQueryExpression
from themost_framework.query.open_data_formatter import OpenDataFormatter

def test_get_any_expression():
    expr = any(lambda x: (x.customer,))
    TestCase().assertIsNotNone(expr)
    TestCase().assertEqual(expr.__collection__, {
        'customer': 1
    })

def test_get_any_nested_expression():
    expr = any(lambda x: (x.customer.address,))
    TestCase().assertIsNotNone(expr)
    TestCase().assertEqual(expr.__collection__, {
        'customer': 1
    })
    TestCase().assertGreater(len(expr.__expand__), 0)
    TestCase().assertEqual(expr.__expand__[0].__collection__, {
        'address': 1
    })

def test_expand_expr():
    Orders = QueryEntity('Orders')
    query = OpenDataQueryExpression(Orders).expand(lambda x: (x.customer.address,))
    TestCase().assertIsNotNone(query)
    expr = OpenDataFormatter().format_select(query)
    TestCase().assertEqual(expr, {
        '$expand': 'customer($expand=address)'
    })