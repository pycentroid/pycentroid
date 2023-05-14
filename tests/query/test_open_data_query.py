# noinspection PyUnresolvedReferences
from unittest import TestCase
from pycentroid.query.query_entity import QueryEntity
from pycentroid.query.open_data_query import any, OpenDataQueryExpression
from pycentroid.query.open_data_formatter import OpenDataFormatter


def test_get_any_expression():
    expr = any(lambda x: (x.customer,))
    TestCase().assertIsNotNone(expr)
    TestCase().assertEqual(expr.__collection__, {
        'customer': 1
    })


def test_expand_expr():
    # noinspection PyPep8Naming
    Orders = QueryEntity('Orders')
    query = OpenDataQueryExpression(Orders).expand(lambda x: (x.customer.address,))
    TestCase().assertIsNotNone(query)
    expr = OpenDataFormatter().format_select(query)
    TestCase().assertEqual(expr, {
        '$expand': 'customer($expand=address)'
    })


# noinspection PyPep8Naming
def test_expand_multiple_expr():
    Orders = QueryEntity('Orders')
    query = OpenDataQueryExpression(Orders).expand(
        lambda x: (x.customer.address,)
    ).expand(
        lambda x: (x.orderedItem,)
    )
    TestCase().assertIsNotNone(query)
    expr = OpenDataFormatter().format_select(query)
    TestCase().assertEqual(expr, {
        '$expand': 'customer($expand=address),orderedItem'
    })


def test_expand_with_select():
    # noinspection PyPep8Naming
    Orders = QueryEntity('Orders')
    query = OpenDataQueryExpression(Orders).expand(
        any(
            lambda x: (x.customer.address,)
        ).select(
            lambda y: (y.mobile,)
        )
    )
    TestCase().assertIsNotNone(query)
    expr = OpenDataFormatter().format_select(query)
    TestCase().assertEqual(expr, {
        '$expand': 'customer($expand=address($select=mobile))'
    })
