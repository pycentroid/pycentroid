# noinspection PyUnresolvedReferences
from pycentroid.query.query_entity import QueryEntity
from pycentroid.query.open_data_query import any, OpenDataQueryExpression
from pycentroid.query.open_data_formatter import OpenDataFormatter


def test_get_any_expression():
    expr = any(lambda x: (x.customer,))
    assert expr is not None
    assert expr.__collection__ == {
        'customer': 1
    }


def test_expand_expr():
    # noinspection PyPep8Naming
    Orders = QueryEntity('Orders')
    query = OpenDataQueryExpression(Orders).expand(lambda x: (x.customer.address,))
    assert query is not None
    expr = OpenDataFormatter().format_select(query)
    assert expr == {
        '$expand': 'customer($expand=address)'
    }


# noinspection PyPep8Naming
def test_expand_multiple_expr():
    Orders = QueryEntity('Orders')
    query = OpenDataQueryExpression(Orders).expand(
        lambda x: (x.customer.address,)
    ).expand(
        lambda x: (x.orderedItem,)
    )
    assert query is not None
    expr = OpenDataFormatter().format_select(query)
    assert expr == {
        '$expand': 'customer($expand=address),orderedItem'
    }


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
    assert query is not None
    expr = OpenDataFormatter().format_select(query)
    assert expr == {
        '$expand': 'customer($expand=address($select=mobile))'
    }
