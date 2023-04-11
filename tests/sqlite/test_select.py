from unittest import TestCase

import pytest

from atmost.common import object
from atmost.query import QueryEntity, QueryExpression
from atmost.sqlite import SqliteAdapter
from os.path import abspath, join, dirname

Products = QueryEntity('ProductData')
People = QueryEntity('PersonData')
Orders = QueryEntity('OrderData')
PostalAddresses = QueryEntity('PostalAddressData', 'address')
Customers = QueryEntity('PersonData', 'customer')


@pytest.fixture()
def db() -> SqliteAdapter:
    return SqliteAdapter(object(database=abspath(join(dirname(__file__), '../db/local.db'))))


def test_select_and_join(db):
    items = db.execute(QueryExpression(People).select(
        lambda x, address: (x.id, x.familyName, x.givenName, address.addressLocality)
    ).join(PostalAddresses).on(
        lambda x, address: x.address == address.id
    ))
    TestCase().assertGreater(len(items), 0)


def test_select_with_nested_filter(db):
    query = QueryExpression().select(
        lambda x: (x.orderedItem,
                   x.customer.familyName,
                   x.customer.givenName,
                   x.customer.address.addressLocality)
    ).from_collection(Orders).join(Customers, 'customer').on(
        lambda x, customer: x.customer == customer.id
    ).join(PostalAddresses, 'address').on(
        lambda x, address: x.customer.address == address.id
    ).where(
        lambda x: x.orderStatus == 1
    )
    items = db.execute(query)
    TestCase().assertGreater(len(items), 0)
