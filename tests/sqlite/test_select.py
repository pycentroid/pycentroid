from unittest import TestCase

import pytest

from pycentroid.common import AnyObject
from pycentroid.query import QueryEntity, QueryExpression
from pycentroid.sqlite import SqliteAdapter
from os.path import abspath, join, dirname

Products = QueryEntity('ProductData')
People = QueryEntity('PersonData')
Orders = QueryEntity('OrderData')
PostalAddresses = QueryEntity('PostalAddressData', 'address')
Customers = QueryEntity('PersonData', 'customer')


@pytest.fixture()
def db() -> SqliteAdapter:
    return SqliteAdapter(AnyObject(database=abspath(join(dirname(__file__), '../db/local.db'))))


async def test_select_and_join(db):
    items = await db.execute(QueryExpression(People).select(
        lambda x, address: (x.id, x.familyName, x.givenName, address.addressLocality)
    ).join(PostalAddresses).on(
        lambda x, address: x.address == address.id
    ))
    TestCase().assertGreater(len(items), 0)


async def test_select_with_nested_filter(db):
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
    items = await db.execute(query)
    TestCase().assertGreater(len(items), 0)
