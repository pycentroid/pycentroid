import pytest
from themost_framework.sqlite import SqliteAdapter
from themost_framework.common import object
from themost_framework.query import DataColumn, QueryEntity, QueryExpression, select, TestUtils
from unittest import TestCase
import re

Products = QueryEntity('ProductData')
People = QueryEntity('PersonData')
PostalAddresses = QueryEntity('PostalAddressData', 'address')
db = None

@pytest.fixture()
def db() -> SqliteAdapter:
    return SqliteAdapter(object(database='tests/db/local.db'))

def test_select_and_join(db):
    items = db.execute(QueryExpression(People).select(
        lambda x,address: (x.id, x.familyName, x.givenName, address.addressLocality)
    ).join(PostalAddresses).on(
        lambda x,address: x.address == address.id
        ))
    TestCase().assertGreater(len(items), 0)
