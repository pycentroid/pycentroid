from collections import namedtuple
from dataclasses import dataclass
from typing import List, Optional
from urllib.parse import urljoin
from pycentroid.query import select, count
from pycentroid.common import year, AnyDict
import typing_extensions

import pytest
import requests
import logging


from pycentroid.client import ClientDataContext, ClientContextOptions

REMOTE_SERVER = 'http://localhost:3000/api/'
__popen__ = None


@dataclass
class Product:
    """Any offered product or service. For example: a pair of shoes; a concert ticket;"""
    id: Optional[int]
    name: str
    model: str


@pytest.fixture()
def context() -> ClientDataContext:
    url = urljoin(REMOTE_SERVER, '/auth/token')
    data = {
        'client_id': '9165351833584149',
        'client_secret': 'hTgqFBUhCfHs/quf/wnoB+UpDSfUusKA',
        'username': 'alexis.rees@example.com',
        'password': 'secret',
        'grant_type': 'password',
        'scope': 'profile'
    }
    response = requests.post(url=url, data=data)
    context = ClientDataContext(ClientContextOptions(REMOTE_SERVER))
    token = response.json()
    context.service.set('Authorization', 'Bearer ' + token.get('access_token'))
    return context


def test_context():
    ctx = ClientDataContext(ClientContextOptions(REMOTE_SERVER))
    assert ctx is not None


async def test_get_items(context):
    items = await context.model('Products').as_queryable().where(
        lambda x: x.category == 'Laptops'
    ).get_items()
    assert items is not None
    assert len(items) > 0


async def test_get_items_with_query(context):
    items = await context.model('Products').as_queryable().select(
        lambda x: select(name=x.name, price=x.price)
    ).where(
        lambda x: x.category == 'Laptops'
    ).get_items()
    assert items is not None
    assert len(items) > 0

def filterable(func):
    return func


async def test_where(context):
    items = await context.model('Orders').as_queryable().where(
        where = lambda x, orderStatus: x.orderStatus.alternateName == orderStatus, orderStatus = 'OrderPickup'
        ).take(10).get_items()
    assert items is not None
    assert len(items) > 0
    for item in items:
        assert item.get('orderStatus').get('alternateName') == 'OrderPickup'

async def test_select_nested_attrs(context):
    items = await context.model('Orders').as_queryable().select(
        lambda x: select(id=x.id, product=x.orderedItem.name, orderStatus=x.orderStatus.alternateName, orderDate=x.orderDate)
    ).where(
        where = lambda x, orderStatus: x.orderStatus.alternateName == orderStatus, orderStatus = 'OrderPickup'
        ).take(10).get_items()
    assert items is not None
    assert len(items) > 0
    logging.debug(items)
    for item in items:
        assert item.get('orderStatus') == 'OrderPickup'

async def test_select_as_array(context):
    items = await context.model('Products').as_queryable().select(
        lambda x: [x.id, x.name, x.model, x.category]
    ).where(
        where = lambda x, category: x.category == category, category = 'Laptops'
        ).take(10).get_items()
    assert items is not None
    assert len(items) > 0
    logging.debug(items)
    for item in items:
        assert item.get('category') == 'Laptops'

async def test_where_with_def(context):

    def filter(x, orderStatus):
        return x.orderStatus.alternateName == orderStatus;

    items = await context.model('Orders').as_queryable().where(
        where = filter, orderStatus = 'OrderPickup'
        ).take(10).get_items()
    assert len(items) > 0
    for item in items:
        assert item.get('orderStatus').get('alternateName') == 'OrderPickup'


async def test_select_attrs_with_alias(context):
    items: List = await context.model('Products').as_queryable().select(
        lambda x: select(id=x.id, name=x.name, product_model=x.model,)
    ).where(
        lambda x: x.category == 'Laptops'
    ).get_items()
    assert items is not None
    assert len(items) > 0
    Product = namedtuple('Product', ['id', 'name', 'product_model'])
    for item in items:
        obj = Product(**item)
        assert obj.product_model is not None


async def test_select_attrs(context):
    items: List = await context.model('Orders').as_queryable().select(
        lambda x: select(id=x.id,
                         customer=x.customer.description,
                         paymentMethod=x.paymentMethod.alternateName,
                         orderDate=x.orderDate,
                         product=x.orderedItem.name,)
    ).where(
        lambda x: x.paymentMethod.alternateName == 'DirectDebit'
    ).take(10).get_items()
    assert items is not None
    assert len(items) == 10
    keys = list(items[0].keys())
    assert keys == [
        'id',
        'customer',
        'paymentMethod',
        'orderDate',
        'product'
    ]
    for item in items:
        assert item.get('paymentMethod') == 'DirectDebit'


async def test_filter_with_params(context):

    where_stmt = lambda x, category: x.category == category
    items: List = await (context.model('Products').as_queryable().select(
        lambda x: select(id=x.id,
                         name=x.name,
                         category=x.category,
                         releaseYear=year(x.releaseDate),
                         price=round(x.price, 2),)
    ).where(
        where_stmt,
        category='Desktops'
    ).take(10).get_items())
    assert items is not None
    assert len(items) == 10
    for item in items:
        assert item.get('category') == 'Desktops'


async def test_select_expr(context):
    items: List = await context.model('Products').as_queryable().select(
        lambda x: (x.id, x.name, x.model,)
    ).where(
        lambda x: x.category == 'Laptops'
    ).get_items()
    assert items is not None
    assert len(items) > 0
    for item in items:
        obj = Product(**item)
        assert obj.name is not None

async def test_order_by_items(context):
    items = await context.model('Products').as_queryable().select(
        lambda x: (x.id, x.name, x.model, x.price)
    ).where(
        lambda x: x.category == 'Laptops'
        ).order_by(
            lambda x: [round(x.price, 2)]
        ).take(10).get_items()
    assert items is not None
    assert len(items) > 0
    for index, item in enumerate(items):
        if index > 0:
            current = item.get('price')
            previous = items[index - 1].get('price')
            assert current >= previous

async def test_then_by_descnding(context):
    items = await context.model('Products').as_queryable().select(
        lambda x: (x.id, x.name, x.model, x.price, x.releaseDate)
    ).where(
        lambda x: x.category == 'Laptops'
        ).order_by(
            lambda x: (round(x.price, 2),)
        ).then_by_descending(
            lambda x: (x.releaseDate,)
        ).take(10).get_items()
    assert items is not None
    assert len(items) > 0
    for index, item in enumerate(items):
        if index > 0:
            current = item.get('price')
            previous = items[index - 1].get('price')
            assert current >= previous

async def test_limit_results(context):
    items = await context.model('Products').as_queryable().select(
        lambda x: (x.id, x.name, x.model, x.price, x.releaseDate)
    ).where(
        lambda x: x.category == 'Laptops'
        ).order_by(
            lambda x: (round(x.price, 2),)
        ).take(5).get_items()
    assert items is not None
    assert len(items) > 0

    next_items = await context.model('Products').as_queryable().select(
        lambda x: (x.id, x.name, x.model, x.price, x.releaseDate)
    ).where(
        lambda x: x.category == 'Laptops'
        ).order_by(
            lambda x: (round(x.price, 2),)
        ).take(5).skip(5).get_items()

    assert next_items is not None
    assert len(items) > 0

    for index, item in enumerate(next_items):
        found = next(filter(lambda x: x.get('id') == item.get('id'), items), None)
        assert found is None

async def test_count_results(context):
    result = await context.model('Products').as_queryable().select(
        lambda x: (x.id, x.name, x.model, x.price, x.releaseDate)
    ).where(
        lambda x: x.category == 'Laptops'
        ).order_by(
            lambda x: (round(x.price, 2),)
        ).take(5).get_list();
    assert result is not None
    assert result is not None

async def test_group_by_results(context):
    items = await context.model('Products').as_queryable().select(
        lambda x: select(category=x.category, total=count(x.id))
    ).group_by(
        lambda x: (x.category,)
    ).get_items()
    assert items is not None
    assert len(items) > 0


async def test_get_metadata(context):
    schema = await context.get_metadata()
    assert schema is not None


async def test_get_item(context):
    item = await context.model('Products').as_queryable().where(
        lambda x: x.category == 'Laptops' and x.name.startswith('Apple')
    ).get_item()
    assert item is not None
    assert item['name'].startswith('Apple')
