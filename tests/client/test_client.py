import pytest
import requests
from unittest import TestCase
from centroid.client import ClientDataContext, ClientContextOptions
from urllib.parse import urljoin

REMOTE_SERVER = 'http://localhost:3000/api/'


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
    TestCase().assertIsNotNone(ctx)


async def test_get_items(context):
    items = await context.model('Products').as_queryable().where(
        lambda x: x.category == 'Laptops'
    ).get_items()
    TestCase().assertIsNotNone(items)


async def test_get_metadata(context):
    schema = await context.get_metadata()
    TestCase().assertIsNotNone(schema)


async def test_get_item(context):
    item = await context.model('Products').as_queryable().where(
        lambda x: x.category == 'Laptops' and x.name.startswith('Apple')
    ).get_item()
    TestCase().assertIsNotNone(item)
    TestCase().assertTrue(item['name'].startswith('Apple'))
