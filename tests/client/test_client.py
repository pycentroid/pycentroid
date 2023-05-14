import pytest
import requests
from pycentroid.client import ClientDataContext, ClientContextOptions
from urllib.parse import urljoin
from subprocess import Popen

REMOTE_SERVER = 'http://localhost:3000/api/'
__popen__ = None


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


async def test_get_metadata(context):
    schema = await context.get_metadata()
    assert schema is not None


async def test_get_item(context):
    item = await context.model('Products').as_queryable().where(
        lambda x: x.category == 'Laptops' and x.name.startswith('Apple')
    ).get_item()
    assert item is not None
    assert item['name'].startswith('Apple')
