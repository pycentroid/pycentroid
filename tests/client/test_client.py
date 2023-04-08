import pytest
import requests
from unittest import TestCase
from themost_framework.client import ClientContext, ClientContextOptions
from urllib.parse import urljoin

REMOTE_SERVER='http://localhost:3000/api/'

@pytest.fixture()
def context() -> ClientContext:
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
    context = ClientContext(ClientContextOptions(REMOTE_SERVER))
    token = response.json()
    context.service.set('Authorization', 'Bearer ' + token.get('access_token'))
    return context

def test_context():
    ctx = ClientContext(ClientContextOptions(REMOTE_SERVER))
    TestCase().assertIsNotNone(ctx)

def test_get_items(context):
    items = context.model('Products').as_queryable().where(
        lambda x: x.category == 'Laptops'
    ).get_items()
    TestCase().assertIsNotNone(items)