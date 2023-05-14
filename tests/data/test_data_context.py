import pytest
from pycentroid.data.application import DataApplication
from os.path import abspath, join, dirname

APP_PATH = abspath(join(dirname(__file__), '..'))


@pytest.fixture()
def app() -> DataApplication:
    return DataApplication(cwd=APP_PATH)


def test_create_context(app):
    context = app.create_context()
    assert context.application is not None
    assert context.db is not None
    context.finalize()


def test_get_model(app):
    context = app.create_context()
    model = context.model('Product')
    assert model is not None
    assert model.properties.name == 'Product'
    context.finalize()
