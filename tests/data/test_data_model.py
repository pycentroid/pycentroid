import pytest
from centroid.data.loaders import SchemaLoaderStrategy
from centroid.data.application import DataApplication
from centroid.data.types import DataModelProperties
from centroid.data.model import DataModel
from centroid.data.configuration import DataConfiguration
from centroid.common.objects import AnyObject
from os.path import abspath, join, dirname
from unittest import TestCase

APP_PATH = abspath(join(dirname(__file__), '..'))

@pytest.fixture()
def app() -> DataApplication:
    return DataApplication(cwd=APP_PATH)

def test_get_model(app):
    model = app.configuration.getstrategy(SchemaLoaderStrategy).get('Thing')
    TestCase().assertEqual(model.name, 'Thing')

def test_get_attributes(app):
    context = app.create_context()
    model: DataModel = context.model('Product')
    TestCase().assertIsNotNone(model)
    TestCase().assertIsNotNone(model.attributes)
    category = next(filter(lambda x: x.name=='category', model.attributes), None)
    TestCase().assertIsNotNone(category)
    TestCase().assertEqual(category.model, 'Product')
    name = next(filter(lambda x: x.name=='name', model.attributes), None)
    TestCase().assertIsNotNone(category)
    TestCase().assertEqual(name.model, 'Thing')
    context.finalize()

def test_get_primary_key(app):
    context = app.create_context()
    model: DataModel = context.model('Product')
    TestCase().assertIsNotNone(model)
    TestCase().assertIsNotNone(model.attributes)
    attr = next(filter(lambda x: x.primary is True, model.attributes), None)
    TestCase().assertIsNotNone(attr)
    context.finalize()

def test_get_source(app):
    model = DataModelProperties(name='TestAction')
    TestCase().assertEqual(model.get_source(), 'TestActionBase')
    TestCase().assertEqual(model.get_view(), 'TestActionView')
    TestCase().assertEqual(model, {
        'name': 'TestAction'
    })
    model = DataModelProperties(name='TestAction', source='TestActions')
    TestCase().assertEqual(model.get_source(), 'TestActions')
    model.source = 'TestActionBase'

    

async def test_upgrade(app):
    context = app.create_context()
    model: DataModel = context.model('InteractAction')
    await model.migrate()
    context.finalize()




