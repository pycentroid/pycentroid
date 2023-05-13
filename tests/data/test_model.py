import pytest
from pycentroid.data.loaders import SchemaLoaderStrategy
from pycentroid.data.application import DataApplication
from pycentroid.data.types import DataModelProperties
from pycentroid.data.model import DataModel
from pycentroid.data.context import DataContext
from os.path import abspath, join, dirname
from unittest import TestCase
from pycentroid.query import TestUtils


APP_PATH = abspath(join(dirname(__file__), '..'))


@pytest.fixture()
def context() -> DataContext:
    app = DataApplication(cwd=APP_PATH)
    return app.create_context()


def test_get_model(context):
    model = context.application.configuration.getstrategy(SchemaLoaderStrategy).get('Thing')
    TestCase().assertEqual(model.name, 'Thing')


def test_get_attributes(context):
    model: DataModel = context.model('Product')
    TestCase().assertIsNotNone(model)
    TestCase().assertIsNotNone(model.attributes)
    category = next(filter(lambda x: x.name == 'category', model.attributes), None)
    TestCase().assertIsNotNone(category)
    TestCase().assertEqual(category.model, 'Product')
    name = next(filter(lambda x: x.name == 'name', model.attributes), None)
    TestCase().assertIsNotNone(category)
    TestCase().assertEqual(name.model, 'Thing')
    context.finalize()


def test_get_primary_key(context):
    model: DataModel = context.model('Product')
    TestCase().assertIsNotNone(model)
    TestCase().assertIsNotNone(model.attributes)
    attr = next(filter(lambda x: x.primary is True, model.attributes), None)
    TestCase().assertIsNotNone(attr)
    context.finalize()


def test_get_source():
    model = DataModelProperties(name='TestAction')
    TestCase().assertEqual(model.get_source(), 'TestActionBase')
    TestCase().assertEqual(model.get_view(), 'TestActionData')
    TestCase().assertEqual(model, {
        'name': 'TestAction'
    })
    model = DataModelProperties(name='TestAction', source='TestActions')
    TestCase().assertEqual(model.get_source(), 'TestActions')
    model.source = 'TestActionBase'


async def test_upgrade(context):
    
    async def execute():
        model: DataModel = context.model('InteractAction')
        await model.migrate()
    await TestUtils(context.db).execute_in_transaction(execute)
    await context.finalize()


def test_many_to_one(context):
    
    mapping = context.model('Action').infermapping('actionStatus')
    TestCase().assertIsNotNone(mapping)
    TestCase().assertEqual(mapping.associationType, 'association')

    mapping = context.model('InteractAction').infermapping('actionStatus')
    TestCase().assertIsNotNone(mapping)
    TestCase().assertEqual(mapping.associationType, 'association')
    TestCase().assertEqual(mapping.childModel, 'InteractAction')


def test_many_to_many(context):
    
    mapping = context.model('AuthClient').infermapping('scopes')
    TestCase().assertIsNotNone(mapping)
    TestCase().assertEqual(mapping.associationType, 'junction')
