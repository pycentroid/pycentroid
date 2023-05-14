import pytest
from pycentroid.data.loaders import SchemaLoaderStrategy
from pycentroid.data.application import DataApplication
from pycentroid.data.types import DataModelProperties
from pycentroid.data.model import DataModel
from pycentroid.data.context import DataContext
from os.path import abspath, join, dirname
from pycentroid.query import TestUtils


APP_PATH = abspath(join(dirname(__file__), '..'))


@pytest.fixture()
def context() -> DataContext:
    app = DataApplication(cwd=APP_PATH)
    return app.create_context()


def test_get_model(context):
    model = context.application.configuration.getstrategy(SchemaLoaderStrategy).get('Thing')
    assert model.name == 'Thing'


def test_get_attributes(context):
    model: DataModel = context.model('Product')
    assert model is not None
    assert model.attributes is not None
    category = next(filter(lambda x: x.name == 'category', model.attributes), None)
    assert category is not None
    assert category.model == 'Product'
    name = next(filter(lambda x: x.name == 'name', model.attributes), None)
    assert category is not None
    assert name.model == 'Thing'
    context.finalize()


def test_get_primary_key(context):
    model: DataModel = context.model('Product')
    assert model is not None
    assert model.attributes is not None
    attr = next(filter(lambda x: x.primary is True, model.attributes), None)
    assert attr is not None
    context.finalize()


def test_get_source():
    model = DataModelProperties(name='TestAction')
    assert model.get_source() == 'TestActionBase'
    assert model.get_view() == 'TestActionData'
    assert model == {
        'name': 'TestAction'
    }
    model = DataModelProperties(name='TestAction', source='TestActions')
    assert model.get_source() == 'TestActions'
    model.source = 'TestActionBase'


async def test_upgrade(context):

    async def execute():
        model: DataModel = context.model('InteractAction')
        await model.migrate()
        exists = await context.db.table('InteractActionBase').exists()
        assert exists
    await TestUtils(context.db).execute_in_transaction(execute)
    await context.finalize()


def test_many_to_one(context):

    mapping = context.model('Action').infermapping('actionStatus')
    assert mapping is not None
    assert mapping.associationType == 'association'

    mapping = context.model('InteractAction').infermapping('actionStatus')
    assert mapping is not None
    assert mapping.associationType == 'association'
    assert mapping.childModel == 'InteractAction'


def test_many_to_many(context):

    mapping = context.model('AuthClient').infermapping('scopes')
    assert mapping is not None
    assert mapping.associationType == 'junction'
