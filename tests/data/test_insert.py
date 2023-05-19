import pytest
from pycentroid.data.application import DataApplication
from pycentroid.data.context import DataContext
from pycentroid.common.objects import AnyObject
from os.path import abspath, join, dirname
from pycentroid.query import TestUtils

APP_PATH = abspath(join(dirname(__file__), '..'))


@pytest.fixture()
def context() -> DataContext:
    app = DataApplication(cwd=APP_PATH)
    return app.create_context()


async def test_insert_one(context):

    async def execute():
        new_item = AnyObject(name='MacbookPro 13.3 16GB', model='MAC16512')
        await context.model('Product').insert(new_item)
        result = await context.model('Product').where(
            lambda x: x.name == 'MacbookPro 13.3 16GB'
        ).get_item()
        assert result is not None
        assert result.model == 'MAC16512'

    await TestUtils(context.db).execute_in_transaction(execute)
    await context.finalize()
