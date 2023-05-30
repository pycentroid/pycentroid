import pytest
from os.path import abspath, join, dirname
from pycentroid.data.application import DataApplication
from pycentroid.data.context import DataContext
from types import SimpleNamespace


APP_PATH = abspath(join(dirname(__file__), '..'))


@pytest.fixture()
def context() -> DataContext:
    app = DataApplication(cwd=APP_PATH)
    return app.create_context()


async def test_count(context: DataContext):
    length = await context.model('Product').as_queryable().count()
    assert length > 0


async def test_select(context: DataContext):
    results = await context.model('Product').as_queryable().select(
        lambda x: (x.id, x.category, x.price,)
    ).get_items()
    assert len(results) > 0


async def test_find(context: DataContext):
    results = await context.model('Product').as_queryable().find({
        'category': 'Desktops'
    }).get_items()
    assert len(results) > 0
    for result in results:
        assert result.category == 'Desktops'

    results = await context.model('Product').as_queryable().find(SimpleNamespace(category='Laptops')).get_items()
    assert len(results) > 0
    for result in results:
        assert result.category == 'Laptops'


async def test_query_association(context: DataContext):
    results = await context.model('Order').where(
        lambda x: x.orderedItem.category == 'Desktops'
        ).get_items()
    assert len(results) > 0


async def test_find_by_obj(context: DataContext):
    item = {
            'orderedItem': {
                'name': 'Nikon D7100'
            },
            'orderStatus': {
                'name': 'Pickup'
            }
        }
    results = await context.model('Order').find(item).get_items()
    assert len(results) > 0
    for result in results:
        assert result.orderedItem.id == 84
        assert result.orderStatus.name == 'Pickup'


async def test_expand_parent_object(context: DataContext):
    results = await context.model('Order').where(
        lambda x: x.orderStatus.alternateName == 'OrderProcessing'
    ).expand(
        lambda x: (x.customer,)
        ).take(25).get_items()
    assert len(results) > 0
    for result in results:
        assert result.customer.id is not None


async def test_expand_children(context: DataContext):
    results = await context.model('Person').where(
        lambda x: x.jobTitle == 'Civil Engineer'
    ).expand(
        lambda x: (x.orders,)
        ).take(10).get_items()
    assert len(results) > 0
    for result in results:
        assert isinstance(result.orders, list)
        for order in result.orders:
            assert order.customer == result.id


async def test_expand_many_to_many(context: DataContext):
    results = await context.model('Group').where(
        lambda x: x.name == 'Users'
    ).expand(
        lambda x: (x.members,)
        ).take(10).get_items()
    assert len(results) > 0
    for result in results:
        assert isinstance(result.members, list)


async def test_expand_many_to_many_parent_objects(context: DataContext):
    results = await context.model('User').as_queryable().expand(
        lambda x: (x.groups,)
        ).take(10).get_items()
    assert len(results) > 0
    for result in results:
        assert isinstance(result.groups, list)
