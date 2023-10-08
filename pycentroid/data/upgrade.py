from .types import DataContextBase, DataModelBase, DataField, DataModelProperties, UpgradeEventArgs
from .configuration import DataConfiguration
from .loaders import SchemaLoaderStrategy
from pycentroid.common import expect, DataError
from pycentroid.query import DataColumn
from .data_types import DataTypes
import json
from os.path import dirname, join


class DataModelUpgrade:

    @staticmethod
    async def before(event: UpgradeEventArgs):
        # get context
        context: DataContextBase = event.model.context
        configuration: DataConfiguration = context.application.services.get(DataConfiguration)
        # get schema loader
        loader: SchemaLoaderStrategy = configuration.getstrategy(SchemaLoaderStrategy)
        # and check if the current model has been already loaded
        if event.model.properties.name in loader.loaded:
            return
        # check if the current model is sealed and cannot be upgrade
        if event.model.properties.sealed is True:
            loader.loaded.update({
                event.model.properties.name: 1
            })
            # exit
            return
        # check version
        with open(join(dirname(__file__), 'resources/models/Migration.json'), 'r') as file:
            # load json schema for Migrations
            d = json.load(file)
            migrations: DataModelProperties = DataModelProperties(**d)
        # create table if it does not exist
        exists = await context.db.table(migrations.get_source()).exists()
        if exists is False:
            await context.db.table(migrations.get_source()).create(migrations.fields)
        # get version of current model
        version = await context.db.table(event.model.properties.get_source()).version()
        # if version found is greater than or equal to current version
        if version is not None and version >= event.model.properties.version:
            # set updated
            loader.loaded.update({
                event.model.properties.name: 1
            })
            # and exit
            return
        # get base model
        base: DataModelBase = event.model.base()
        if base is not None:
            # try to upgrade base model
            upgrade_event = UpgradeEventArgs(model=base)
            await base.before.upgrade.emit(upgrade_event)
        # get model attributes
        attributes = list(
            filter(lambda x: x.model == event.model.properties.name and bool(x.many) is False, event.model.attributes)
            )
        if base is not None:
            # get primary key
            attr: DataField = next(filter(lambda x: x.primary is True, base.attributes), None)
            if attr is not None:
                # insert primary key
                key = DataField(**attr.copy())
                # change auto increment to integer
                if key.type == 'Counter':
                    key.type = 'Integer'
                attributes.insert(0, key)
        # convert attributes to a list of data fields
        types: DataTypes = configuration.getstrategy(DataTypes)
        columns: list[DataColumn] = []
        for attribute in attributes:
            # search attribute type in data types collection
            if types.has(attribute.type):
                sqltype = types.get(attribute.type).sqltype
                nullable = attribute.nullable if 'nullable' in attribute else True
                # noinspection PyUnusedLocal
                column = DataColumn(name=attribute.name, type=sqltype, nullable=nullable)
                size = attribute.size if 'size' in attribute else None
                scale = attribute.scale if 'scale' in attribute else None
            else:
                # try to find attribute type as data model
                parent = context.model(attribute.type)
                expect(parent).to_be_truthy(
                    DataError('The specified type cannot be found', None, event.model.properties.name, attribute.name)
                    )
                # find primary key
                attr = next(filter(lambda x: x.primary is True, parent.attributes), None)
                expect(attr).to_be_truthy(DataError('Primary key cannot be found', None, parent.properties.name))
                # get column type
                sqltype = types.get(attr.type).sqltype
                if sqltype == 'Counter':
                    sqltype = 'Integer'
                # get nullable
                nullable = attribute.nullable if 'nullable' in attribute else True
                size = attr.size if 'size' in attr else None
                scale = attr.scale if 'scale' in attr else None
            # append column
            column = DataColumn(name=attribute.name, type=sqltype, nullable=nullable, size=size, scale=scale)
            columns.append(column)
        # do upgrade
        await context.db.table(event.model.properties.get_source()).change(columns)
        # set loaded
        loader.loaded.update({
                event.model.properties.name: 1
            })
        # emit after upgrade event
        setattr(event, 'done', True)
        await event.model.after.upgrade.emit(event)

    @staticmethod
    async def after(event: UpgradeEventArgs):
        items = event.model.properties.seed
        if isinstance(items, list) and len(items) > 0:
            length = await event.model.as_queryable().silent().count()
            if length == 0:
                await event.model.insert(items)
