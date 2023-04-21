from typing import List
from .types import DataContextBase, DataModelBase, DataField, DataModelProperties, DataEventArgs, UpgradeEventArgs
from .queryable import DataQueryable
from .configuration import DataConfiguration
from .loaders import SchemaLoaderStrategy
from pydash import assign
from centroid.common import expect, DataError
from centroid.query import DataColumn
from .data_types import DataTypes

class DataModelAttribute(DataField):

    model: str

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
        # check if the current model is sealed and connot be upgrade
        if model.properties.sealed is True:
            loader.loaded.update({
                event.model.properties.name : 1
            })
            # exit
            return
        # get base model
        base: DataModel = event.model.base()
        if base is not None:
            # try to upgrade base model
            upgrade_event = DataEventArgs(model=base)
            await base.before.upgrade.emit(upgrade_event)
        # get model attributes
        attributes = list(filter(lambda x:x.model==event.model.name and bool(x.many) is False, event.model.attributes))
        if base is not None:
            # get primary key
            primary_key: DataField = next(filter(lambda x: x.primary is True, base.attributes), None)
            if primary_key is not None:
                # insert primary key
                new_key = primary_key.copy()
                # change auto increment to integer
                if new_key.type == 'Counter':
                    new_key.type = 'Integer'
                attributes.insert(0, new_key)
        # convert attributes to a list of data fields
        types: DataTypes = configuration.getstrategy(DataTypes)
        columns: list[DataColumn] = []
        for attribute in attributes:
            # search attribute type in data types collection
            if types.has(attribute.type):
                sqltype = types.get(attribute.type).sqltype
                nullable = attribute.nullable if 'nullable' in attribute else True
                column = DataColumn(name=attribute.name, type=sqltype, nullable=nullable)
                size = attribute.size if 'size' in attribute else None
                scale = attribute.scale if 'scale' in attribute else None
            else:
                # try to find attribute type as data model
                attribute_model = context.model(attribute.type)
                expect(attribute_model).to_be_truthy(DataError('The specified type cannot be found', None, model.name, attribute.name)) 
                # find primary key
                primary_key = next(filter(lambda x: x.primary is True, attribute_model.attributes), None)
                expect(primary_key).to_be_truthy(DataError('Primary key cannot be found', None, attribute_model.name))
                # get column type
                sqltype = types.get(primary_key.type).sqltype
                # get nullable
                nullable = attribute.nullable if 'nullable' in attribute else True
                size = primary_key.size if 'size' in primary_key else None
                scale = primary_key.scale if 'scale' in primary_key else None
            # append column
            column = DataColumn(name=attribute.name, type=sqltype, nullable=nullable, size=size, scale=scale)
            columns.append(column)
        # do upgrade
        context.db.table(model.properties.view).change(columns)
        # emit after upgrade
        await event.model.after.upgrade.emit(event)

    
    async def after(event: UpgradeEventArgs):
        pass
        
class DataModel(DataModelBase):

    __silent__ = False
    __attributes__: List[DataModelAttribute]

    def __init__(self, context: DataContextBase=None, properties: DataModelProperties=None, **kwargs):
        super().__init__(context, properties)
        self.before.upgrade.subscribe(DataModelUpgrade.before)
        self.after.upgrade.subscribe(DataModelUpgrade.after)


    def silent(self, value: bool = True):
        self.__silent__ = value
        return self

    def base(self) -> DataModelBase:
        if self.properties is not None and self.properties.inherits is not None:
             return self.context.model(self.properties.inherits)
        return None

    @property
    def attributes(self) -> List[DataModelAttribute]:
        if hasattr(self, '__attributes__'):
            return self.__attributes__
        base_model = self.base()
        base_attributes: List[DataModelAttribute] = []
        if base_model is not None:
           base_attributes = base_model.attributes
        # append base attributes
        attributes: List[DataModelAttribute]  = base_attributes
        for field in self.properties.fields:
            found = next(filter(lambda x: x.name==field.name, base_attributes), None)
            if found is None:
                attr = DataModelAttribute(**field, model=self.properties.name)
            else:
                clone = assign(found, field)
                attr = DataModelAttribute(**clone)
            attributes.append(attr)
        self.__attributes__ = attributes
        return self.__attributes__
        

    def as_queryable(self):
        return DataQueryable(self)

    async def upsert(self, o: object or List[object]):
        pass

    async def save(self, o: object or List[object]):
        pass

    async def update(self, o: object or List[object]):
        pass

    async def remove(self, o: object or List[object]):
        pass

    async def insert(self, o: object or List[object]):
        pass
