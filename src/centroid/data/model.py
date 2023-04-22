from typing import List
from .types import DataContextBase, DataModelBase, DataField, DataModelProperties, DataEventArgs, UpgradeEventArgs
from .queryable import DataQueryable
from .configuration import DataConfiguration
from .loaders import SchemaLoaderStrategy
from pydash import assign
from centroid.common import expect, DataError, AnyObject
from centroid.query import DataColumn
from .data_types import DataTypes
import json
from os.path import dirname, join
from .upgrade import DataModelUpgrade

class DataModelAttribute(DataField):
    model: str

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
        attributes: List[DataModelAttribute] = []
        if base_model is not None:
           attributes = base_model.attributes
        else:
            implements = self.context.model(self.properties.implements) if self.properties.implements is not None else None
            if implements is not None:
                attributes = list(map(lambda x: assign(DataModelAttribute(**x), {
                    'model': self.properties.name
                }), implements.attributes))
        for field in self.properties.fields:
            found = next(filter(lambda x: x.name==field.name, attributes), None)
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

    async def migrate(self):
        async def execute():
            event = AnyObject(model=self)
            await self.before.upgrade.emit(event)
        await self.context.execute_in_transaction(execute)

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
