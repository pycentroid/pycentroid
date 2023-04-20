from typing import List
from .types import DataContextBase, DataModelBase, DataField, DataModelProperties
from .queryable import DataQueryable

class DataModelAttribute(DataField):

    model: str

class DataModel(DataModelBase):

    __silent__ = False
    __attributes__: List[DataModelAttribute]

    def __init__(self, context: DataContextBase=None, properties: DataModelProperties=None, **kwargs):
        super().__init__(context, properties)

    def silent(self, value: bool = True):
        self.__silent__ = value
        return self

    def base(self) -> DataModelBase:
        if self.properties is not None and self.properties.inherits is not None:
             return self.context.model(self.properties.inherits)
        return None

    @property
    def attributes(self) -> List[DataModelAttribute]:
        if self.__attributes__ is not None:
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
                attr = DataModelAttribute(**found)
            else:
                attr = DataModelAttribute(**found, model=self.name)
            attributes.push(attr)
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
