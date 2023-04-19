from typing import List
from .types import DataModelBase
from .queryable import DataQueryable


class DataModel(DataModelBase):

    __silent__ = False

    def __init__(self, context=None, properties=None, **kwargs):
        super().__init__(context, properties)

    def silent(self, value: bool = True):
        self.__silent__ = value
        return self

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
