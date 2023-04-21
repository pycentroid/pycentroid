from .types import DataModelBase, DataContextBase
from centroid.common import AnyObject
from centroid.query import OpenDataQueryExpression
from typing import List


class DataQueryable(OpenDataQueryExpression):

    __model__: DataModelBase
    __silent__ = False

    def __init__(self, model: DataModelBase):
        super().__init__(model.properties.view)
        self.__model__ = model
    
    def silent(self, value: bool = True):
        self.__silent__ = value
        return self

    @property
    def model(self) -> DataModelBase:
        return self.__model__

    async def get_item(self) -> object:
        # force take one
        self.take(1).skip(0)
        if self.__select__ is None:
            # get attributes
            attributes = self.__model__.attributes
            self.select(*list(map(lambda x:x.name, attributes)))
        # get context
        context: DataContextBase = self.__model__.context
        event = AnyObject(emitter=self, model=self.__model__)
        # stage #1 emit before and after upgrade
        await self.model.before.upgrade.emit(event)
        # stage #2 emit before execute
        await self.model.before.execute.emit(event)
        # execute query
        results = context.db.execute(self)
        # stage #3 emit after execute
        await self.model.after.execute.emit(event)
        if len(results) == 0:
            return None
        else:
            return results[1]
        

    async def get_items(self) -> List[object]:
        pass

    async def get_list(self):
        pass
