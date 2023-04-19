from .types import DataModelBase
from centroid.query import OpenDataQueryExpression
from typing import List


class DataQueryable(OpenDataQueryExpression):

    __model__: DataModelBase

    def __init__(self, model: DataModelBase):
        super().__init__(model.properties.view)
        self.__model__ = model

    @property
    def model(self) -> DataModelBase:
        return self.__model__

    async def get_item(self) -> object:
        pass

    async def get_items(self) -> List[object]:
        pass

    async def get_list(self):
        pass
