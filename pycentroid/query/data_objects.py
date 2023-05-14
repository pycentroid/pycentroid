from typing import Callable
from abc import abstractmethod
from pycentroid.common import AnyDict
import logging


class DataAdapterBase:

    __raw_connection__ = None

    @abstractmethod
    async def open(self):
        pass

    @abstractmethod
    async def close(self):
        pass

    @abstractmethod
    async def execute(self, query, values=None):
        pass

    @abstractmethod
    async def execute_in_transaction(self, func: Callable):
        pass


class DataColumn(AnyDict):
    def __init__(self, **kwargs):
        self.name = None
        self.type = None
        self.nullable = True
        self.size = None
        self.scale = None
        super().__init__(**kwargs)


class DataTableIndex:

    def __init__(self, table: str, adapter: DataAdapterBase):
        self.table = table
        self.__adapter__ = adapter

    @abstractmethod
    async def create(self, name: str, columns: list):
        pass

    @abstractmethod
    async def exists(self, name: str):
        pass

    @abstractmethod
    async def drop(self, name: str):
        pass

    @abstractmethod
    async def list(self):
        pass


class DataTable:

    def __init__(self, table: str, adapter: DataAdapterBase):
        self.table = table
        self.__adapter__ = adapter

    @abstractmethod
    async def create(self, fields: list):
        pass

    @abstractmethod
    async def change(self, fields: list):
        pass

    @abstractmethod
    async def exists(self):
        pass

    @abstractmethod
    async def drop(self):
        pass

    @abstractmethod
    async def version(self):
        pass

    @abstractmethod
    async def columns(self):
        pass

    @abstractmethod
    async def indexes(self) -> DataTableIndex:
        pass


class DataView:

    def __init__(self, view: str, adapter: DataAdapterBase):
        self.view = view
        self.__adapter__ = adapter

    @abstractmethod
    async def create(self, query):
        pass

    @abstractmethod
    async def exists(self):
        pass

    @abstractmethod
    async def drop(self):
        pass


class DataAdapter(DataAdapterBase):

    def __init__(self):
        super().__init__()

    def __del__(self):
        try:
            self.close()
        except Exception as error:
            logging.warning('An error occurred while closing database connection.')
            logging.warning(error)

    @abstractmethod
    async def open(self):
        pass

    @abstractmethod
    async def close(self):
        pass

    @abstractmethod
    async def execute(self, query, values=None):
        pass

    @abstractmethod
    async def execute_in_transaction(self, func: Callable):
        pass

    @abstractmethod
    async def select_identity(self):
        pass

    @abstractmethod
    async def last_identity(self):
        pass

    @abstractmethod
    def table(self, table: str) -> DataTable:
        pass

    @abstractmethod
    def view(self, view: str) -> DataView:
        pass
