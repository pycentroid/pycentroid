from typing import Callable
from abc import abstractmethod
from themost_framework.common import AnyObject

class DataColumn(AnyObject):
    def __init__(self, **kwargs):
        self.name = None
        self.type = None
        self.nullable = True
        self.size = None
        self.scale = None
        super().__init__(**kwargs)

class DataAdapter:

    @abstractmethod
    def open(self):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def execute(query, values = None):
        pass

    @abstractmethod
    def execute_in_transaction(self, func: Callable):
        pass

    @abstractmethod
    def select_identity(self):
        pass

    @abstractmethod
    def last_identity(self):
        pass

    @abstractmethod
    def upgrade(self, table_upgrade):
        pass

class DataTable:

    def __init__(self,table: str, adapter: DataAdapter):
        self.table = table
        self.__adapter__ = adapter

    @abstractmethod
    def create(self, fields: list):
        pass

    @abstractmethod
    def change(self, fields: list):
        pass

    @abstractmethod
    def exists(self):
        pass

    @abstractmethod
    def drop(self):
        pass

    @abstractmethod
    def version(self):
        pass

    @abstractmethod
    def columns(self):
        pass

    @abstractmethod
    def indexes(self):
        pass

class DataTableIndex:

    def __init__(self,table: str, adapter: DataAdapter):
        self.table = table
        self.__adapter__ = adapter

    @abstractmethod
    def create(self, name: str, columns: list):
        pass

    @abstractmethod
    def exists(self, name: str):
        pass

    @abstractmethod
    def drop(self, name: str):
        pass

    @abstractmethod
    def list(self):
        pass

class DataView:

    def __init__(self,view: str, adapter: DataAdapter):
        self.view = view
        self.__adapter__ = adapter

    @abstractmethod
    def create(self, query):
        pass

    @abstractmethod
    def exists(self):
        pass

    @abstractmethod
    def drop(self):
        pass
