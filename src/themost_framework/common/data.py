from typing import Callable
from abc import abstractmethod

class ObjectMap:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

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

class DatabaseTable:

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


class DatabaseTableIndexes:

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

class DatabaseView:

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
