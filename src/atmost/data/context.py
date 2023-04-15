from abc import abstractmethod
from typing import Callable
from atmost.common import ApplicationBase, expect
from .configuration import DataConfiguration, DataAdapterStrategy
from atmost.query import DataAdapter


class DataContext:
    __db__: DataAdapter

    def __init__(self, application: ApplicationBase):
        self.application = application
        return

    @abstractmethod
    def db(self) -> DataAdapter or None:
        pass

    def finalize(self):
        if self.__db__ is not None:
            self.__db__.close()

    def execute_in_transaction(self, func: Callable):
        return self.__db__.execute_in_transaction(func)


class NamedDataContext(DataContext):
    name: str or None = None

    def __init__(self, application: ApplicationBase, name: str or None):
        super().__init__(application)
        self.name = name

    @property
    def db(self) -> DataAdapter or None:
        # get adapters
        configuration = self.application.services.get(DataConfiguration)
        data_adapters: DataAdapterStrategy = configuration.getstrategy(DataAdapterStrategy)
        adapter = data_adapters.get(self.name)
        # validate data adapter
        expect(adapter).to_be_truthy(Exception('The default data adapter cannot be found.'))
        expect(adapter.adapterType).to_be_truthy(Exception('Data adapter type cannot be empty at this context.'))
        # noinspection PyPep8Naming
        AdapterClass = adapter.adapterType.adapterClass
        # validate data adapter class
        expect(AdapterClass).to_be_truthy(Exception('Data adapter has not been set.'))
        # create instance
        self.__db__ = AdapterClass(adapter.options)
        return self.__db__


class DefaultDataContext(NamedDataContext):

    def __init__(self, application: ApplicationBase):
        super().__init__(application, None)
