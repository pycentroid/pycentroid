import inspect
from pycentroid.common import expect
from os import getcwd
from typing import TypeVar

T = TypeVar('T')


class ApplicationServiceBase:
    def __init__(self):
        pass


class ServiceContainer:
    __services__: dict = {}

    def __init__(self):
        pass

    # noinspection PyPep8Naming,PyShadowingNames
    def get(self, T) -> T:
        expect(inspect.isclass(T)).to_be_truthy(TypeError('Application service must be a type'))
        return self.__services__.get(T.__name__)

    def use(self, service, strategy=None):
        expect(inspect.isclass(service)).to_be_truthy(TypeError('Application service must be a type'))
        if strategy is None:
            self.__services__.update({
                service.__name__: service(self)
            })
        elif inspect.isclass(strategy):
            self.__services__.update({
                service.__name__: strategy(self)
            })
        else:
            self.__services__.update({
                service.__name__: strategy
            })
        return self

    def has(self, service) -> bool:
        expect(inspect.isclass(service)).to_be_truthy(TypeError('Application service must be a type'))
        return service.__name__ in self.__services__


class ApplicationBase:
    cwd = getcwd()
    services: ServiceContainer = ServiceContainer()

    def __init__(self):
        pass


class ApplicationService(ApplicationServiceBase):
    def __init__(self, application: ApplicationBase):
        super().__init__()
        # set application
        self.application = application
