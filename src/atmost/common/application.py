import inspect
from atmost.common import expect
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

    # noinspection PyPep8Naming
    def get(self, T) -> T:
        expect(inspect.isclass(T)).to_be_truthy(TypeError('Application service must be a type'))
        return self.__services__.get(type(T))

    def use(self, service, strategy=None):
        expect(inspect.isclass(service)).to_be_truthy(TypeError('Application service must be a type'))
        if strategy is None:
            self.__services__.update({
                type(service): service(self)
            })
        elif inspect.isclass(strategy):
            self.__services__.update({
                type(service): strategy(self)
            })
        else:
            self.__services__.update({
                type(service): strategy
            })
        return self

    def has(self, service) -> bool:
        return type(service) in self.__services__


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
