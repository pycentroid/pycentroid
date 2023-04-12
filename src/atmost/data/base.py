import inspect
from atmost.common import expect
from os import getcwd


class ApplicationServiceBase:
    def __init__(self):
        pass


class DataApplicationBase:
    cwd = getcwd()
    __services__: dict = {}

    def __init__(self):
        pass

    def get(self, service):
        expect(inspect.isclass(service)).to_be_truthy(TypeError('Application service must be a type'))
        return self.__services__.get(type(service))

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

    def has(self, service):
        return type(service) in self.__services__

