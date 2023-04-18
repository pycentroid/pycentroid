import inspect
import re
from os import environ as env
from os import getcwd
from os.path import join, isfile
from typing import TypeVar

import pydash
import yaml

from .expect import expect

T = TypeVar('T')


def replace_slash_with_dot(path: str) -> str:
    return re.sub(r'/', '.', path)


class ExpectedStrategyTypeError(Exception):
    def __init__(self, message='Configuration strategy must be a type'):
        self.message = message
        super().__init__(self.message)


class ExpectedConfigurationStrategyError(Exception):
    def __init__(self, message='The specified object must be an instance of configuration strategy'):
        self.message = message
        super().__init__(self.message)


class ConfigurationStrategy:
    configuration = None

    def __init__(self, configuration):
        self.configuration = configuration
        pass


class ConfigurationBase:
    __strategy__ = {}
    __source__ = {}
    cwd = None

    def __init__(self, cwd=None):
        self.cwd = cwd or join(getcwd(), 'config')
        # load configuration from file
        path = join(self.cwd, f'app.{self.__env__}.yml')
        if isfile(path):
            with open(path, 'r') as file:
                self.__source__ = yaml.load(file, yaml.FullLoader)
        else:
            path = join(self.cwd, 'app.yml')
            if isfile(path):
                with open(path, 'r') as file:
                    self.__source__ = yaml.load(file, yaml.FullLoader)

    @property
    def __env__(self):
        return env['ENV'] if 'ENV' in env else 'development'

    # noinspection PyPep8Naming
    def getstrategy(self, T) -> T:
        expect(inspect.isclass(T)).to_be_truthy(ExpectedStrategyTypeError())
        return self.__strategy__.get(T.__name__)

    def usestrategy(self, strategy, useclass=None):
        expect(inspect.isclass(useclass)).to_be_truthy(ExpectedStrategyTypeError())
        if useclass is None:
            self.__strategy__[strategy.__name__] = strategy(self)
        elif inspect.isclass(useclass):
            instance = useclass(self)
            self.__strategy__[strategy.__name__] = instance
        elif type(useclass) is ConfigurationStrategy:
            self.__strategy__[strategy.__name__] = useclass
        else:
            raise ExpectedConfigurationStrategyError()
        return self

    def hasstrategy(self, strategy):
        expect(inspect.isclass(strategy)).to_be_truthy(ExpectedStrategyTypeError())
        return strategy.__name__ in self.__strategy__

    def get(self, path: str):
        return pydash.get(self.__source__, replace_slash_with_dot(path))

    def has(self, path: str):
        return pydash.has(self.__source__, replace_slash_with_dot(path))

    def set(self, path: str, value):
        return pydash.update(self.__source__, replace_slash_with_dot(path), value)

    def unset(self, path: str):
        return pydash.unset(self.__source__, replace_slash_with_dot(path))
