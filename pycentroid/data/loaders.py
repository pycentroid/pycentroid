import json
from abc import abstractmethod
from inspect import isclass
from pycentroid.common import ConfigurationStrategy
from typing import List
from os.path import abspath, join, isfile, splitext
from os import listdir
import re
import importlib
from .types import DataModelProperties


class SchemaLoaderStrategy(ConfigurationStrategy):
    __models__: dict = {}
    loaded: dict = {}

    def __init__(self, configuration):
        super().__init__(configuration)

    def get(self, name: str):
        if name in self.__models__:
            return self.__models__[name].copy()
        return None

    def set(self, model):
        name = model['name']
        self.__models__.update({
            name: model
        })

    def list(self) -> List[str]:
        return list(self.__models__.keys())

    @abstractmethod
    def read(self):
        pass


class FileSchemaLoaderStrategy(SchemaLoaderStrategy):

    path = None
    __items__ = None

    def __init__(self, configuration):
        super().__init__(configuration)
        self.path = abspath(join(configuration.cwd, 'models'))

    def read(self):
        items = listdir(self.path)
        results = []
        for item in items:
            if isfile(join(self.path, item)):
                name, extension = splitext(item)
                if extension == '.json':
                    results.append(name)
        return results

    def get(self, name: str) -> DataModelProperties:
        if self.__items__ is None:
            self.__items__ = self.read()
        # case-insensitive search
        matcher = re.compile(f'^{name}$', re.IGNORECASE)
        item = next(filter(matcher.match, self.__items__), None)
        result = None
        if item is not None:
            # get schema
            with open(join(self.path, item + '.json'), 'r') as file:
                # load file
                d = json.load(file)
                result = DataModelProperties(**d)
                # set model
                self.set(result)
        # and return definition
        return result


class DefaultSchemaLoaderStrategy(FileSchemaLoaderStrategy):

    loaders = []

    def __init__(self, configuration):
        super().__init__(configuration)
        self.path = abspath(join(configuration.cwd, 'models'))
        # enumerate loaders
        loaders = configuration.get('settings/schema/loaders')
        if type(loaders) is list:
            for loader in loaders:
                # use loaderClass attribute and create instance
                if isclass(loader['loaderClass']):
                    # noinspection PyPep8Naming
                    LoaderClass = loader['loaderClass']
                    self.loaders.append(LoaderClass(configuration))
                # use loaderType attribute and import loader
                elif loader['loaderType'] is not None:
                    module_name, class_name = loader['loaderType'].split('#')
                    module = importlib.import_module(module_name)
                    if class_name in module:
                        # noinspection PyPep8Naming
                        LoaderClass = module[class_name]
                        loader['loaderClass'] = LoaderClass
                        self.loaders.append(LoaderClass(configuration))

    def get(self, name: str) -> DataModelProperties | None:
        model = super().get(name)
        if model is not None:
            return model
        for loader in self.loaders:
            model = loader.get(name)
            if model is not None:
                self.set(model)
                return model
        return None
