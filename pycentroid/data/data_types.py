from pycentroid.common import ConfigurationBase, ConfigurationStrategy, AnyDict
from os.path import join, dirname
import json


class DataTypeProperties:

    maxValue: int
    minValue: int
    minLength: int
    maxLength: int
    pattern: str
    patternMessage: str


class DataType:

    label: str
    comment: str
    url: str
    edmtype: str
    instances: list[str]
    supertypes: list[str]
    sqltype: str
    version: str
    properties: DataTypeProperties


class DataTypes(ConfigurationStrategy):

    __types__: AnyDict = AnyDict()

    def __init__(self, configuration: ConfigurationBase):
        super().__init__(configuration)
        # load defaults
        with open(join(dirname(__file__), 'resources/dataTypes.json'), 'r') as file:
            # load file
            d = json.load(file)
            self.__types__ = AnyDict(**d)

    def has(self, data_type: str) -> bool:
        return data_type in self.__types__.keys()

    def get(self, data_type: str) -> DataType:
        return self.__types__[data_type]
      
    def pop(self, data_type: str):
        return self.__types__.pop(data_type)
