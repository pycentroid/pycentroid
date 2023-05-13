import re
from pycentroid.common import expect


class QueryEntity(dict):
    def __init__(self, name: str, alias: str = None):
        super().__init__()
        if alias is None:
            self.__setitem__(name, 1)
        else:
            self.__setitem__(alias, '$' + name)

    @property
    def alias(self):
        for key in self:
            value = self[key]
            if type(value) is int and value == 1:
                return None
            else:
                return key

    @property
    def collection(self):
        for key in self:
            value = self[key]
            if type(value) is int and value == 1:
                return key
            else:
                expect(type(value)).to_equal(str, TypeError('Collection name must be a string'))
                return re.sub('^\\$', '', value)
