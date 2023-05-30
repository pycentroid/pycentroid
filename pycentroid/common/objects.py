from collections import namedtuple
from types import SimpleNamespace
from datetime import datetime, date, time
import inspect


class AnyObject(SimpleNamespace):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        for key, value in kwargs.items():
            if type(value) is dict:
                setattr(self, key, AnyObject(**value))
            elif type(value) is list:
                setattr(self, key, list(
                    map(lambda x: AnyObject(**x) if isinstance(x, dict) else x, value))
                        )

    def __str__(self):
        return self.__dict__.__str__()


class SimpleDict(dict):
    """A dictionary object which exports keys as attributes
    """
    def __init__(self, **kwargs):
        super(SimpleDict, self).__init__()
        for key, value in kwargs.items():
            if type(value) is dict:
                self[key] = SimpleDict(**value)
            elif isinstance(value, list):
                values = map(lambda x: SimpleDict(**x) if type(x) is dict else x, value)
                self[key] = list(values)
            else:
                self[key] = value

    def __setattr__(self, key, value):
        self[key] = value


class AnyDict(dict):
    """A dictionary object which exports keys as attributes
    """
    def __init__(self, **kwargs):
        super(AnyDict, self).__init__()
        for key, value in kwargs.items():
            if type(value) is dict:
                self[key] = AnyDict(**value)
            elif isinstance(value, list):
                values = map(lambda x: AnyDict(**x) if type(x) is dict else x, value)
                self[key] = list(values)
            else:
                self[key] = value

    def __setattr__(self, key, value):
        self[key] = value

    def __getattr__(self, name):
        return self.get(name, None)


def dict_to_object(d):
    for k, v in d.items():
        if isinstance(v, dict):
            d[k] = dict_to_object(v)
    return namedtuple('X', d.keys())(*d.values())


def is_object_like(obj):
    """Determines whether the given value is an object or not
    """
    if obj is None:
        return False
    for t in [str, int, float, bool, datetime, date, time]:
        if isinstance(obj, t):
            return False
    if inspect.isfunction(obj) or inspect.isclass(obj):
        return False
    return isinstance(obj, object)
