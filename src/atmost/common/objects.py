from collections import namedtuple
from types import SimpleNamespace


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

    def __getattr__(self, name):
        return self.__dict__.get(name, None)
    
    def __str__(self):
        return self.__dict__.__str__()


def object(**kwargs):
    return AnyObject(**kwargs)


def dict_to_object(d):
    for k, v in d.items():
        if isinstance(v, dict):
            d[k] = dict_to_object(v)
    return namedtuple('X', d.keys())(*d.values())
