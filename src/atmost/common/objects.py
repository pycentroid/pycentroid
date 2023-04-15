from collections import namedtuple


class AnyObject:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
    
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
    return namedtuple('AnyObject', d.keys())(*d.values())
