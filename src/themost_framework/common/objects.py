from typing import Callable
from abc import abstractmethod

class ObjectMap:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
