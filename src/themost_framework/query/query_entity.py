class QueryEntity(dict):
    def __init__(self, name):
        self.__setitem__('$collection', name)