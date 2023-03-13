class QueryValue(dict):
    def __init__(self, value):
        self.__setitem__('$literal', value)