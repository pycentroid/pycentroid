
class QueryValue(dict):
    def __init__(self, value):
        super().__init__()
        self.__setitem__('$literal', value)
