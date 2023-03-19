class QueryEntity(dict):
    def __init__(self, name):
        self.__setitem__('$collection', name)
    
    def get_collection(self):
        return self.__getitem__('$collection')