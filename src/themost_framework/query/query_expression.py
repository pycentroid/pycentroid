
class UnknownPropertyException(Exception):
    """Query property not set"""
    pass


class QueryExpression:
    def __init__(self):
        self.__query = None
        self.__property = None
        self.__last_logical = None
        return

    def where(self, name: str):
        self.__query = None
        self.__property = name
        return self

    def equal(self, value):
        if self.__property is None:
            raise UnknownPropertyException
        self.__append_expression({
            '$eq': [
                self.__property,
                value
            ]
        })
        return self

    def equals(self, value):
        return self.equal(value)

    def not_equal(self, value):
        if self.__property is None:
            raise UnknownPropertyException
        self.__append_expression({
            '$ne': [
                self.__property,
                value
            ]
        })
        return self

    def not_equals(self, value):
        return self.not_equal(value)

    def greater_than(self, value):
        if self.__property is None:
            raise UnknownPropertyException
        self.__append_expression({
            '$gt': [
                self.__property,
                value
            ]
        })
        return self

    def greater_or_equal(self, value):
        if self.__property is None:
            raise UnknownPropertyException
        self.__append_expression({
            '$gte': [
                self.__property,
                value
            ]
        })
        return self

    def lower_than(self, value):
        if self.__property is None:
            raise UnknownPropertyException
        self.__append_expression({
            '$lt': [
                self.__property,
                value
            ]
        })
        return self

    def lower_or_equal(self, value):
        if self.__property is None:
            raise UnknownPropertyException
        self.__append_expression({
            '$lte': [
                self.__property,
                value
            ]
        })
        return self

    def also(self, name: str):
        self.__property = name
        self.__last_logical = '$and'
        return self

    def either(self, name: str):
        self.__property = name
        self.__last_logical = '$or'
        return self

    def __append_expression(self, expr):
        if self.__query is None:
            self.__query = expr
