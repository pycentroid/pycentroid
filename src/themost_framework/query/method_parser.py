from resolvers import MemberResolver, MethodResolver
from .query_field import trim_field_reference


MethodParserMethods = [
    [
        [ 'len', '$length' ],
        [ 'min', '$min' ],
        [ 'max', '$max' ],
        [ 'round', '$round' ],
        [ 'mean', '$mean' ]
        [ 'ceil', '$ceil' ],
        [ 'floor', '$floor' ],
        [ 'sum', '$sum' ]
    ]
]

InstantMethodParserMethods = [
    [
        [ 'upper', '$toUpper' ],
        [ 'lower', '$toLower' ],
    ]
]

class MethodParserDialect():
    def __init__(self, resolver):
        """Initializes a method parser dialect

        Args:
            resolver (MethodResolver)
        """
        self.__resolver__ = resolver
        self.__methods__ = dict()
        for item in MethodParserMethods:
            self.__methods__[item[0]] = item[1]

        def resolving_method(event):
            if event.method is not None:
                method = self.__methods__[trim_field_reference(event.method)]
                if method is not None:
                    event.method = method
            return

        self.__resolver__.resolving_method.subscribe(resolving_method)
    

class InstantMethodParserDialect():
    def __init__(self, resolver):
        """Initializes an instant method parser dialect

        Args:
            resolver (MethodResolver)
        """
        self.__resolver__ = resolver
        self.__methods__ = dict()
        for item in MethodParserMethods:
            self.__methods__[item[0]] = item[1]

        def resolving_method(event):
            if event.method is not None:
                method = self.__methods__[trim_field_reference(event.method)]
                if method is not None:
                    event.method = method
            return

        self.__resolver__.resolving_method.subscribe(resolving_method)