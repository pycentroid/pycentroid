import re
from .query_field import trim_field_reference


# noinspection PyMethodMayBeStatic
class MethodParserDialect:
    def __init__(self, resolver):
        """Initializes a method parser dialect

        Args:
            resolver
        """
        self.resolver = resolver

        def resolving_method(event):
            if event.instance_method:
                return
            if event.method is not None:
                method = trim_field_reference(event.method)
                if hasattr(self, '__' + method + '__'):
                    func = getattr(self, '__' + method + '__')
                    if func is not None:
                        event.resolve = func

        self.resolver.resolving_method.subscribe(resolving_method)

    def __len__(self, *args):
        return {
            '$length': list(args)
        }

    def __count__(self, *args):
        return {
            '$count': list(args)
        }

    def __min__(self, *args):
        return {
            '$min': list(args)
        }

    def __max__(self, *args):
        return {
            '$max': list(args)
        }

    def __mean__(self, *args):
        return {
            '$avg': list(args)
        }

    def __sum__(self, *args):
        return {
            '$sum': list(args)
        }

    def __round__(self, *args):
        return {
            '$round': list(args)
        }

    def __ceil__(self, *args):
        return {
            '$ceil': list(args)
        }

    def __floor__(self, *args):
        return {
            '$floor': list(args)
        }

    def __year__(self, *args):
        return {
            '$year': list(args)
        }

    def __month__(self, *args):
        return {
            '$month': list(args)
        }

    def __day__(self, *args):
        return {
            '$dayOfMonth': list(args)
        }

    def __hour__(self, *args):
        return {
            '$hour': list(args)
        }

    def __minute__(self, *args):
        return {
            '$minute': list(args)
        }

    def __second__(self, *args):
        return {
            '$second': list(args)
        }


class InstanceMethodParser:
    def __init__(self, resolver):
        self.resolver = resolver


# noinspection PyMethodMayBeStatic
class InstanceMethodParserDialect(InstanceMethodParser):
    def __init__(self, resolver):
        """Initializes an instant method parser dialect

        Args:
            resolver
        """
        super().__init__(resolver)

        def resolving_method(event):
            if not event.instance_method:
                return
            if event.method is not None:
                method = trim_field_reference(event.method)
                if hasattr(self, '__' + method + '__'):
                    func = getattr(self, '__' + method + '__')
                    if func is not None:
                        event.resolve = func

        self.resolver.resolving_method.subscribe(resolving_method)

    def __upper__(self, *args):
        return {
            '$toUpper': list(args)
        }

    def __lower__(self, *args):
        return {
            '$toLower': list(args)
        }

    def __index__(self, *args):
        return {
            '$indexOfBytes': list(args)
        }

    def __startswith__(self, *args):
        return {
            '$eq': [
                {
                    '$regexMatch': {
                        'input': args[0],
                        'regex': '^' + re.escape(args[1])
                    }
                },
                1
            ]
        }

    def __endswith__(self, *args):
        return {
            '$eq': [
                {
                    '$regexMatch': {
                        'input': args[0],
                        'regex': re.escape(args[1]) + '$'
                    }
                },
                1
            ]
        }

    def ____contains____(self, *args):
        return {
            '$eq': [
                {
                    '$regexMatch': {
                        'input': args[0],
                        'regex': re.escape(args[1])
                    }
                },
                1
            ]
        }

    def __strip__(self, *args):
        return {
            '$trim': list(args)
        }

