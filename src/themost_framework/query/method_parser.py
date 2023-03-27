from .resolvers import MemberResolver, MethodResolver
from .query_field import trim_field_reference

class MethodParserDialect():
    def __init__(self, resolver):
        """Initializes a method parser dialect

        Args:
            resolver (MethodResolver)
        """
        self.resolver = resolver
        
        def resolving_method(event):
            if event.method is not None:
                method = trim_field_reference(event.method)
                if hasattr(self, '__' + method + '__') == True:
                    func = getattr(self, '__' + method + '__')
                    if func is not None:
                        event.resolve = func

        self.resolver.resolving_method.subscribe(resolving_method)

    def __len__(self, *args):
        return {
            '$length': list(args)
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
    

class InstantMethodParserDialect():
    def __init__(self, resolver):
        """Initializes an instant method parser dialect

        Args:
            resolver (MethodResolver)
        """
        self.resolver = resolver
        
        def resolving_method(event):
            if event.method is not None:
                method = trim_field_reference(event.method)
                func = None
                if hasattr(self, '__' + method + '__') == True:
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
    
    def __year__(self, *args):
        return {
            '$year': list(args)
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
    
    def __strip__(self, *args):
        return {
            '$trim': list(args)
        }
    
    def __strip__(self, *args):
        return {
            '$trim': list(args)
        }
    
    def __startswith__(self, *args):
        return {
            '$eq': [
                {
                    '$indexOfBytes': list(args)
                },
                0
            ]
        }
    
    def __endswith__(self, *args):
        return {
            '$eq': [
                {
                    '$indexOfBytes': args
                },
                0
            ]
        }
