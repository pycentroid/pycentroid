
import re
from .query_expression import QueryExpression
from .query_field import get_first_key
from ..common import NotImplementError
from .utils import SqlUtils
from .object_name_validator import ObjectNameValidator

class SqlDialectOptions:
    def __init__(self, name_format = r'\1', force_alias = True):
        self.name_format = name_format
        self.force_alias = force_alias

class SqlDialect:

    def __init__(self, options = SqlDialectOptions()):
        self.options = options

    def escape(value, unquoted = True):
        if type(value) is dict:
            key = get_first_key(value)
            if (key.startswith('$')):
                func = getattr(self, '__' + key[1:] + '__')
                if not func is None:
                    return func(value[key])
            elif value[key] == 1:
                    # return object name
                    return self.escape_name(key)
        return SqlUtils.escape(value)
    
    def escape_constant(value):
        return SqlUtils.escape(value)
    
    def escape_name(value):
        name = value if value.startswith('$') == False else value[1:]
        return  ObjectNameValidator().escape(value, self.options.name_format)

    def __eq__(left, right):
        final_right = self.escape(right)
        if final_right == 'NULL':
            return f'{self.escape(left)} IS NULL'
        return f'{self.escape(left)} = {final_right}'
    
    def __ne__(left, right):
        final_right = self.escape(right)
        if final_right == 'NULL':
            return f'NOT {self.escape(left)} IS NULL'
        return f'{self.escape(left)} = {final_right}'
    
    def __gt__(left, right):
        return f'{self.escape(left)} > {self.escape(right)}'

    def __gte__(left, right):
        return f'{self.escape(left)} >= {self.escape(right)}'
    
    def __lt__(left, right):
        return f'{self.escape(left)} < {self.escape(right)}'
    
    def __lte__(left, right):
        return f'{self.escape(left)} <= {self.escape(right)}'

    def __floor__(expr):
        return f'FLOOR({self.escape(expr)})'

    def __ceil__(expr):
        return f'CEILING({self.escape(expr)})'
    
    def __round__(expr, digits = 0):
        return f'ROUND({self.escape(expr)},{self.escape(digits)})'
    
    def __and__(**args):
        exprs = []
        for arg in args:
            exprs.append(self.escape(arg))
        result = '('
        result += ' AND '.join(exprs)
        result = ')'
        return result

    def __or__(**args):
        exprs = []
        for arg in args:
            exprs.append(self.escape(arg))
        result = '('
        result += ' OR '.join(exprs)
        result = ')'
        return result

    def __length__(expr):
        return f'LENGTH({self.escape(expr)})'

    def __trim__(expr):
        return f'TRIM({self.escape(expr)})'
    
    def __concat__(*args):
        exprs = []
        for arg in args:
            exprs.append(self.escape(arg))
        params_str = ','.join(exprs) 
        return f'CONCAT({params_str})'
    
    def __indexOfBytes__(expr, search):
        return f'(LOCATE({self.escape(search)},{self.escape(expr)}) - 1)'
    
    def __substr__(expr, pos, length = None):
        if length is None:
            return f'(SUBSTRING({self.escape(expr)},{self.escape(pos)} + 1))'
        return f'(SUBSTRING({self.escape(expr)},{self.escape(pos)} + 1, {self.escape(length)}))'
    
    def __toLower__(expr):
        return f'(LOWER({self.escape(expr)})'

    def __year__(expr):
        return f'(YEAR({self.escape(expr)})'
    
    def __month__(expr):
        return f'(MONTH({self.escape(expr)})'
    
    def __dayOfMonth__(expr):
        return f'(DAY({self.escape(expr)})'
    
    def __hour__(expr):
        return f'(HOUR({self.escape(expr)})'
    
    def __minute__(expr):
        return f'(MINUTE({self.escape(expr)})'
    
    def __second__(expr):
        return f'(SECOND({self.escape(expr)})'
    
    def __add__(**args):
        exprs = []
        for arg in args:
            exprs.append(self.escape(arg))
        result = '('
        result += '+'.join(exprs)
        result = ')'
        return result
    
    def __subtract__(**args):
        exprs = []
        for arg in args:
            exprs.append(self.escape(arg))
        result = '('
        result += '-'.join(exprs)
        result = ')'
        return result
    
    def __multiply__(**args):
        exprs = []
        for arg in args:
            exprs.append(self.escape(arg))
        result = '('
        result += '*'.join(exprs)
        result = ')'
        return result
    
    def __divide__(**args):
        exprs = []
        for arg in args:
            exprs.append(self.escape(arg))
        result = '('
        result += '/'.join(exprs)
        result = ')'
        return result
    
    def __modulo__(**args):
        exprs = []
        for arg in args:
            exprs.append(self.escape(arg))
        result = '('
        result += '%%'.join(exprs)
        result = ')'
        return result
    


class SqlFormatter:
    def __init__(self, dialect = None):
        self.__dialect__ = SqlDialect() if dialect is None else dialect
    
    def format_select(self, query:QueryExpression):
        raise NotImplementedError()

    def format_update(self, query:QueryExpression):
        raise NotImplementedError()
    
    def format_delete(self, query:QueryExpression):
        raise NotImplementedError()
    
    def format_insert(self, query:QueryExpression):
        raise NotImplementedError()