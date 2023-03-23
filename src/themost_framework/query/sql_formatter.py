from .query_expression import QueryExpression, QueryField
from .query_field import get_first_key
from ..common import expect
from .utils import SqlUtils
from .object_name_validator import ObjectNameValidator


class SqlDialectOptions:
    def __init__(self, name_format=r'\1', force_alias=True):
        self.name_format = name_format
        self.force_alias = force_alias


class SqlDialect:
    Space = ' '
    From = 'FROM'
    Where = 'WHERE'
    Select = 'SELECT'
    Update = 'UPDATE'
    Insert = 'INSERT INTO'
    Values = 'VALUES'
    Join = 'JOIN'
    Set = 'SET'
    As = 'AS'

    def __init__(self, options=SqlDialectOptions()):
        self.options = options

    # noinspection PyUnusedLocal
    def escape(self, value, unquoted=True):
        if type(value) is dict:
            key = get_first_key(value)
            if key.startswith('$'):
                func = getattr(self, '__' + key[1:] + '__')
                if func is not None:
                    params = value[key]
                    if type(params) is list:
                        return func(*params)
                    else:
                        return func(self, params)
            elif value[key] == 1:
                # return object name
                return self.escape_name(key)
        if type(value) is str and value.startswith('$'):
            return self.escape_name(value)
        return SqlUtils.escape(value)

    # noinspection PyMethodMayBeStatic
    def escape_constant(self, value):
        return SqlUtils.escape(value)

    def escape_name(self, value):
        name = value if value.startswith('$') is False else value[1:]
        return ObjectNameValidator().escape(name, self.options.name_format)

    # noinspection PyMethodOverriding
    def __eq__(self, left, right):
        final_right = self.escape(right)
        if final_right == 'NULL':
            return f'{self.escape(left)} IS NULL'
        return f'{self.escape(left)}={final_right}'

    # noinspection PyMethodOverriding
    def __ne__(self, left, right):
        final_right = self.escape(right)
        if final_right == 'NULL':
            return f'NOT {self.escape(left)} IS NULL'
        return f'{self.escape(left)}<>{final_right}'

    def __gt__(self, left, right):
        return f'{self.escape(left)}>{self.escape(right)}'

    def __gte__(self, left, right):
        return f'{self.escape(left)}>={self.escape(right)}'

    def __lt__(self, left, right):
        return f'{self.escape(left)}<{self.escape(right)}'

    def __lte__(self, left, right):
        return f'{self.escape(left)}<={self.escape(right)}'

    def __floor__(self, expr):
        return f'FLOOR({self.escape(expr)})'

    def __ceil__(self, expr):
        return f'CEILING({self.escape(expr)})'

    def __round__(self, expr, digits=0):
        return f'ROUND({self.escape(expr)},{self.escape(digits)})'

    def __and__(self, *args):
        exprs = []
        for arg in args:
            exprs.append(self.escape(arg))
        result = '('
        result += ' AND '.join(exprs)
        result += ')'
        return result

    def __or__(self, *args):
        exprs = []
        for arg in args:
            exprs.append(self.escape(arg))
        result = '('
        result += ' OR '.join(exprs)
        result += ')'
        return result

    def __length__(self, expr):
        return f'LENGTH({self.escape(expr)})'

    def __trim__(self, expr):
        return f'TRIM({self.escape(expr)})'

    def __concat__(self, *args):
        exprs = []
        for arg in args:
            exprs.append(self.escape(arg))
        params_str = ','.join(exprs)
        return f'CONCAT({params_str})'

    def __indexOfBytes__(self, expr, search):
        return f'(LOCATE({self.escape(search)},{self.escape(expr)}) - 1)'

    def __substr__(self, expr, pos, length=None):
        if length is None:
            return f'(SUBSTRING({self.escape(expr)},{self.escape(pos)} + 1))'
        return f'(SUBSTRING({self.escape(expr)},{self.escape(pos)} + 1, {self.escape(length)}))'

    def __toLower__(self, expr):
        return f'(LOWER({self.escape(expr)})'

    def __year__(self, expr):
        return f'(YEAR({self.escape(expr)})'

    def __month__(self, expr):
        return f'(MONTH({self.escape(expr)})'

    def __dayOfMonth__(self, expr):
        return f'(DAY({self.escape(expr)})'

    def __hour__(self, expr):
        return f'(HOUR({self.escape(expr)})'

    def __minute__(self, expr):
        return f'(MINUTE({self.escape(expr)})'

    def __second__(self, expr):
        return f'(SECOND({self.escape(expr)})'

    def __add__(self, **args):
        exprs = []
        for arg in args:
            exprs.append(self.escape(arg))
        result = '('
        result += '+'.join(exprs)
        result += ')'
        return result

    def __subtract__(self, **args):
        exprs = []
        for arg in args:
            exprs.append(self.escape(arg))
        result = '('
        result += '-'.join(exprs)
        result += ')'
        return result

    def __multiply__(self, *args):
        exprs = []
        for arg in args:
            exprs.append(self.escape(arg))
        result = '('
        result += '*'.join(exprs)
        result = ')'
        return result

    def __divide__(self, *args):
        exprs = []
        for arg in args:
            exprs.append(self.escape(arg))
        result = '('
        result += '/'.join(exprs)
        result += ')'
        return result

    def __modulo__(self, *args):
        exprs = []
        for arg in args:
            exprs.append(self.escape(arg))
        result = '('
        result += '%%'.join(exprs)
        result += ')'
        return result


class SqlFormatter:
    def __init__(self, dialect=None):
        self.__dialect__ = SqlDialect() if dialect is None else dialect

    def format_join(self, query: QueryExpression):
        expect(query.__collection__).to_be_truthy(Exception('Expected query collection'))
        sql = ''
        if hasattr(query, '__lookup__'):
            collection = query.__collection__.collection
            alias = query.__collection__.alias
            joins = getattr(query, '__lookup__')
            for join in joins:
                lookup: dict = join['$lookup']
                local_field = lookup.__getitem__('localField') if lookup.__contains__('localField') else None
                foreign_field = lookup.__getitem__('foreignField') if lookup.__contains__('foreignField') else None
                pipeline = lookup.__getitem__('pipeline') if lookup.__contains__('pipeline') else None
                from_collection = lookup['from']
                as_collection = lookup['as']
                sql = lookup['direction'].upper()  # LEFT INNER or RIGHT
                sql += SqlDialect.Space
                sql += SqlDialect.Join
                sql += SqlDialect.Space
                sql += self.__dialect__.escape_name(from_collection)
                if as_collection is not None:
                    sql += SqlDialect.Space
                    sql += self.__dialect__.escape_name(as_collection)
                sql += SqlDialect.Space
                sql += 'ON'
                sql += SqlDialect.Space
                if local_field is not None:
                    sql += self.__dialect__.escape_name((alias or collection) + '.' + local_field)
                    sql += '='
                    sql += self.__dialect__.escape_name((as_collection or from_collection) + '.' + foreign_field)
                elif pipeline is not None:
                    match = pipeline['$match']
                    expect(match).to_be_truthy(TypeError('Pipeline match expression cannot be empty'))
                    expr = match['$expr']
                    expect(expr).to_be_truthy(TypeError('Expected a valid match express'))
                    sql += self.format_where(expr)
        return sql

    def format_select(self, query: QueryExpression):
        expect(query.__collection__).to_be_truthy(Exception('Expected query collection'))
        # get collection name
        collection = query.__collection__.collection
        # and collection alias
        collection_alias = query.__collection__.alias

        sql = SqlDialect.Select
        if query.__select__ is None:
            sql += ' * '  # wildcard select
        else:
            fields = []
            for key in query.__select__:
                if query.__select__[key] == 1:
                    fields.append(self.__dialect__.escape_name(key))
                else:
                    fields.append(self.__dialect__.escape(query.__select__[key]) +
                                  SqlDialect.Space +
                                  SqlDialect.As +
                                  SqlDialect.Space +
                                  self.__dialect__.escape_name(key))
            sql += SqlDialect.Space
            sql += ','.join(fields)
            sql += SqlDialect.Space
        sql += SqlDialect.From
        sql += SqlDialect.Space

        sql += self.__dialect__.escape_name(collection)
        # append alias, if any
        if collection_alias is not None:
            sql += SqlDialect.Space
            sql += self.__dialect__.escape_name(collection_alias)
            sql += SqlDialect.Space
        if query.__where__ is not None:
            sql += SqlDialect.Space
            sql += SqlDialect.Where
            sql += SqlDialect.Space
            sql += self.format_where(query.__where__)

        join_sql = self.format_join(query)
        if len(join_sql) > 0:
            sql += SqlDialect.Space
            sql += join_sql
        return sql

    def format_update(self, query: QueryExpression):
        expect(query.__collection__).to_be_truthy(Exception('Expected query collection'))
        expect(query.__update__).to_be_truthy(Exception('Expected a valid update expression'))
        sql = SqlDialect.Update
        sql += SqlDialect.Space
        sql += self.__dialect__.escape_name(query.__collection__.collection)
        expect(query.__where__).to_be_truthy(
            Exception('Where expression cannot be empty while formatting an update expression'))
        # format set
        sql += SqlDialect.Space
        sql += SqlDialect.Set
        sql += SqlDialect.Space

        if type(query.__update__) is dict:
            items = query.__update__.items()
        else:
            items = query.__update__.__dict__.items()
        update_obj = ''
        for key, value in items:
            final_key = self.__dialect__.escape_name(key)
            final_value = self.__dialect__.escape(value)
            update_obj += f',{final_key}={final_value}'
        sql += update_obj[1:]
        # format where
        sql += SqlDialect.Space
        sql += SqlDialect.Where
        sql += SqlDialect.Space
        sql += self.format_where(query.__where__)
        return sql

    def format_insert(self, query: QueryExpression):
        expect(query.__collection__).to_be_truthy(Exception('Expected query collection'))
        expect(query.__insert__).to_be_truthy(Exception('Expected a valid update expression'))
        sql = SqlDialect.Insert
        sql += SqlDialect.Space
        sql += self.__dialect__.escape_name(query.__collection__.collection)

        values = []
        keys = []
        # get keys and values
        if type(query.__insert__) is dict:
            pairs = query.__insert__.items()
        else:
            pairs = query.__insert__.__dict__.items()
        for key, value in pairs:
            keys.append(self.__dialect__.escape_name(key))
            values.append(self.__dialect__.escape(value))

        # format keys
        sql += '('
        sql += ','.join(keys)
        sql += ')'

        # format values
        sql += SqlDialect.Space
        sql += SqlDialect.Values
        sql += SqlDialect.Space

        sql += '('
        sql += ','.join(values)
        sql += ')'

        return sql

    def format_delete(self, query: QueryExpression):
        raise NotImplementedError()

    def format_where(self, where):
        return self.__dialect__.escape(where)
