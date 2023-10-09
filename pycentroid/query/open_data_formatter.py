import re
from .sql_formatter import SqlFormatter, SqlDialect
from .query_field import get_first_key


class NotSupportedException(Exception):
    def __init__(self, message='This operation is not supported by OData protocol'):
        self.message = message
        super().__init__(self.message)


# noinspection PyMethodMayBeStatic
class OpenDataDialect(SqlDialect):
    def __init__(self):
        super().__init__()

    def __format_name__(self, value):
        name = re.sub(r'\.', '/', value)
        return name if name.startswith('$') is False else name[1:]

    def __eq__(self, left, right):
        final_right = self.escape(right)
        if final_right == 'null':
            return f'{self.escape(left)} eq null'
        return f'({self.escape(left)} eq {final_right})'

    def __ne__(self, left, right):
        final_right = self.escape(right)
        if final_right == 'null':
            return f'NOT {self.escape(left)} ne null'
        return f'(NOT {self.escape(left)} ne {final_right})'
    
    def __gt__(self, left, right):
        return f'({self.escape(left)} gt {self.escape(right)})'

    def __gte__(self, left, right):
        return f'({self.escape(left)} ge {self.escape(right)})'

    def __lt__(self, left, right):
        return f'({self.escape(left)} lt {self.escape(right)})'

    def __lte__(self, left, right):
        return f'({self.escape(left)} le {self.escape(right)})'

    def __floor__(self, expr):
        return f'floor({self.escape(expr)})'

    def __ceil__(self, expr):
        return f'ceiling({self.escape(expr)})'

    def __round__(self, expr, digits=0):
        return f'round({self.escape(expr)},{self.escape(digits)})'

    def __and__(self, *args):
        exprs = []
        for arg in args:
            exprs.append(self.escape(arg))
        result = '('
        result += ' and '.join(exprs)
        result += ')'
        return result

    def __or__(self, *args):
        exprs = []
        for arg in args:
            exprs.append(self.escape(arg))
        result = '('
        result += ' or '.join(exprs)
        result += ')'
        return result

    def __count__(self, expr):
        return f'count({self.escape(expr)})'

    def __min__(self, expr):
        return f'min({self.escape(expr)})'

    def __max__(self, expr):
        return f'max({self.escape(expr)})'

    def __avg__(self, expr):
        return f'avg({self.escape(expr)})'

    def __sum__(self, expr):
        return f'sum({self.escape(expr)})'

    def __length__(self, expr):
        return f'length({self.escape(expr)})'

    def __trim__(self, expr):
        return f'TRIM({self.escape(expr)})'

    def __concat__(self, *args):
        exprs = []
        for index, arg in enumerate(args):
            exprs.append(self.escape(arg))
        params_str = ','.join(exprs)
        return f'concat({params_str})'

    def __indexOfBytes__(self, expr, search):
        return f'indexof({self.escape(search)},{self.escape(expr)})'

    def __substr__(self, expr, pos, length=None):
        if length is None:
            return f'substring({self.escape(expr)},{self.escape(pos)})'
        return f'substring({self.escape(expr)},{self.escape(pos)},{self.escape(length)})'

    def __toLower__(self, expr):
        return f'tolower({self.escape(expr)})'

    def __toUpper__(self, expr):
        return f'toupper({self.escape(expr)})'

    def __year__(self, expr):
        return f'year({self.escape(expr)})'

    def __month__(self, expr):
        return f'month({self.escape(expr)})'

    def __dayOfMonth__(self, expr):
        return f'day({self.escape(expr)})'

    def __hour__(self, expr):
        return f'hour({self.escape(expr)})'

    def __minute__(self, expr):
        return f'minute({self.escape(expr)})'

    def __second__(self, expr):
        return f'second({self.escape(expr)})'

    def __add__(self, *args):
        exprs = []
        for arg in args:
            exprs.append(self.escape(arg))
        result = '('
        result += ' add '.join(exprs)
        result += ')'
        return result

    def __subtract__(self, *args):
        exprs = []
        for arg in args:
            exprs.append(self.escape(arg))
        result = '('
        result += ' sub '.join(exprs)
        result += ')'
        return result

    def __multiply__(self, *args):
        exprs = []
        for arg in args:
            exprs.append(self.escape(arg))
        result = '('
        result += ' mul '.join(exprs)
        result += ')'
        return result

    def __divide__(self, *args):
        exprs = []
        for arg in args:
            exprs.append(self.escape(arg))
        result = '('
        result += ' div '.join(exprs)
        result += ')'
        return result

    def __modulo__(self, *args):
        exprs = []
        for arg in args:
            exprs.append(self.escape(arg))
        result = '('
        result += ' mod '.join(exprs)
        result += ')'
        return result

    def __regexMatch__(self, input, regex, options=None):
        if regex.startswith('^'):
            return f'startswith({self.escape(input)},{self.escape(regex[1:])})'
        elif regex.endswith('$'):
            return f'endswith({self.escape(input)},{self.escape(regex[:-1])})'
        else:
            return f'contains({self.escape(input)},{self.escape(regex)})'

    def __cond__(self, *args):
        return f'case({self.escape(args[0])}:{self.escape(args[1])},true:{self.escape(args[2])})'

    def __switch__(self, expr):
        branches = expr.branches
        if len(branches) == 0:
            raise Exception('Switch branches cannot be empty')
        s = 'case('
        for index, branch in enumerate(branches):
            if index > 0:
                s += ','
            s += f'{self.escape(branch.case)}:{self.escape(branch.then)}'
        if 'default' in expr:
            default_value = expr.default
            s += ',true:'
            s += self.escape(default_value)
        s += ')'
        return s

    def __now__(self):
        return 'now()'

    def __me__(self):
        return 'me()'

    def __whoami__(self):
        return 'whoami()'


class OpenDataFormatter(SqlFormatter):
    def __init__(self):
        super().__init__()
        self.__dialect__ = OpenDataDialect()

    def format_delete(self, query):
        raise NotSupportedException()

    def format_update(self, query):
        raise NotSupportedException()

    def format_insert(self, query):
        raise NotSupportedException()

    def format_group_by(self, query):
        if query.__group_by__ is None:
            return None
        if len(query.__group_by__) == 0:
            return None
        return ','.join(map(lambda x: self.__dialect__.escape(x), query.__group_by__))

    def format_where(self, where):
        if where is None:
            return None
        return self.__dialect__.escape(where)

    def format_limit_select(self, query):
        result = self.format_select(query)
        if query.__limit__ > 0:
            result.update({
                '$count': True
            })
            result.update({
                '$top': query.__limit__
            })
        if query.__skip__ > 0:
            result.update({
                '$skip': query.__skip__
            })
        return result

    def format_order(self, query):

        def format_direction(direction: int | str):
            if direction == -1 or direction == 'desc':
                return 'desc'
            return 'asc'

        if query.__order_by__ is None:
            return None
        if len(query.__order_by__) == 0:
            return None
        res = ','.join(
            map(lambda x: self.__dialect__.escape(x.get('$expr')) + ' ' + format_direction(x.get('direction')), query.__order_by__))
        return res

    def format_select(self, query):
        result = {}
        if query.__select__ is not None and len(query.__select__) > 0:
            fields = []
            for key in query.__select__:
                if query.__select__[key] == 1:
                    fields.append(self.__dialect__.escape_name(self.__dialect__.__format_name__(key)))
                else:
                    fields.append(self.__dialect__.escape(query.__select__[key]) +
                                  ' as ' +
                                  self.__dialect__.escape_name(key))
            result.update({
                '$select': ','.join(fields)
            })
        # format filter
        _filter = self.format_where(query.__where__)
        if _filter is not None:
            result.update({
                '$filter': _filter
            })
        # format group by
        group_by = self.format_group_by(query)
        if group_by is not None:
            result.update({
                '$groupby': group_by
            })
        # format order by
        order_by = self.format_order(query)
        if order_by is not None:
            result.update({
                '$orderby': order_by
            })

        expand = self.format_expand(query)
        if expand is not None:
            result.update({
                '$expand': expand
            })

        return result

    def format_expand(self, query):
        """Formats expand segment of the given query

        Args:
            query (QueryExpression): An instance of query expression

        Returns:
            (string | None): Returns a string which represents an OData system query option
        """
        if len(query.__expand__) == 0:
            return None
        results = []
        for expand in query.__expand__:
            # get expand params
            params = self.format_select(expand)
            collection = get_first_key(expand.__collection__).split('.')
            expr = ''
            for index, item in enumerate(collection):
                if index > 0:
                    expr += '('
                    expr += '$expand'
                    expr += '='
                    expr += item
                else:
                    expr = item
            param_keys = list(filter(lambda key: params[key] is not None, params.keys()))
            if len(param_keys) > 0:
                expr += '('
                expr += ';'.join(map(lambda key: key + '=' + params[key], param_keys))
                expr += ')'
            if len(collection) > 1:
                expr += ')' * (len(collection) - 1)
            results.append(expr)
        return ','.join(results)
