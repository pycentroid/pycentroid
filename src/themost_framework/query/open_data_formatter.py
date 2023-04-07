from .sql_formatter import SqlFormatter, SqlDialect

class NotSupportedException(Exception):
    def __init__(self, message='This operation is not supported by OData protocol'):
        self.message = message
        super().__init__(self.message)


class OpenDataDialect(SqlDialect):
    def __init(self):
        super().__init__(self)

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
    
    def __and__(self, expr, digits=0):
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
        result += ' and '.join(exprs)
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
    
    def __regexMatch__(self, expr, search):
        if search.startswith('^'):
            return f'startswith({self.escape(expr)},{self.escape(search[1:])})'
        elif search.endswith('$'):
            return f'endswith({self.escape(expr)},{self.escape(search[:-1])})'
        else:
            return f'contains({self.escape(expr)},{self.escape(search)})'
    
    def __cond__(self, *args):
        return f'case({self.escape(args[0])}:{self.escape(args[1])},true:{self.escape(args[2])})'

    def __switch__(self, expr):
        branches = self.branches
        if len(branches) == 0:
            raise Exception('Switch branches cannot be empty');
        s = 'case('
        for index,branch in enumerate(branches):
            if index > 0:
                s += ','
            s += f'{self.escape(branch.case)}:{self.escape(branch.then)}'
        if 'default' in expr:
            default_value = expr.default
            s += ',true:';
            s += self.escape(default_value);
        s += ')'
        return s

class OpenDataFormatter(SqlFormatter):
    def __init(self):
        super().__init__(self)
        self.__dialect__ = OpenDataDialect()
    
    def format_delete(self, query):
        raise NotSupportedException()
    
    def format_update(self, query):
        raise NotSupportedException()
    
    def format_insert(self, query):
        raise NotSupportedException()
    


