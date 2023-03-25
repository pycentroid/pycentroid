from themost_framework.query import SqlDialect, SqlFormatter


class SqliteDialect(SqlDialect):
    def __init__(self):
        super().__init__(name_format=r'"\1"', force_alias=True)
    
    def __ceil__(self, expr):
        return f'CEIL({self.escape(expr)})'

    def __indexOfBytes__(self, expr, search):
        return f'(INSTR({self.escape(search)},{self.escape(expr)}) - 1)'
    
    def __concat__(self, *args):
        exprs = []
        for arg in args:
            exprs.append(f'IFNULL({self.escape(arg)},\'\')')
        params_str = ' || '.join(exprs)
        return f'CONCAT({params_str})'
    
    def __substr__(self, expr, pos, length=None):
        if length is None:
            return f'SUBSTR({self.escape(expr)},{self.escape(pos)} + 1)'
        return f'SUBSTR({self.escape(expr)},{self.escape(pos)} + 1, {self.escape(length)})'
    
    def __year__(self, expr):
        return f'CAST(strftime(\'%Y\',{self.escape(expr)}) AS INTEGER)'
    
    def __month__(self, expr):
        return f'CAST(strftime(\'%m\',{self.escape(expr)}) AS INTEGER)'
    
    def __dayOfMonth__(self, expr):
        return f'CAST(strftime(\'%d\',{self.escape(expr)}) AS INTEGER)'
    
    def __hour__(self, expr):
        return f'CAST(strftime(\'%H\',{self.escape(expr)}) AS INTEGER)'
    
    def __minute__(self, expr):
        return f'CAST(strftime(\'%M\',{self.escape(expr)}) AS INTEGER)'
    
    def __second__(self, expr):
        return f'CAST(strftime(\'%S\',{self.escape(expr)}) AS INTEGER)'

    def __if_null__(self, expr, default_value):
        return f'IFNULL({self.escape(expr)},{self.escape(default_value)})'
    
    def __to_string__(self, expr):
        return f'CAST({self.escape(expr)} as TEXT)'



class SqliteFormatter(SqlFormatter):
    def __init__(self):
        super().__init__(SqliteDialect())
    
    
