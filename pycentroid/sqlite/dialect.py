from pycentroid.query import SqlDialect, SqlFormatter, SqlDialectOptions

SqlDialectTypes = [
        ['Boolean', 'INTEGER(0,1)'],
        ['Byte', 'INTEGER(0,1)'],
        ['Number', 'REAL'],
        ['Number', 'REAL'],
        ['Counter', 'INTEGER PRIMARY KEY AUTOINCREMENT'],
        ['Currency', 'NUMERIC(19,4)'],
        ['Decimal', 'NUMERIC(?,?)'],
        ['Date', 'NUMERIC'],
        ['DateTime', 'NUMERIC'],
        ['Time', 'TEXT(18,0)'],
        ['Long', 'NUMERIC'],
        ['Duration', 'TEXT(48)'],
        ['Integer', 'INTEGER(?)'],
        ['Url', 'TEXT(?)'],
        ['Text', 'TEXT(?)'],
        ['Note', 'TEXT(?)'],
        ['Image', 'BLOB'],
        ['Binary', 'BLOB'],
        ['Guid', 'TEXT(36,0)'],
        ['Short', 'INTEGER(2,0)']
    ]


class SqliteDialect(SqlDialect):
    def __init__(self):
        super().__init__(SqlDialectOptions(name_format=r'"\1"', force_alias=True))
        # set field type definitions
        self.types = dict()
        for item in SqlDialectTypes:
            self.types[item[0]] = item[1]

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

    def __substr__(self, expr, pos, length=None):
        if length is None:
            return f'SUBSTR({self.escape(expr)},{self.escape(pos)} + 1)'
        return f'SUBSTR({self.escape(expr)},{self.escape(pos)} + 1,{self.escape(length)})'
    
    def __regexMatch__(self, input, regex, options=None):
        match_type = 'm'  # support multiline
        if options is not None and options.__contains__('i'):  # ignore case
            match_type += 'i'
        # allows the dot character to match all characters including newline characters
        if options is not None and options.__contains__('s'):
            match_type += 'n'
        return f'REGEXP_LIKE({self.escape(input)},{self.escape(regex)}, {self.escape(match_type)})'


class SqliteFormatter(SqlFormatter):
    def __init__(self):
        super().__init__(SqliteDialect())
    
    
