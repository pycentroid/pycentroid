import re


def trim_field_reference(name: str):
    return re.sub(r'\$(\w+)', r'\1', name)


def format_collection_reference(name: str):
    return re.sub(r'\$?(\w+)', r'$\1', name)


def format_field_reference(name: str):
    return re.sub(r'\$?(\w+)', r'$\1', name)


def format_any_field_reference(name: str):
    return re.sub(r'^\$?(\w+)', r'$\1', name)


def is_qualified_reference(name: str):
    pattern = r'(\$\w+)\.((\w+)+)?'
    return re.search(pattern, name)


def get_first_key(any_dict: dict):
    """Returns the first key of a dictionary

    Args:
        any_dict (dict): Any dictionary

    Returns:
        str
    """
    for key in any_dict:
        return key


def get_field_expression(expr: dict):
    key: str = get_first_key(expr)
    value = expr[key]
    if type(value) is int and value == 1:
        return format_any_field_reference(key)
    return value


class QueryField(dict):
    def __init__(self, name: str = None):
        super().__init__()
        if type(name) is str:
            self.__setitem__(trim_field_reference(name), 1)

    def from_collection(self, collection: str):
        key: str = get_first_key(self)
        if key is None:
            raise 'Field name cannot be empty when defining collection'
        if key.startswith('$'):
            raise 'Unsupported field expression. Define collection before assigning an expression'
        self.__delitem__(key)
        key = trim_field_reference(collection) + '.' + key
        self.__setitem__(key, 1)
        return self

    def asattr(self, alias: str):
        key: str = get_first_key(self)
        if key is None:
            raise 'Field name cannot be empty when defining an alias'
        value = self.__getitem__(key)
        self.__delitem__(key)
        self.__setitem__(alias, {
            key: value
        })
        return self

    def _as(self, alias: str):
        return self.asattr(alias)

    def __use_datetime_function__(self, date_function, timezone=None):
        field: str = get_first_key(self)
        value = self.__getitem__(field)
        if type(value) is int:
            value = format_any_field_reference(field)
        self.__delitem__(field)
        self.__setitem__(date_function, {
            'date': value,
            'timezone': timezone
        })
        return self

    def __use_method_call__(self, method, *args):
        field: str = get_first_key(self)
        value = self.__getitem__(field)
        if type(value) is int and value == 1:
            value = format_any_field_reference(field)
        self.__delitem__(field)
        arguments = [value]
        for a in args:
            if type(a) is QueryField:
                field: str = get_first_key(a)
                value = a.__getitem__(field)
                if type(value) is int and value == 1:
                    arguments.append(format_any_field_reference(field))
                else:
                    arguments.append(a)
            else:
                arguments.append(a)
        self.__setitem__(method, arguments)
        return self

    def get_year(self, timezone=None):
        return self.__use_datetime_function__('$year', timezone)

    def year(self, timezone=None):
        return self.get_year(timezone)

    def get_date(self, timezone=None):
        return self.__use_datetime_function__('$dayOfMonth', timezone)

    def day(self, timezone=None):
        return self.get_date(timezone)

    def get_month(self, timezone=None):
        return self.__use_datetime_function__('$month', timezone)

    def month(self, timezone=None):
        return self.get_month(timezone)

    def get_hours(self, timezone=None):
        return self.__use_datetime_function__('$hour', timezone)

    def hour(self, timezone=None):
        return self.get_hours(timezone)

    def get_minutes(self, timezone=None):
        return self.__use_datetime_function__('$minute', timezone)

    def minute(self, timezone=None):
        return self.get_minutes(timezone)

    def get_seconds(self, timezone=None):
        return self.__use_datetime_function__('$second', timezone)

    def second(self, timezone=None):
        return self.get_seconds(timezone)

    def length(self):
        return self.__use_method_call__('$size')

    def len(self):
        return self.__use_method_call__('$size')
    
    def trim(self):
        return self.__use_method_call__('$trim')

    def ceil(self):
        return self.__use_method_call__('$ceil')

    def floor(self):
        return self.__use_method_call__('$floor')

    def round(self, digits):
        return self.__use_method_call__('$round', digits)

    def add(self, value):
        return self.__use_method_call__('$add', value)

    def subtract(self, value):
        return self.__use_method_call__('$subtract', value)

    def multiply(self, value):
        return self.__use_method_call__('$multiply', value)

    def divide(self, value):
        return self.__use_method_call__('$divide', value)

    def modulo(self, value=2):
        return self.__use_method_call__('$mod', value)

    def concat(self, *args):
        return self.__use_method_call__('$concat', *args)

    def substring(self, start, length):
        return self.__use_method_call__('$substr', start, length)

    def index_of(self, search):
        return self.__use_method_call__('$indexOfBytes', search)
    
    def min(self):
        return self.__use_method_call__('$min')

    def max(self):
        return self.__use_method_call__('$max')

    def count(self):
        return self.__use_method_call__('$count')
    
    def sum(self):
        return self.__use_method_call__('$sum')

    def average(self):
        return self.__use_method_call__('$avg')

    def to_lower(self):
        return self.__use_method_call__('$toLower')

    def to_upper(self):
        return self.__use_method_call__('$toUpper')

    def __search__(self, pattern):
        field: str | None = get_first_key(self)
        value = self.__getitem__(field)
        # check if field expression is simple e.g. { "givenName": 1 }
        if type(value) is int and value == 1:
            # use field reference e.g. $givenName
            field = None
            value = format_any_field_reference(field)
        self.__delitem__(field)
        regex_match = {
            'input': value,
            'regex': pattern
        }
        if field is None:
            self['$regexMatch'] = regex_match
        else:
            self[field] = {
                '$regexMatch': regex_match
            }
        return self

    def startswith(self, search: str):
        return self.__search__('^' + re.escape(search))
    
    def endswith(self, search: str):
        return self.__search__(re.escape(search) + '$')
    
    def contains(self, search: str):
        return self.__search__(re.escape(search))
