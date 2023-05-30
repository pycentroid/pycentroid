import re
from typing import Callable
from datetime import datetime
from dateutil.relativedelta import relativedelta
from .object_name_validator import ObjectNameValidator
from .data_objects import DataAdapter


class SelectMap:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


def select(**kwargs):
    return SelectMap(**kwargs)


class CancelTransactionError(Exception):
    def __init__(self):
        super().__init__('cancel')


class TestUtils:
    def __init__(self, db: DataAdapter):
        self.__adapter__ = db

    async def execute_in_transaction(self, func: Callable):
        try:
            async def execute():
                await func()
                raise CancelTransactionError()
            # execute in transaction
            await self.__adapter__.execute_in_transaction(execute)
        except Exception as error:
            if type(error) is not CancelTransactionError:
                raise error


class SqlUtils:

    @staticmethod
    def escape(value, timezone=None):
        """Escapes any value to an equivalent sql string

        Args:
            value (*): A value to escape
            timezone (*): A value to escape

        Returns:
                value (str) An escaped sql string
        """
        if value is None:
            return 'NULL'
        # escape boolean
        if type(value) is bool:
            return 'true' if value is True else 'false'
        # escape boolean
        if type(value) is int or type(value) is float:
            return str(value)
        if type(value) is bytearray:
            return SqlUtils.bytes_to_string(value)
        if type(value) is datetime:
            return SqlUtils.date_to_string(value, timezone)
        if type(value) is dict:
            return SqlUtils.dict_to_values(value, timezone)

        val = str(value)
        # surround with single quotes
        return '\'' + SqlUtils.escape_string(val) + '\''

    @staticmethod
    def escape_string(val):
        if val is None:
            return 'NULL'
        val = re.sub('\'', '\\\'', val)  # ' => \\'
        val = re.sub('\n', '\\\\n', val)  # \n => \\n
        val = re.sub('\r', '\\\\r', val)  # \r => \\r
        val = re.sub('\b', '\\\\b', val)  # \b => \\b
        val = re.sub('\t', '\\\\t', val)  # \t => \\t
        val = re.sub('\0', '\\\\0', val)  # \0 => \\0
        val = re.sub('\x1a', '\\\\Z', val)  # \x1a => \\Z
        val = re.sub('"', '\\"', val)  # " => \"
        val = re.sub('\\\\', '\\\\\\\\', val)  # \\ => \\\\
        return val

    @staticmethod
    def convert_timezone(tz):
        """Converts a timezone to a time offset e.g. -120, +60 in minutes

        Args:
            tz (str): A string which represents a timezone

        Returns:
                (int) An integer which represents the time offset
        """
        if tz == 'Z':
            return 0
        matches = re.match("([+\\-\\s])(\\d\\d):?(\\d\\d)?", tz)
        if matches is None:
            Exception('Invalid timezone expression.')
        sign = -1 if matches[1] == '-' else 1
        hours = int(matches[2])
        minutes = 0 if matches[3] is None else int(matches[3])
        return sign * (hours * 60 + minutes)

    @staticmethod
    def bytes_to_string(value):
        """Converts a bytearray to an equivalent hex string

        Args:
            value (bytearray): The array of bytes to convert

        Raises:
            TypeError: Expected a valid bytearray

        Returns:
            str: The equivalent hex string
        """
        if not type(value) is bytearray:
            raise TypeError('Expected a valid bytearray')
        return ''.join('{:02x}'.format(x) for x in value)

    @staticmethod
    def dict_to_values(value, timezone=None):
        """Converts a dictionary object to an equivalent sequence of SQL fields values

        Args:
            value (dict): The dictionary object to convert
            timezone (str, optional): An optional timezone expression to use while converting datetime values

        Raises:
            TypeError: Expected a valid dictionary

        Returns:
            str: The equivalent SQL expression
        """
        if not type(value) is dict:
            raise TypeError('Expected a valid dictionary')
        result = ''
        for key in value:
            final_key = ObjectNameValidator().escape(key)
            final_value = SqlUtils.escape(value[key], timezone)
            result += f',{final_key}={final_value}'
        return result[1:]

    @staticmethod
    def object_to_values(value, timezone=None):
        """Converts an object to an equivalent sequence of SQL fields values

        Args:
            value (obj): The object to convert
            timezone (str, optional): An optional timezone expression to use while converting datetime values

        Raises:
            TypeError: Expected a valid object

        Returns:
            str: The equivalent SQL expression
        """
        if not type(value) is object:
            raise TypeError('Expected a valid object')
        result = ''
        for key, value in object.__dict__.items():
            final_key = ObjectNameValidator().escape(key)
            final_value = SqlUtils.escape(value, timezone)
            result += f',{final_key}={final_value}'
        return result[1:]

    @staticmethod
    def date_to_string(value, timezone=None):
        if type(value) is None:
            raise TypeError('Expected a valid datetime value')
        if timezone is None:
            return value.strftime('%Y-%m-%d %H:%M:%S')
        else:
            n = SqlUtils.convert_timezone(timezone)
            relative = value + relativedelta(minutes=n)
            return relative.strftime('%Y-%m-%d %H:%M:%S')
