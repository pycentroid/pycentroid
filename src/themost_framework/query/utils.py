import re
from ..common.expect import expect
from datetime import datetime
from dateutil.relativedelta import relativedelta


class SqlUtils:

    def escape(value):
        """Escapes any value to an equivalent sql string

        Args:
            value (*): A value to escqpe

        Returns:
                value (str) An escaped sql string
        """
        if value is None:
            return 'NULL';
        # escape string
        if type(value) is str:
            return '\'' + value + '\''
        # escape boolean
        if type(value) is bool:
            return 'true' if value == True else 'false'
        # escape boolean
        if type(value) is int or type(value) is float:
            return str(value)
        if type(value) is datetime:
            return SqlUtils.date_to_string(value, timezone)
    
    def convert_timezone(tz):
        """Converts a timezone to a time offset e.g. -120, +60 in minutes

        Args:
            tz (str): A string which represents a timezone
        
        Returns:
                (int) An integer which represents the time offset
        """
        if tz == 'Z':
            return 0
        matches = re.match("([+\-\s])(\d\d):?(\d\d)?", tz)
        if matches is None:
            Exception('Invalid timezone expression.')
        sign = -1 if matches[1] == '-' else 1
        hours = int(matches[2])
        minutes = 0 if matches[3] is None else int(matches[3])
        return sign * (hours * 60 + minutes)

    def date_to_string(value, timezone = None):
        if type(value) is None:
            raise TypeError('Expected a valid datetime value')
        if timezone is None:
            return value.strftime('%Y-%m-%d %H:%M:%S')
        else:
            n = SqlUtils.convert_timezone(timezone)
            relative = value + relativedelta(minutes = n)
            return relative.strftime('%Y-%m-%d %H:%M:%S')

