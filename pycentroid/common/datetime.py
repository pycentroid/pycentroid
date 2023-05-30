from datetime import datetime, date, timedelta, timezone
import re

# noinspection PyPep8
timestamp_pattern = r'^(\d{4})-([01]\d)-([0-3]\d)[\sT]([0-2]\d):([0-5]\d):([0-5]\d)(\.(?P<millisecond>\d+))?((?P<timezone>[+-][0-2]\d:[0-5]\d)|Z)$'  # noqa:E501

date_only_pattern = r'^(\d{4})-([01]\d)-([0-3]\d)$'


def isdatetime(value) -> bool:
    """Validates if the given value is datetime

    Args:
        value (datetime | str): The value to check

    Returns:
        bool: Returns true if the given value is an instance of datetime or an ISO formatted date string
    """
    if isinstance(value, datetime):
        return True
    elif type(value) is str:
        return re.search(timestamp_pattern, value) is not None
    return False


def getdatetime(value):
    """Converts the given parameter to datetime

    Args:
        value (date | str): The value to convert from

    Returns:
        (datetime | None): Returns an instance of datetime or None
    """
    if isinstance(value, datetime):
        return value
    elif type(value) is str:
        match = re.search(timestamp_pattern, value)
        if match is not None:
            args = list()
            for x in range(6):
                args.append(int(match.group(x + 1)))
            # get milliseconds
            if match.group('millisecond') is not None:
                args.append(int(match.group('millisecond')))
            else:
                args.append(0)
            # get timezone
            if match.group('timezone') is not None:
                tz = match.group('timezone')
                args.append(timezone(timedelta(hours=int(tz[:3]), minutes=int(tz[4:]))))
            else:
                args.append(timezone.utc)
            return datetime(*args)
    return None


def isdate(value):
    """Validates if the given value is date

    Args:
        value (datetime | str): The value to check

    Returns:
        bool: Returns true if the given value is an instance of date or an ISO formatted date only string
    """
    if isinstance(value, date):
        return True
    elif type(value) is str:
        return re.search(date_only_pattern, value) is not None
    return False


def getdate(value):
    """Converts the given parameter to date

    Args:
        value (date | str): The value to convert from

    Returns:
        (date | None): Returns an instance of date or None
    """
    if isinstance(value, date):
        return value
    elif type(value) is str:
        match = re.search(date_only_pattern, value)
        if match is not None:
            args = list()
            for x in range(3):
                args.append(int(match.group(x + 1)))
            return date(*args)
    return None


def year(value) -> int:
    """Returns an integer that represents the year of the specified value.

    Args:
        value (date | datetime | str): The value to extract from

    Returns:
        int: The year of the specified date.
    """
    if isdate(value):
        return getdate(value).year
    d = getdatetime(value)
    return None if isinstance(d, datetime) else d.year


def month(value):
    """Returns an integer that represents the month of the specified value.

    Args:
        value (date | datetime | str): The value to extract from

    Returns:
        int: The month of the specified date.
    """
    if isdate(value):
        return getdate(value).month
    d = getdatetime(value)
    return None if isinstance(d, datetime) else d.month


def day(value):
    """Returns an integer that represents the day of month of the specified value.

    Args:
        value (date | datetime | str): The value to extract from

    Returns:
        int: The day of the specified date.
    """
    if isdate(value):
        return getdate(value).day
    d = getdatetime(value)
    return None if isinstance(d, datetime) else d.day


def hour(value):
    """Returns an integer that represents the hour of the specified value.

    Args:
        value (datetime | str): The value to extract from

    Returns:
        int: The hour of the specified date.
    """
    d = getdatetime(value)
    return None if isinstance(d, datetime) else d.hour


def minute(value):
    """Returns an integer that represents the minute of the specified value.

    Args:
        value (datetime | str): The value to extract from

    Returns:
        int: The minute of the specified date.
    """
    d = getdatetime(value)
    return None if isinstance(d, datetime) else d.minute


def second(value):
    """Returns an integer that represents the second of the specified value.

    Args:
        value (datetime | str): The value to extract from

    Returns:
        int: The second of the specified date.
    """
    d = getdatetime(value)
    return None if isinstance(d, datetime) else d.second
