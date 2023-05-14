from pycentroid.common.datetime import isdatetime, getdatetime, getdate
from datetime import datetime


def test_isdatetime():
    assert isdatetime(datetime.now()) is True
    assert isdatetime('2022-10-20') is False
    assert isdatetime('2022-10-20T14:35:40.000Z') is True
    assert isdatetime('2022-10-20T14:35:40Z') is True
    assert isdatetime('2022-10-20T14:35:40.000+02:00') is True
    assert isdatetime('2022-10-20 14:35:40.000+02:00') is True
    assert isdatetime('2022-10-20 14:35:40+02:00') is True


def test_get_datetime():
    value = getdatetime('2011-11-04T00:05:23.125Z')
    assert value.year == 2011
    assert value.month == 11
    assert value.day == 4
    assert value.hour == 0
    assert value.minute == 5
    assert value.second == 23
    assert value.microsecond == 125
    value = getdatetime('2011-11-04T00:05:23.000+02:00')
    assert value.microsecond == 0


def test_getdate():
    value = getdate('2011-11-04')
    assert value.year == 2011
    assert value.month == 11
    assert value.day == 4
