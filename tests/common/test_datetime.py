import pytest
from unittest import TestCase
from themost_framework.common.datetime import isdatetime, getdatetime, getdate
from datetime import datetime

def test_isdatetime():
    TestCase().assertEqual(isdatetime(datetime.now()), True)
    TestCase().assertEqual(isdatetime('2022-10-20'), False)
    TestCase().assertEqual(isdatetime('2022-10-20T14:35:40.000Z'), True)
    TestCase().assertEqual(isdatetime('2022-10-20T14:35:40Z'), True)
    TestCase().assertEqual(isdatetime('2022-10-20T14:35:40.000+02:00'), True)
    TestCase().assertEqual(isdatetime('2022-10-20 14:35:40.000+02:00'), True)
    TestCase().assertEqual(isdatetime('2022-10-20 14:35:40+02:00'), True)

def test_getdatetime():
    value = getdatetime('2011-11-04T00:05:23.125Z')
    TestCase().assertEqual(value.year, 2011)
    TestCase().assertEqual(value.month, 11)
    TestCase().assertEqual(value.day, 4)
    TestCase().assertEqual(value.hour, 0)
    TestCase().assertEqual(value.minute, 5)
    TestCase().assertEqual(value.second, 23)
    TestCase().assertEqual(value.microsecond, 125)
    value = getdatetime('2011-11-04T00:05:23.000+02:00')
    TestCase().assertEqual(value.microsecond, 0)

def test_getdate():
    value = getdate('2011-11-04')
    TestCase().assertEqual(value.year, 2011)
    TestCase().assertEqual(value.month, 11)
    TestCase().assertEqual(value.day, 4)