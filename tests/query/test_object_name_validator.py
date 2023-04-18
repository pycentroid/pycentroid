import pytest
from unittest import TestCase
from centroid.query import ObjectNameValidator, InvalidObjectNameError


def test_object_name():
    TestCase().assertEqual(ObjectNameValidator().test('field1', True, False), True)
    TestCase().assertEqual(ObjectNameValidator().test('Table1.field1', False, False), False)
    TestCase().assertEqual(ObjectNameValidator().test('fie ld1', True, False), False)
    TestCase().assertEqual(ObjectNameValidator().test('Ta ble.field1', True, False), False)
    TestCase().assertRaises(InvalidObjectNameError, lambda :ObjectNameValidator().test('field 1'))


def test_object_name_escape():
    TestCase().assertEqual(ObjectNameValidator().escape('field1'), 'field1')
    TestCase().assertEqual(ObjectNameValidator().escape('Table1.field1'), 'Table1.field1')
    TestCase().assertEqual(ObjectNameValidator().escape('field1', r'[\1]'), '[field1]')
    TestCase().assertEqual(ObjectNameValidator().escape('Table1.field1', r'[\1]'), '[Table1].[field1]')
    TestCase().assertRaises(InvalidObjectNameError, lambda :ObjectNameValidator().escape('Tab le1.field1'))
