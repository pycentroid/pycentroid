import pytest
from pycentroid.query import ObjectNameValidator, InvalidObjectNameError


def test_object_name():
    assert ObjectNameValidator().test('field1', True, False)
    assert ObjectNameValidator().test('Table1.field1', False, False) is False
    assert ObjectNameValidator().test('fie ld1', True, False) is False
    assert ObjectNameValidator().test('Ta ble.field1', True, False) is False
    with pytest.raises(InvalidObjectNameError):
        ObjectNameValidator().test('field 1')


def test_object_name_escape():
    assert ObjectNameValidator().escape('field1') == 'field1'
    assert ObjectNameValidator().escape('Table1.field1') == 'Table1.field1'
    assert ObjectNameValidator().escape('field1', r'[\1]') == '[field1]'
    assert ObjectNameValidator().escape('Table1.field1', r'[\1]') == '[Table1].[field1]'
    with pytest.raises(InvalidObjectNameError):
        ObjectNameValidator().escape('Tab le1.field1')
