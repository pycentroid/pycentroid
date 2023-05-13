from pycentroid.common.objects import AnyObject, is_object_like
from datetime import datetime, date
from collections import namedtuple

SAMPLE_SCHEMA = {
    "$schema": "https://themost-framework.github.io/themost/models/2018/2/schema.json",
    "name": "TestAction",
    "title": "TestActions",
    "hidden": False,
    "sealed": False,
    "abstract": False,
    "version": "2.0",
    "inherits": "Action",
    "fields": [
    ],
    "privileges": [
        {
            "mask": 15,
            "type": "global"
        }
    ]
}


def test_new_object():
    obj = AnyObject(**SAMPLE_SCHEMA)
    assert obj.name == 'TestAction'
    assert obj.privileges[0].mask == 15


def test_is_object():
    assert is_object_like(100) is False
    assert is_object_like(4.5) is False
    assert is_object_like(True) is False
    assert is_object_like(date.today()) is False
    assert is_object_like(datetime.today()) is False
    assert is_object_like(dict) is False

    assert is_object_like({}) is True
    Student = namedtuple('Student', ['name', 'age', 'DOB'])
    stud = Student('Nandini', '19', '2541997')
    assert is_object_like(stud) is True
