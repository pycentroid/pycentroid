from centroid.common.objects import AnyObject, is_object_like
from unittest import TestCase
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
    TestCase().assertEqual(obj.name, 'TestAction')
    TestCase().assertEqual(obj.privileges[0].mask, 15)


def test_is_object():
    TestCase().assertEqual(is_object_like(100), False)
    TestCase().assertEqual(is_object_like(4.5), False)
    TestCase().assertEqual(is_object_like(True), False)
    TestCase().assertEqual(is_object_like(date.today()), False)
    TestCase().assertEqual(is_object_like(datetime.today()), False)
    TestCase().assertEqual(is_object_like(dict), False)

    TestCase().assertEqual(is_object_like({}), True)
    Student = namedtuple('Student', ['name', 'age', 'DOB'])
    S = Student('Nandini', '19', '2541997')
    TestCase().assertEqual(is_object_like(S), True)
