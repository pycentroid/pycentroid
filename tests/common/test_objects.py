from atmost.common.objects import AnyObject
from unittest import TestCase

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
