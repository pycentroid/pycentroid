import pytest

from atmost.data.application import DataApplication, DataConfiguration
from os.path import abspath, join, dirname
from unittest import TestCase

APP_PATH = abspath(join(dirname(__file__), '..'))


def test_create_context():
    app = DataApplication(cwd=APP_PATH)
    context = app.create_context()
    TestCase().assertIsNotNone(context.application)


def test_get_configuration():
    app = DataApplication(cwd=APP_PATH)
    TestCase().assertIsNotNone(app.configuration)
    TestCase().assertEqual(type(app.configuration), DataConfiguration)

