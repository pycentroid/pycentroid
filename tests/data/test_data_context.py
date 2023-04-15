import pytest
from atmost.data.application import DataApplication
from os.path import abspath, join, dirname
from unittest import TestCase
from atmost.common import ApplicationBase, ApplicationService

APP_PATH = abspath(join(dirname(__file__), '..'))


def test_create_context():
    app = DataApplication(cwd=APP_PATH)
    context = app.create_context()
    TestCase().assertIsNotNone(context.application)
    TestCase().assertIsNotNone(context.db)
    context.finalize()



