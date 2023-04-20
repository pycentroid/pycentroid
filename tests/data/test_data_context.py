import pytest
from centroid.data.application import DataApplication
from os.path import abspath, join, dirname
from unittest import TestCase
from centroid.common import ApplicationBase, ApplicationService

APP_PATH = abspath(join(dirname(__file__), '..'))


def test_create_context():
    app = DataApplication(cwd=APP_PATH)
    context = app.create_context()
    TestCase().assertIsNotNone(context.application)
    TestCase().assertIsNotNone(context.db)
    context.finalize()

def test_get_model():
    app = DataApplication(cwd=APP_PATH)
    context = app.create_context()
    model = context.model('Product')
    TestCase().assertIsNotNone(model)
    TestCase().assertEqual(model.properties.name, 'Product')
    context.finalize()



