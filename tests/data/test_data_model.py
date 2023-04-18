from centroid.data.loaders import SchemaLoaderStrategy
from centroid.data.application import DataApplication
from centroid.common.objects import AnyObject
from os.path import abspath, join, dirname
from unittest import TestCase

APP_PATH = abspath(join(dirname(__file__), '..'))


def test_get_model():
    app = DataApplication(cwd=APP_PATH)
    d: dict = app.configuration.getstrategy(SchemaLoaderStrategy).get('Thing')
    model = AnyObject(**d)
    TestCase().assertEqual(model.name, 'Thing')




