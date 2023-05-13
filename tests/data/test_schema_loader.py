import pytest
from pycentroid.data.application import DataApplication
from pycentroid.data.loaders import SchemaLoaderStrategy, FileSchemaLoaderStrategy, DefaultSchemaLoaderStrategy
from os.path import abspath, join, dirname
from unittest import TestCase

APP_PATH = abspath(join(dirname(__file__), '..'))


class TestSchemaLoader(FileSchemaLoaderStrategy):
    def __init__(self, configuration):
        super().__init__(configuration)
        self.path = join(dirname(__file__), 'models')


def test_use_read():
    app = DataApplication(cwd=APP_PATH)
    loader = FileSchemaLoaderStrategy(app.configuration)
    files = loader.read()
    TestCase().assertGreater(len(files), 0)
    TestCase().assertGreaterEqual(files.index('Action'), 0)


def test_use_get():
    app = DataApplication(cwd=APP_PATH)
    loader = FileSchemaLoaderStrategy(app.configuration)
    model = loader.get('Action')
    TestCase().assertIsNotNone(model)
    model = loader.get('Unknown')
    TestCase().assertIsNone(model)


def test_use_default_schema_loader():
    app = DataApplication(cwd=APP_PATH)
    loader: SchemaLoaderStrategy = app.configuration.getstrategy(SchemaLoaderStrategy)
    model = loader.get('Action')
    TestCase().assertIsNotNone(model)
    model = loader.get('Unknown')
    TestCase().assertIsNone(model)


def test_use_loaders():
    app = DataApplication(cwd=APP_PATH)
    loader = DefaultSchemaLoaderStrategy(app.configuration)
    model = loader.get('TestAction')
    TestCase().assertIsNone(model)
    app.configuration.set('settings/schema/loaders', [
        {
            'loaderClass': TestSchemaLoader
        }
    ])
    loader = DefaultSchemaLoaderStrategy(app.configuration)
    TestCase().assertGreater(len(loader.loaders), 0)
    model = loader.get('TestAction')
    TestCase().assertIsNotNone(model)
