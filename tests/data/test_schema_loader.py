from pycentroid.data.application import DataApplication
from pycentroid.data.loaders import SchemaLoaderStrategy, FileSchemaLoaderStrategy, DefaultSchemaLoaderStrategy
from os.path import abspath, join, dirname

APP_PATH = abspath(join(dirname(__file__), '..'))


class TestSchemaLoader(FileSchemaLoaderStrategy):
    def __init__(self, configuration):
        super().__init__(configuration)
        self.path = join(dirname(__file__), 'models')


def test_use_read():
    app = DataApplication(cwd=APP_PATH)
    loader = FileSchemaLoaderStrategy(app.configuration)
    files = loader.read()
    assert len(files) > 0
    assert files.index('Action') >= 0


def test_use_get():
    app = DataApplication(cwd=APP_PATH)
    loader = FileSchemaLoaderStrategy(app.configuration)
    model = loader.get('Action')
    assert model is not None
    model = loader.get('Unknown')
    assert model is None


def test_use_default_schema_loader():
    app = DataApplication(cwd=APP_PATH)
    loader: SchemaLoaderStrategy = app.configuration.getstrategy(SchemaLoaderStrategy)
    model = loader.get('Action')
    assert model is not None
    model = loader.get('Unknown')
    assert model is None


def test_use_loaders():
    app = DataApplication(cwd=APP_PATH)
    loader = DefaultSchemaLoaderStrategy(app.configuration)
    model = loader.get('TestAction')
    assert model is None
    app.configuration.set('settings/schema/loaders', [
        {
            'loaderClass': TestSchemaLoader
        }
    ])
    loader = DefaultSchemaLoaderStrategy(app.configuration)
    assert len(loader.loaders) > 0
    model = loader.get('TestAction')
    assert model is not None
