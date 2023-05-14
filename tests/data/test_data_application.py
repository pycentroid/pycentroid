from pycentroid.data.application import DataApplication, DataConfiguration
from os.path import abspath, join, dirname
from pycentroid.common import ApplicationService

APP_PATH = abspath(join(dirname(__file__), '..'))


class TestService(ApplicationService):
    __test__ = False

    def __init__(self, application):
        super().__init__(application)

    # noinspection PyMethodMayBeStatic
    def get_message(self):
        return 'Hello World!'


class TestServiceB(TestService):
    __test__ = False

    # noinspection PyMethodMayBeStatic
    def get_message(self):
        return 'Hello World!!!'


def test_get_configuration():
    app = DataApplication(cwd=APP_PATH)
    assert app.configuration is not None
    assert type(app.configuration) is DataConfiguration
    section = app.configuration.get('settings/mail')
    assert section is not None


def test_get_service():
    app = DataApplication(cwd=APP_PATH)
    app.services.use(TestService)
    assert app.services.get(TestService) is not None
    message = app.services.get(TestService).get_message()
    assert message == 'Hello World!'


def test_use_service():
    app = DataApplication(cwd=APP_PATH)
    app.services.use(TestService, TestServiceB)
    assert app.services.get(TestService) is not None
    message = app.services.get(TestService).get_message()
    assert message == 'Hello World!!!'

