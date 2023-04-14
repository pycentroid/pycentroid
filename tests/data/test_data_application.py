import pytest
import typing
from atmost.data.application import DataApplication, DataConfiguration
from os.path import abspath, join, dirname
from unittest import TestCase
from atmost.common import ApplicationBase, ApplicationService

APP_PATH = abspath(join(dirname(__file__), '..'))


class TestService(ApplicationService):
    def __init__(self, application):
        super().__init__(application)

    # noinspection PyMethodMayBeStatic
    def get_message(self):
        return 'Hello World!'


class TestServiceB(TestService):

    # noinspection PyMethodMayBeStatic
    def get_message(self):
        return 'Hello World!!!'


def test_get_configuration():
    app = DataApplication(cwd=APP_PATH)
    TestCase().assertIsNotNone(app.configuration)
    TestCase().assertEqual(type(app.configuration), DataConfiguration)
    section = app.configuration.get('settings/mail')
    TestCase().assertIsNotNone(section)


def test_get_service():
    app = DataApplication(cwd=APP_PATH)
    app.services.use(TestService)
    TestCase().assertIsNotNone(app.services.get(TestService))
    message = app.services.get(TestService).get_message()
    TestCase().assertEqual(message, 'Hello World!')


def test_use_service():
    app = DataApplication(cwd=APP_PATH)
    app.services.use(TestService, TestServiceB)
    TestCase().assertIsNotNone(app.services.get(TestService))
    message = app.services.get(TestService).get_message()
    TestCase().assertEqual(message, 'Hello World!!!')


