import pytest
from unittest import TestCase
from atmost.common import ConfigurationBase, ConfigurationStrategy
from os import getcwd
from os.path import join


class TestStrategy(ConfigurationStrategy):
    remote = 'https://example.com'


def test_create_configuration():
    configuration = ConfigurationBase()
    TestCase().assertEqual(configuration.cwd, join(getcwd(), 'config'))


def test_configuration_use_strategy():
    configuration = ConfigurationBase()
    configuration.usestrategy(TestStrategy)
    TestCase().assertTrue(configuration.hasstrategy(TestStrategy))
    test: TestStrategy = configuration.getstrategy(TestStrategy)
    TestCase().assertIsNotNone(test)
    TestCase().assertEqual(test.remote, 'https://example.com')


def test_configuration_get_section():
    configuration = ConfigurationBase()
    section = configuration.get('settings/mail')
    TestCase().assertIsNone(section)
    configuration.set('settings/mail', {
        'host': '127.0.0.1',
        'port': 25
    })
    section = configuration.get('settings/mail')
    TestCase().assertIsNotNone(section)
    configuration.set('settings/remote', {
        'server': 'https://api.example.com/'
    })
    TestCase().assertEqual(section.get('host'), '127.0.0.1')
    value = configuration.get('settings/mail/port')
    TestCase().assertEqual(value, 25)
    TestCase().assertEqual(configuration.get('settings/remote/server'), 'https://api.example.com/')

