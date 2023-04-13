import pytest
from unittest import TestCase
from atmost.common import ConfigurationBase, ConfigurationStrategy
from os import getcwd


class TestStrategy(ConfigurationStrategy):
    remote = 'https://example.com'


def test_create_configuration():
    configuration = ConfigurationBase()
    TestCase().assertEqual(configuration.cwd, getcwd())


def test_configuration_use_strategy():
    configuration = ConfigurationBase()
    configuration.usestrategy(TestStrategy)
    TestCase().assertTrue(configuration.hasstrategy(TestStrategy))
    test: TestStrategy = configuration.getstrategy(TestStrategy)
    TestCase().assertIsNotNone(test)
    TestCase().assertEqual(test.remote, 'https://example.com')
