from pycentroid.common import ConfigurationBase, ConfigurationStrategy
from os import getcwd
from os.path import join


class TestStrategy(ConfigurationStrategy):
    __test__ = False

    def __init__(self, configuration):
        super().__init__(configuration)
        self.remote = 'https://example.com'


def test_create_configuration():
    configuration = ConfigurationBase()
    assert configuration.cwd == join(getcwd(), 'config')


def test_configuration_use_strategy():
    configuration = ConfigurationBase()
    configuration.usestrategy(TestStrategy)
    assert configuration.hasstrategy(TestStrategy) is True
    test: TestStrategy = configuration.getstrategy(TestStrategy)
    assert test is not None
    assert test.remote == 'https://example.com'


def test_configuration_get_section():
    configuration = ConfigurationBase()
    section = configuration.get('settings/mail')
    assert section is None
    configuration.set('settings/mail', {
        'host': '127.0.0.1',
        'port': 25
    })
    section = configuration.get('settings/mail')
    assert section is not None
    configuration.set('settings/remote', {
        'server': 'https://api.example.com/'
    })
    assert section.get('host') == '127.0.0.1'
    value = configuration.get('settings/mail/port')
    assert value == 25
    assert configuration.get('settings/remote/server') == 'https://api.example.com/'
