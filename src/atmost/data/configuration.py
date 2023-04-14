from os.path import abspath, join, isfile
from os import environ as env
from atmost.common import ConfigurationBase, ApplicationBase
import yaml


class DataConfiguration(ConfigurationBase):
    def __init__(self, application: ApplicationBase):
        super().__init__(abspath(join(application.cwd, 'config')))







