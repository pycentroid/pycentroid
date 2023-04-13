from os.path import abspath, join
from atmost.common import ConfigurationBase, ApplicationBase


class DataConfiguration(ConfigurationBase):
    def __init__(self, application: ApplicationBase):
        super().__init__()
        self.__configuration_dir__ = abspath(join(application.cwd, 'config'))


