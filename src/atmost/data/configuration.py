from .base import DataApplicationBase
from .service import ApplicationService
from os.path import abspath, join

class DataConfiguration(ApplicationService):
    def __init__(self, application: DataApplicationBase):
        super().__init__(application)
        self.__configuration_dir__ = abspath(join(application.cwd, 'config'))

