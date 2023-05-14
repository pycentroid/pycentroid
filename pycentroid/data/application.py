from .context import DataContext, DefaultDataContext
from pycentroid.common import ApplicationBase
from .configuration import DataConfiguration
from os import getcwd


class DataApplication(ApplicationBase):
    def __init__(self, **kwargs):
        super().__init__()
        # get current directory
        cwd = kwargs.get('cwd') or getcwd()
        # set current directory
        self.cwd = cwd
        # init data configuration
        self.services.use(DataConfiguration, DataConfiguration(self))

    def create_context(self) -> DataContext:
        return DefaultDataContext(self)

    @property
    def configuration(self) -> DataConfiguration:
        return self.services.get(DataConfiguration)

