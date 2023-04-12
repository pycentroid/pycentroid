from .context import DataContext
from .base import DataApplicationBase
from .configuration import DataConfiguration
from os import getcwd


class DataApplication(DataApplicationBase):
    def __init__(self, **kwargs):
        super().__init__()
        # get current directory
        cwd = kwargs.get('cwd') or getcwd()
        self.cwd = cwd
        # initialize data configuration
        configuration = DataConfiguration(self)
        self.use(DataConfiguration, configuration)

    def create_context(self):
        return DataContext(self)

    @property
    def configuration(self) -> DataConfiguration:
        return self.get(DataConfiguration)

