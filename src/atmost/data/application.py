from .context import DataContext
from atmost.common import ApplicationBase
from .configuration import DataConfiguration
from os import getcwd


class DataApplication(ApplicationBase):
    def __init__(self, **kwargs):
        super().__init__()
        # get current directory
        cwd = kwargs.get('cwd') or getcwd()
        self.cwd = cwd
        # initialize data configuration
        configuration = DataConfiguration(self)
        self.services.use(DataConfiguration, configuration)

    def create_context(self):
        return DataContext(self)

    @property
    def configuration(self) -> DataConfiguration:
        return self.services.get(DataConfiguration)

