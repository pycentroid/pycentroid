from .data_context import DataContext
from .data_application_base import DataApplicationBase


class DataApplication(DataApplicationBase):
    def __init__(self):
        super().__init__()
        return

    def create_context(self):
        return DataContext(self)
