from .base import DataApplicationBase, ApplicationServiceBase


class ApplicationService(ApplicationServiceBase):
    def __init__(self, application: DataApplicationBase):
        super().__init__()
        # set application
        self.application = application

