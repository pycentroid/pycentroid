from atmost.common import ApplicationBase


class DataContext:
    def __init__(self, application: ApplicationBase):
        self.application = application
        return
