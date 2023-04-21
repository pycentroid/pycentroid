

class NotImplementError(Exception):
    def __init__(self, message='Not yet implemented'):
        self.message = message
        super().__init__(self.message)


class DataError(Exception):

    model: str
    field: str
    inner_message: str

    def __init__(self, message, inner_message=None, model=None, field=None):
        self.message = message
        self.inner_message = inner_message
        self.model = model
        self.field = field
        super().__init__(self.message)
