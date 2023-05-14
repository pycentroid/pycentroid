

class NotImplementError(Exception):
    def __init__(self, message='Not yet implemented'):
        self.message = message
        super().__init__(self.message)


class DataError(Exception):

    code: str
    model: str
    field: str
    inner_message: str

    def __init__(self, message, inner_message=None, model=None, field=None, code='ERR_DATA'):
        self.message = message
        self.inner_message = inner_message
        self.model = model
        self.field = field
        self.code = code
        super().__init__(self.message)
