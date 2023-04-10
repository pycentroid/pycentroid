
class NotImplementError(Exception):
    def __init__(self, message='Not yet implemented'):
        self.message = message
        super().__init__(self.message)
