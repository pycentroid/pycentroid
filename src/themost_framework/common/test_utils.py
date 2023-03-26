from .data import DataAdapter
from typing import Callable

class CancelTransactionError(Exception):
    def __init__(self):
        super().__init__('cancel')

class TestUtils:
    def __init__(self, db: DataAdapter):
        self.__adapter__ = db

    def execute_in_transaction(self, func: Callable):
        try:
            def execute():
                func()
                raise CancelTransactionError()
            # execute in transaction
            self.__adapter__.execute_in_transaction(execute)
        except Exception as error:
            if type(error) is not CancelTransactionError:
                raise error