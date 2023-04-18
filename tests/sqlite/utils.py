from typing import Callable
from centroid.sqlite import SqliteAdapter

class ExecuteAndCancel:
    def __init__(self, func: Callable):
        self.func = func
    
    def void():
        pass

    def execute():
        self.func()
        raise Exception('cancel')

def execute_in_transaction(adapter: SqliteAdapter, func: Callable):
    try:
        adapter.execute_in_transaction(ExecuteAndCancel(func).execute)
    except ex:
        if ex.message != 'cancel':
            raise ex
        