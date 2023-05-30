from .types import DataContextBase, DataModelBase
from datetime import date, datetime
import uuid
import random
import string


class FunctionContext:

    context: DataContextBase = None
    model: DataModelBase = None
    target: object = None

    def __init__(self, context: DataContextBase, model=None, target=None):
        self.context = context
        self.model = model
        self.target = target

    async def __today__(self):
        return date.today()

    async def __now__(self):
        return date.today()

    async def __me__(self):
        return datetime.today()

    async def __newGuid__(self):
        return uuid.uuid4()

    async def __int__(self, min: int, max: int):
        return random.randint(min, max)

    async def __numbers__(self, length: int = 10):
        return ''.join(random.choice(string.digits) for i in range(length))
