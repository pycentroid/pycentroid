
class SyncEventHandler:
    def __init__(self, handler):
        self.handler = handler

    def execute(self, *args):
        self.handler(*args)


class OnceSyncEventHandler(SyncEventHandler):
    def __init__(self, handler):
        self.fired = False
        super().__init__(handler)

    def execute(self, *args):
        if self.fired:
            return
        super().execute(*args)
        self.fired = True


class EventSubscription:
    def __init__(self, emitter, handler):
        self.__emitter__ = emitter
        self.__handler__ = handler

    def unsubscribe(self):
        self.__emitter__.unsubscribe(self.__handler__.handler)


class SyncSeriesEventEmitter:

    __handlers__ = []

    def __init__(self):
        self.__handlers__ = []

    def subscribe(self, handler):
        """Appends an event handler and waits for event
        Parameters:
                    handler (Callable): An event handler to include
        Returns:
                subscription (EventSubscription): An object which represents an event subscription for later use
        """
        handle = SyncEventHandler(handler)
        self.__handlers__.append(handle)
        return EventSubscription(self, handle)

    def subscribe_once(self, handler):
        """Appends an event handler and waits for event
        Parameters:
                    handler (Callable): An event handler to include
        Returns:
                subscription (EventSubscription): An object which represents an event subscription for later use
        """
        handle = OnceSyncEventHandler(handler)
        self.__handlers__.append(handle)
        return EventSubscription(self, handle)

    def unsubscribe(self, handler):
        """Removes a previously added event handler

        Args:
            handler (SyncEventHandler): An event handler to remove
        """
        __handler__ = next(filter(lambda x: x.handler == handler, self.__handlers__), None)
        if __handler__ is not None:
            self.__handlers__.remove(__handler__)

    def emit(self, *args):
        for handler in self.__handlers__:
            handler.execute(*args)


class AsyncEventHandler:
    def __init__(self, handler):
        self.handler = handler

    async def execute(self, *args):
        await self.handler(*args)


class OnceAsyncEventHandler(AsyncEventHandler):
    def __init__(self, handler):
        self.fired = False
        super().__init__(handler)

    async def execute(self, *args):
        if self.fired:
            return
        await super().execute(*args)
        self.fired = True


class AsyncSeriesEventEmitter:

    __handlers__ = []

    def __init__(self):
        self.__handlers__ = []

    def subscribe(self, handler):
        """Appends an event handler and waits for event
        Parameters:
                    handler (Callable): An event handler to include
        Returns:
                subscription (SyncSubscription): An object which represents an event subscription for later use
        """
        handle = AsyncEventHandler(handler)
        self.__handlers__.append(handle)
        return EventSubscription(self, handle)

    def subscribe_once(self, handler):
        """Appends an asynchronous event handler and waits for event
        Parameters:
                    handler (Callable): An asynchronous event handler to include
        Returns:
                subscription (SyncSubscription): An object which represents an event subscription for later use
        """
        handle = OnceAsyncEventHandler(handler)
        self.__handlers__.append(handle)
        return EventSubscription(self, handle)

    def unsubscribe(self, handler):
        """Removes a previously added asynchronous event handler

        Args:
            handler (SyncEventHandler): An asynchronous event handler to remove
        """
        __handler__ = next(filter(lambda x: x.handler == handler, self.__handlers__), None)
        if __handler__ is not None:
            self.__handlers__.remove(__handler__)

    async def emit(self, *args):
        for handler in self.__handlers__:
            await handler.execute(*args)
