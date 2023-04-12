import inspect
from .expect import expect


class ExpectedStrategyTypeError(Exception):
    def __init__(self, message='Configuration strategy must be a type'):
        self.message = message
        super().__init__(self.message)


class ExpectedConfigurationStrategyError(Exception):
    def __init__(self, message='The specified object must be an instance of configuration strategy'):
        self.message = message
        super().__init__(self.message)


class ConfigurationStrategy:
    configuration = None

    def __init__(self, configuration):
        self.configuration = configuration
        pass


class ConfigurationBase:
    __strategy__ = {}

    def __init__(self):
        pass

    def get(self, strategy):
        expect(inspect.isclass(strategy)).to_be_truthy(ExpectedStrategyTypeError())
        return self.__strategy__.get(type(strategy))

    def use(self, strategy, useclass=None):
        expect(inspect.isclass(useclass)).to_be_truthy(ExpectedStrategyTypeError())
        if useclass is None:
            self.__strategy__.update({
                type(strategy): strategy(self)
            })
        elif inspect.isclass(useclass):
            instance = useclass(self)
            self.__strategy__.update({
                type(strategy): instance
            })
        elif type(useclass) is ConfigurationStrategy:
            self.__strategy__.update({
                type(strategy): useclass
            })
        else:
            raise ExpectedConfigurationStrategyError()

    def has(self, strategy):
        return type(strategy) in self.__strategy__
