from os.path import abspath, join
from pycentroid.common import ConfigurationBase, ConfigurationStrategy, ApplicationBase, expect, AnyDict
import importlib
from .loaders import SchemaLoaderStrategy, DefaultSchemaLoaderStrategy
from .data_types import DataTypes


class DataAdapters(ConfigurationStrategy):

    __adapters__ = []

    def __init__(self, configuration):
        super().__init__(configuration)
        self.configure()

    def configure(self):
        adapter_types = self.configuration.get('adapterTypes')
        self.__adapters__ = []
        if type(adapter_types) is list:
            for adapter_type in adapter_types:
                if 'type' in adapter_type:
                    # get adapter type by name
                    name = adapter_type['type'].split('#')
                    # expect adapter type name to be a hashed like string
                    # e.g. mylibrary.subpackage#AdapterClass
                    expect(len(name)).to_equal(2, Exception('Adapter type declaration is invalid. Expected a string '
                                                            'containing both module path and class name.'))
                    # import module
                    module = importlib.import_module(name[0])
                    # get adapter class name
                    class_name = name[1]
                    expect(hasattr(module, class_name)).to_be_truthy(
                        Exception('The specified adapter type cannot be found in target module.')
                    )
                    # and class
                    adapter_class = getattr(module, class_name)
                    adapter_type.update({
                        'adapterClass': adapter_class
                    })

        adapters = self.configuration.get('adapters')
        if type(adapters) is list:
            for adapter in adapters:
                # find adapter type
                adapter_type = next(
                    filter(lambda x: x['invariantName'] == adapter['invariantName'], adapter_types), None
                )
                adapter.update({
                    'adapterType': adapter_type.copy()
                })
                self.__adapters__.append(AnyDict(**adapter))

    def get(self, name=None):
        if name is None:
            return next(filter(lambda x: x.default is True, self.__adapters__), None)
        return next(filter(lambda x: x.name == name, self.__adapters__), None)


class DataConfiguration(ConfigurationBase):
    def __init__(self, application: ApplicationBase):
        super().__init__(abspath(join(application.cwd, 'config')))
        # use DataAdapters
        self.usestrategy(DataAdapters)
        # use DataTypes
        self.usestrategy(DataTypes)
        # use DefaultSchemaLoaderStrategy
        self.usestrategy(SchemaLoaderStrategy, DefaultSchemaLoaderStrategy)

