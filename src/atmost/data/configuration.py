from os.path import abspath, join
from atmost.common import ConfigurationBase, ApplicationBase, expect
import importlib


class DataConfiguration(ConfigurationBase):
    def __init__(self, application: ApplicationBase):
        super().__init__(abspath(join(application.cwd, 'config')))
        # load adapter types
        adapter_types = self.get('adapterTypes')
        self.__adapter_types__ = {}
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
                    # get invariant name
                    invariant_name = adapter_type['invariantName']
                    # and class
                    adapter_class = getattr(module, class_name)
                    self.__adapter_types__.update({
                        invariant_name: adapter_class
                    })
        adapters = self.get('adapters')
        if adapters is None:
            self.set('adapters', [])

    @property
    def adapter_types(self):
        return self.__adapter_types__




