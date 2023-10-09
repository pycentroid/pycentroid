from abc import abstractmethod
from enum import Enum
from typing import List, Callable
from types import SimpleNamespace
from pycentroid.common import ApplicationBase, AsyncSeriesEventEmitter, AnyDict
from pycentroid.query import DataAdapter, QueryExpression


class DataObjectState(Enum):
    INSERT = 1
    UPDATE = 2
    DELETE = 4


class PrivilegeMask(Enum):
    READ = 1
    CREATE = 2
    UPDATE = 4
    DELETE = 8
    EXECUTE = 16
    ALL = 31


class DataAssociationType(str, Enum):
    ASSOCIATION = 'association'
    JUNCTION = 'junction'


class DataObjectPrivilege(AnyDict):
    mask: PrivilegeMask
    """A number which represents permission mask (1=Read, 2=Create, 4=Update, 8=Delete, 16=Execute)"""
    type: str
    """A string which represents the permission scope."""
    account: str
    """A string which represents the name of the security group
    where this privilege will be applied e.g. Administrators, Sales etc."""
    filter: str
    """A string which represents a filter expression for this privilege.
    This attribute is used for self privileges which are commonly derived from user's attributes
    e.g. 'owner eq me()' or 'orderStatus eq 1 and customer eq me()' etc.
    """


class DataFieldValidation:
    minValue: object
    maxValue: object
    minLength: int
    maxLength: int
    pattern: str
    patternMessage: str
    message: str
    type: str
    validator: str


class DataFieldAssociationMapping(AnyDict):
    associationType: str
    associationAdapter: str
    associationObjectField: str
    associationValueField: str
    parentModel: str
    parentField: str
    childModel: str
    childField: str
    cascade: str
    privileges: List[DataObjectPrivilege]


class DataModelConstraint(AnyDict):
    type: str
    """A string which represents the type of this constraint e.g. unique"""
    description: str
    """A short description for this constraint
    e.g. Unique identifier field must be unique across different records."""
    fields: List[str]


class DataField(AnyDict):
    name: str
    """A string which represents the name of this attribute e.g. title, description, dateCreated etc"""
    description: str
    """ A string which represents a short description of this attribute"""
    title: str
    """A string which represents the name of this attribute e.g. title, description, dateCreated etc"""
    type: str
    """A string which represents the type of this attribute e.g. Counter, Integer, Number, Text etc"""
    size: int
    """A number which represents the maximum size for this attribute e.g. the size of a text field etc"""
    scale: int
    """A number which represents the number of digits of a decimal number"""
    nullable: bool
    """A boolean which indicates whether this attribute is nullable or not."""
    editable: bool
    """A boolean which indicates whether this attribute is editable or not."""
    readonly: bool
    """A boolean which indicates whether this attribute is readonly or not.
    A readonly value must have a default value or a calculated value."""
    primary: bool
    """A boolean which indicates whether this attribute is a key column or not."""
    indexed: bool
    """A boolean which indicates whether this attribute is an indexed column or not."""
    property: str
    """A string which optionally represents the name of this attribute in object mapping.
    This name may defer from the name of the database field."""
    many: bool
    """A boolean value which indicates whether this attribute
    represents a one-to-many or many-to-many association between two models."""
    multiplicity: str
    """A string which defines the multiplicity level of an association between two objects"""
    expandable: bool
    """A boolean value which indicates whether the associated object(s) will be automatically expanded or not."""
    nested: bool
    """A boolean which indicates whether this attribute defines an association between two models
    where child objects are always treated as a part of the parent object."""
    mapping: DataFieldAssociationMapping
    validation: DataFieldValidation


class DataModelEventListener(AnyDict):
    name: str
    """A string which the name of this event listener e.g. 'After Update Person'"""
    type: str
    """A string which represents the path of the module that exports this listener."""


class DataModelProperties(AnyDict):

    name: str
    """A string which represents the name of this model e.g. Order, Customer, Person etc"""
    title: str
    """ A string which represents the title of this e.g. Supplier Orders, Person Followers etc"""
    inherits: str
    """A string which represents the model which is inherited by this model
    e.g. User inherits Account, Person inherits Party etc"""
    implements: str
    """A string which represents the model which is implemented by this model
    e.g. ActionStatusType model implements Enumeration model etc"""
    sealed: bool
    """A boolean which indicates whether this model is being upgraded automatically or not."""
    hidden: bool
    """A boolean which indicates whether this model is hidden or not. The default value is false."""
    classPath: str
    """A string which represents a module path that exports a class which maps this database model"""
    version: str
    """The version of this item"""
    source: str
    """A string which holds the database object of this model."""
    view: str
    """A string which holds the database object that is going to be used while fetching data."""
    fields: List[DataField]
    constraints: List[DataModelConstraint]
    privileges: List[DataObjectPrivilege]
    eventListeners: List[DataModelEventListener]
    seed: List[object]

    def get_source(self):
        return self.source if self.source is not None else f'{self.name}Base'

    def get_view(self):
        return self.view if self.view is not None else f'{self.name}Data'


class ContextUser:

    name: str
    token: str
    scope: str
    authentication_type: str
    key: object


class DataContextBase:

    application: ApplicationBase
    user: ContextUser

    @property
    @abstractmethod
    def db(self) -> DataAdapter:
        pass

    @abstractmethod
    def model(self, m):
        pass

    @abstractmethod
    def finalize(self):
        pass

    @abstractmethod
    def execute_in_transaction(self, func: Callable):
        pass


class DataModelEventEmitter:

    upgrade: AsyncSeriesEventEmitter
    save: AsyncSeriesEventEmitter
    remove: AsyncSeriesEventEmitter
    execute: AsyncSeriesEventEmitter

    def __init__(self):
        self.upgrade = AsyncSeriesEventEmitter()
        self.save = AsyncSeriesEventEmitter()
        self.remove = AsyncSeriesEventEmitter()
        self.execute = AsyncSeriesEventEmitter()


class DataModelBase:

    properties: DataModelProperties
    context: DataContextBase
    before: DataModelEventEmitter
    after: DataModelEventEmitter

    def __init__(self, context: DataContextBase = None, properties: DataModelProperties = None, **kwargs):
        self.context = context
        self.properties = properties
        self.before = DataModelEventEmitter()
        self.after = DataModelEventEmitter()

    @abstractmethod
    def base(self):
        pass

    @property
    @abstractmethod
    def attributes(self) -> List[DataField]:
        pass

    @abstractmethod
    def silent(self, value=True):
        pass

    @abstractmethod
    def as_queryable(self):
        pass

    @abstractmethod
    async def insert(self, o: object or List[object]):
        pass

    @abstractmethod
    async def migrate(self):
        pass

    def key(self):
        return next(filter(lambda x: x.primary is True, self.attributes), None)

    def get_attribute(self, name: str):
        return next(filter(lambda x: x.name == name, self.attributes), None)

    def getattr(self, name: str):
        return next(filter(lambda x: x.name == name, self.attributes), None)

    @abstractmethod
    def get_super_types(self) -> List[str]:
        pass

    @abstractmethod
    def infermapping(self, o: object):
        pass

    @abstractmethod
    async def inferstate(self, item: object) -> bool:
        pass

    @abstractmethod
    async def upsert(self, o: object or List[object]):
        pass

    @abstractmethod
    async def save(self, o: object or List[object]):
        pass

    @abstractmethod
    async def update(self, o: object or List[object]):
        pass

    @abstractmethod
    async def remove(self, o: object or List[object]):
        pass


class UpgradeEventArgs(SimpleNamespace):

    model: DataModelBase
    done: bool = False


class ExecuteEventArgs(SimpleNamespace):

    model: DataModelBase
    emitter: QueryExpression
    results: List[object] = None


class DataEventArgs(SimpleNamespace):

    model: DataModelBase
    state: DataObjectState
    previous: object = None
    target: object = None
