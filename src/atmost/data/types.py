from enum import Enum
from types import SimpleNamespace
from typing import List


class PrivilegeMask(Enum):
    READ = 1
    CREATE = 2
    UPDATE = 4
    DELETE = 8
    EXECUTE = 16
    ALL = 31


class DataObjectPrivilege:
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
    minValue = None
    maxValue = None
    minLength: int
    maxLength: int
    pattern: str
    patternMessage: str
    message: str
    type: str
    validator: str


class DataFieldAssociationMapping:
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


class DataModelConstraint:
    type: str
    """A string which represents the type of this constraint e.g. unique"""
    description: str
    """A short description for this constraint
    e.g. Unique identifier field must be unique across different records."""
    fields: List[str]


class DataField:
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
    nullable = True
    """A boolean which indicates whether this attribute is nullable or not."""
    editable = True
    """A boolean which indicates whether this attribute is editable or not."""
    readonly = False
    """A boolean which indicates whether this attribute is readonly or not. 
    A readonly value must have a default value or a calculated value."""
    primary = False
    """A boolean which indicates whether this attribute is a key column or not."""
    indexed = False
    """A boolean which indicates whether this attribute is an indexed column or not."""
    property: str
    """A string which optionally represents the name of this attribute in object mapping.
    This name may defer from the name of the database field."""
    many: bool
    """A boolean value which indicates whether this attribute
    represents a one-to-many or many-to-many association between two models."""
    multiplicity: str
    """A string which defines the multiplicity level of an association between two objects"""
    expandable = False
    """A boolean value which indicates whether the associated object(s) will be automatically expanded or not."""
    nested: bool
    """A boolean which indicates whether this attribute defines an association between two models
    where child objects are always treated as a part of the parent object."""
    mapping: DataFieldAssociationMapping
    validation: DataFieldValidation


class DataModelEventListener:
    name: str
    """A string which the name of this event listener e.g. 'After Update Person'"""
    type: str
    """A string which represents the path of the module that exports this listener."""


class DataModelProperties(SimpleNamespace):
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
    sealed = False
    """A boolean which indicates whether this model is being upgraded automatically or not."""
    hidden = False
    """A boolean which indicates whether this model is hidden or not. The default value is false."""
    classPath: str
    """A string which represents a module path that exports a class which maps this database model"""
    source: str
    """A string which holds the database object of this model."""
    view: str
    """A string which holds the database object that is going to be used for fetching data."""
    version: str
    """The version of this item"""
    fields: List[DataField]
    constraints: List[DataModelConstraint]
    privileges: List[DataObjectPrivilege]
    eventListeners: List[DataModelEventListener]
