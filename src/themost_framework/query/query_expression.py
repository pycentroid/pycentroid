from .query_field import QueryField, get_first_key, format_any_field_reference, get_field_expression
from .query_entity import QueryEntity
from .lamda_parser import LamdaParser
from themost_framework.common import expect, NoneError
from typing import Callable
import inspect


class Empty:
    pass

class QueryExpression:
    def __init__(self, collection = None):
        self.__where__ = None
        self.__select__ = None
        self.__insert__ = None
        self.__update__ = None
        self.__left__:QueryField = None
        self.__last_logical = None
        if not collection is None:
            self.from_collection(collection)
        return

    def from_(self, collection):
        if type(collection) is QueryEntity:
            self.__collection__ = collection
        else:
            self.__collection__ = QueryEntity(collection)
        return self

    def from_collection(self, collection):
        return self.from_(collection)

    def select(self, *args):
        if inspect.isfunction(args[0]):
            self.__select__ = LamdaParser().parse_select(*args)
            return self
        self.__select__ = {}
        for arg in args:
            if type(arg) is str:
                self.__select__.__setitem__(arg, 1)
            elif type(arg) is QueryField or type(arg) is dict:
                for key in arg:
                    if arg[key] == 1:
                        self.__select__.__setitem__(key, 1)
                    elif arg[key] == 0:
                        break
                    else:
                        self.__select__.__setitem__(key, arg[key])
                    break
            else:
                raise 'Expected string, a dictionary object or an instance of QueryField class'
        return self

    def where(self, *args):
        if type(args[0]) is str:
            self.__where__ = None
            # todo: validate object name
            self.__left__ = QueryField(*args)
        if inspect.isfunction(args[0]):
            expr = args[0]
            params = None
            # parse callable as where statement
            self.__where__ = LamdaParser().parse_filter(*args)
        
        return self

    def equal(self, value):
        expect(self.__left__).to_be_truthy(NoneError)
        self.__append({
            '$eq': [
                get_field_expression(self.__left__),
                value
            ]
        })
        return self

    def equals(self, value):
        return self.equal(value)

    def not_equal(self, value):
        expect(self.__left__).to_be_truthy(NoneError)
        self.__append({
            '$ne': [
                get_field_expression(self.__left__),
                value
            ]
        })
        return self

    def not_equals(self, value):
        return self.not_equal(value)

    def greater_than(self, value):
        expect(self.__left__).to_be_truthy(NoneError)
        self.__append({
            '$gt': [
                get_field_expression(self.__left__),
                value
            ]
        })
        return self

    def greater_or_equal(self, value):
        expect(self.__left__).to_be_truthy(NoneError)
        self.__append({
            '$gte': [
                get_field_expression(self.__left__),
                value
            ]
        })
        return self

    def lower_than(self, value):
        expect(self.__left__).to_be_truthy(NoneError)
        self.__append({
            '$lt': [
                get_field_expression(self.__left__),
                value
            ]
        })
        return self

    def lower_or_equal(self, value):
        expect(self.__left__).to_be_truthy(NoneError)
        self.__append({
            '$lte': [
                get_field_expression(self.__left__),
                value
            ]
        })
        return self

    def and_also(self, name: str):
        self.__left__ = QueryField(name)
        self.__last_logical = '$and'
        return self

    def or_else(self, name: str):
        self.__left__ = QueryField(name)
        self.__last_logical = '$or'
        return self
    
    def get_year(self, timezone = None):
        self.__left__.get_year(timezone)
        return self

    def get_date(self, timezone = None):
        self.__left__.get_date(timezone)
        return self
    
    def get_month(self, timezone = None):
        self.__left__.get_month(timezone)
        return self

    def get_hours(self, timezone = None):
        self.__left__.get_hours(timezone)
        return self
    
    def hour(self, timezone = None):
        self.__left__.get_hours(timezone)
        return self
    
    def get_minutes(self, timezone = None):
        self.__left__.get_year(timezone)
        return self
    
    def minute(self, timezone = None):
        self.__left__.get_minutes(timezone)
        return self

    def get_seconds(self, timezone = None):
        self.__left__.get_seconds(timezone)
        return self
    
    def second(self, timezone = None):
        self.__left__.get_seconds(timezone)
        return self
    
    def index_of(self, search: str):
        assert self.__left__ is None, 'Left operand cannot be empty'
        self.__left__.index_of(search)
        return self
    
    def index_of(self, search: str):
        expect(self.__left__).to_be_truthy(NoneError)
        self.__left__.index_of(search)
        return self

    def index(self, search: str):
        return self.index_of(value);
    
    def add(self, value):
        expect(self.__left__).to_be_truthy(NoneError)
        self.__left__.add(value)
        return self
    
    def subtract(self, value):
        expect(self.__left__).to_be_truthy(NoneError)
        self.__left__.subtract(value)
        return self
    
    def divide(self, value):
        expect(self.__left__).to_be_truthy(NoneError)
        self.__left__.divide(value)
    
    def multiply(self, value):
        expect(self.__left__).to_be_truthy(NoneError)
        self.__left__.multiply(value)
        return self
    
    def round(self, digits):
        expect(self.__left__).to_be_truthy(NoneError)
        self.__left__.round(digits)
        return self
    
    def ceil(self):
        expect(self.__left__).to_be_truthy(NoneError)
        self.__left__.ceil()
    
    def floor(self):
        expect(self.__left__).to_be_truthy(NoneError)
        self.__left__.floor()
    
    def modulo(self):
        expect(self.__left__).to_be_truthy(NoneError)
        self.__left__.modulo()
    
    def length(self):
        expect(self.__left__).to_be_truthy(NoneError)
        self.__left__.length()
        return self
    
    def len(self):
        return self.length()
    
    def trim(self):
        expect(self.__left__).to_be_truthy(NoneError)
        self.__left__.trim()
        return self
    
    def substring(self, start, length):
        expect(self.__left__).to_be_truthy(NoneError)
        self.__left__.substring(start, length)
    
    def concat(self, *args):
        expect(self.__left__).to_be_truthy(NoneError)
        self.__left__.concat(*args)

    def to_lower(self):
        expect(self.__left__).to_be_truthy(NoneError)
        self.__left__.to_lower()
        return self
    
    def to_upper(self):
        expect(self.__left__).to_be_truthy(NoneError)
        self.__left__.to_upper()
        return self
    
    def get_min(self):
        expect(self.__left__).to_be_truthy(NoneError)
        self.__left__.get_min()
        return self

    def get_max(self):
        expect(self.__left__).to_be_truthy(NoneError)
        self.__left__.get_max()
        return self
    
    def get_count(self):
        expect(self.__left__).to_be_truthy(NoneError)
        self.__left__.get_average()
        return self

    def get_average(self):
        expect(self.__left__).to_be_truthy(NoneError)
        self.__left__.get_average()
        return self
    
    def startswith(self, search):
        expect(self.__left__).to_be_truthy(NoneError)
        left = self.__left__.index_of(search)
        self.__left__ = QueryField('t0')
        self.__left__['t0'] = {
            '$eq': [
                get_field_expression(left),
                0
            ]
        }
        return self
    
    def update(self, collection):
        self.__select__ = None
        if type(collection) is QueryEntity:
            self.__collection__ = collection
        else:
            self.__collection__ = QueryEntity(collection)
        return self

    def set(self, object):
        self.__update__ = Empty()
        if type(object) is dict:
            for key in object:
                setattr(self.__update__, key, object[key])
        else:
            for key, value in object.__dict__.items():
                setattr(self.__update__, key, object[key])
        return self

    def __append(self, expr):
        if self.__where__ is None:
            self.__where__ = expr
        else:
            # get first property of current query
            property = list(self.__where__.keys())[0];
            # get logical operator
            logical_operator = self.__last_logical
            # if last logical operator is equal with this property
            if (property == logical_operator):
                # append query expression
                expr[property].append(expr)
            else:
                self.__where__ = {
                    logical_operator: [
                        self.__where__.copy(),
                        expr
                    ]
                }
        self.__left__ = None


