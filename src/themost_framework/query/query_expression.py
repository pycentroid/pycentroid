from .query_field import QueryField, get_first_key, format_any_field_reference
from .query_entity import QueryEntity
from themost_framework.common import expect, NoneError

class QueryExpression:
    def __init__(self, collection = None):
        self.__query__ = None
        self.__left__ = None
        self.__last_logical = None
        self.from_(collection)
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
        self.__select__ = []
        for arg in args:
            if type(arg) is str:
                self.__select__.append(QueryField(arg))
            elif type(arg) is QueryField:
                self.__select__.append(arg)
            elif type(arg) is dict:
                field = QueryField()
                for key in arg:
                    field[key] = arg[key]
                    break
                self.__select__.append(field)
            else:
                raise 'Expected string, a dictionary object or an instance of QueryField class'

    def where(self, name: str):
        self.__query__ = None
        # todo: validate object name
        self.__left__ = QueryField(name)
        return self

    def equal(self, value):
        expect(self.__left__).to_be_truthy(NoneError)
        self.__append({
            '$eq': [
                self.__left__,
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
                self.__left__,
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
                self.__left__,
                value
            ]
        })
        return self

    def greater_or_equal(self, value):
        expect(self.__left__).to_be_truthy(NoneError)
        self.__append({
            '$gte': [
                self.__left__,
                value
            ]
        })
        return self

    def lower_than(self, value):
        expect(self.__left__).to_be_truthy(NoneError)
        self.__append({
            '$lt': [
                self.__left__,
                value
            ]
        })
        return self

    def lower_or_equal(self, value):
        expect(self.__left__).to_be_truthy(NoneError)
        self.__append({
            '$lte': [
                self.__left__,
                value
            ]
        })
        return self

    def and_(self, name: str):
        self.__left__ = QueryField(name)
        self.__last_logical = '$and'
        return self

    def or_(self, name: str):
        self.__left__ = QueryField(name)
        self.__last_logical = '$or'
        return self
    
    def get_year(self, timezone = None):
        self.__left__:QueryField.get_year(timezone)
        return self

    def get_date(self, timezone = None):
        self.__left__:QueryField.get_date(timezone)
        return self
    
    def get_month(self, timezone = None):
        self.__left__:QueryField.get_month(timezone)
        return self

    def get_hours(self, timezone = None):
        self.__left__:QueryField.get_hours(timezone)
        return self
    
    def hour(self, timezone = None):
        self.__left__:QueryField.get_hours(timezone)
        return self
    
    def get_minutes(self, timezone = None):
        self.__left__:QueryField.get_year(timezone)
        return self
    
    def minute(self, timezone = None):
        self.__left__:QueryField.get_minutes(timezone)
        return self

    def get_seconds(self, timezone = None):
        self.__left__:QueryField.get_seconds(timezone)
        return self
    
    def second(self, timezone = None):
        self.__left__:QueryField.get_seconds(timezone)
        return self
    
    def index_of(self, search: str):
        assert self.__left__ is None, 'Left operand cannot be empty'
        self.__left__:QueryField.index_of(search)
        return self

    def index(self, search: str):
        return self.index_of(value);
    
    def length(self):
        self.__left__ = {
            '$length': [
                self.__left__
            ]
        }
        return self

    def to_lower(self):
        self.__left__ = {
            '$toLower': [
                self.__left__
            ]
        }
        return self
    
    def to_upper(self):
        self.__left__ = {
            '$toUpper': [
                self.__left__
            ]
        }
        return self

    def __append(self, expr):
        if self.__query__ is None:
            self.__query__ = expr
        else:
            # get first property of current query
            property = list(self.__query__.keys())[0];
            # get logical operator
            logical_operator = self.__last_logical
            # if last logical operator is equal with this property
            if (property == logical_operator):
                # append query expression
                expr[property].append(expr)
            else:
                self.__query__ = {
                    logical_operator: [
                        self.__query__.copy(),
                        expr
                    ]
                }


