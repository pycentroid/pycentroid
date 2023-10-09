import inspect
from pycentroid.common import expect, AnyObject, NoneError, SyncSeriesEventEmitter
from .closure_parser import ClosureParser
from .query_entity import QueryEntity
from .query_field import QueryField, get_field_expression, format_field_reference
from types import SimpleNamespace
from enum import Enum
import json
from typing import overload
from typing_extensions import Self


class QueryExpressionType(str, Enum):

    SELECT = 'SELECT',
    INSERT = 'INSERT',
    UPDATE = 'UPDATE',
    DELETE = 'DELETE'


class ResolvingJoinMemberEvent(SimpleNamespace):

    target: object
    member: str
    fully_qualified_name: str


class ResolvingMemberEvent(SimpleNamespace):

    target: object
    member: str


class ResolvingMethodEvent(SimpleNamespace):

    target: object
    member: str


class JOIN_DIRECTION(str, Enum):

    LEFT = 'left'
    RIGHT = 'right'
    INNER = 'inner'


class QueryExpression:

    resolving_member: SyncSeriesEventEmitter
    resolving_join_member: SyncSeriesEventEmitter
    resolving_method: SyncSeriesEventEmitter

    def __init__(self, collection=None, alias=None):
        self.__where__ = None
        self.__prepared__ = None
        self.__order_by__ = None
        self.__group_by__ = None
        self.__select__ = None
        self.__insert__ = None
        self.__update__ = None
        self.___delete___ = None
        self.__skip__ = 0
        self.__limit__ = 0
        self.__lookup__ = []
        self.__joining__ = None
        self.__left__: QueryField or None = None
        self.__last_logical = None
        self.__distinct__ = None
        self.__alias__ = alias
        self.__fixed__ = False
        self.resolving_member = SyncSeriesEventEmitter()
        self.resolving_join_member = SyncSeriesEventEmitter()
        self.resolving_method = SyncSeriesEventEmitter()

        if collection is not None:
            self.__set_collection__(collection)

    def __set_collection__(self, collection):
        if type(collection) is QueryEntity:
            self.__collection__ = collection
        else:
            self.__collection__ = QueryEntity(collection)
        return self

    def from_collection(self, collection):
        return self.__set_collection__(collection)

    def as_(self, alias: str):
        self.__alias__ = alias
        return self

    @property
    def alias(self) -> str:
        """Returns the alias of this expression when is going to be used a sub-query

        Returns:
            str: A string which represents the alias of this query expression
        """
        return self.__alias__

    def get_closure_parser(self) -> ClosureParser:
        parser = ClosureParser()

        def resolving_join_member(event):
            new_event = AnyObject(target=self, member=event.member, fully_qualified_name=event.fully_qualified_name)
            self.resolving_join_member.emit(new_event)
        parser.resolving_join_member.subscribe(resolving_join_member)

        def resolving_member(event):
            new_event = AnyObject(target=self, member=event.member)
            self.resolving_member.emit(new_event)
        parser.resolving_member.subscribe(resolving_member)

        def resolving_method(event):
            new_event = AnyObject(target=self, method=event.method)
            self.resolving_method.emit(new_event)
        parser.resolving_method.subscribe(resolving_method)

        return parser

    @overload
    def select(self, expr: callable, **kwargs) -> Self:
        kwargs['expr'] = expr
        return self.select(**kwargs)
    
    @overload
    def select(self, *args: str | QueryField) -> Self:
        return self.select(*args)

    def select(self, *args, **kwargs) -> Self:
        self.__update__ = None
        self.__insert__ = None
        self.___delete___ = None
        if inspect.isfunction(args[0]):
            self.__select__ = self.get_closure_parser().parse_select(*args, kwargs)
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

    @overload
    def where(self, where: callable, **kwargs) -> Self:
        kwargs['where'] = where
        return self.where(**kwargs)
    
    @overload
    def where(self, attribute: str | QueryField) -> Self:
        return self.where(attribute)

    def where(self, *args, **kwargs) -> Self:
        if len(args) == 0:
            # try to find kwargs
            where_param = kwargs.pop('where')
            expect(inspect.isfunction(where_param)).to_be_truthy(Exception('Where statement must be a callable'))
            self.__where__ = self.get_closure_parser().parse_filter(where_param, kwargs)
            return self
        if type(args[0]) is str:
            self.__where__ = None
            # todo: validate object name
            self.__left__ = QueryField(*args)
        elif inspect.isfunction(args[0]):
            # parse callable as where statement
            self.__where__ = self.get_closure_parser().parse_filter(*args, kwargs)
        elif isinstance(args[0], QueryField):
            self.__left__ = args[0]
        else:
            raise Exception('Invalid argument. Expected a string, a lambda function or an instance of query field.')

        return self

    def prepare(self, use_or=False):
        """Stores the underlying filter expression for further processing

        Args:
            use_or (bool, optional): Use logical "or" expression while concatenating an already stored expression

        Returns:
            _type_: Query
        """
        if self.__where__ is not None:
            if self.__prepared__ is not None:
                prepared = self.__prepared__
                if use_or is False:
                    self.__prepared__ = {
                        '$and': [
                            prepared,
                            self.__where__
                        ]
                    }
                else:
                    self.__prepared__ = {
                        '$or': [
                            prepared,
                            self.__where__
                        ]
                    }
            else:
                self.__prepared__ = self.__where__
            self.__where__ = None
        return self

    def equal(self, value):
        expect(self.__left__).to_be_truthy(NoneError)
        self.__append({
            '$eq': [
                get_field_expression(self.__left__),
                get_field_expression(value) if type(value) is QueryField else value
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
                get_field_expression(value) if type(value) is QueryField else value
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

    def get_year(self, timezone=None):
        self.__left__.get_year(timezone)
        return self

    def get_date(self, timezone=None):
        self.__left__.get_date(timezone)
        return self

    def get_month(self, timezone=None):
        self.__left__.get_month(timezone)
        return self

    def get_hours(self, timezone=None):
        self.__left__.get_hours(timezone)
        return self

    def hour(self, timezone=None):
        self.__left__.get_hours(timezone)
        return self

    def get_minutes(self, timezone=None):
        self.__left__.get_year(timezone)
        return self

    def minute(self, timezone=None):
        self.__left__.get_minutes(timezone)
        return self

    def get_seconds(self, timezone=None):
        self.__left__.get_seconds(timezone)
        return self

    def second(self, timezone=None):
        self.__left__.get_seconds(timezone)
        return self

    def index_of(self, search: str):
        expect(self.__left__).to_be_truthy(NoneError)
        self.__left__.index_of(search)
        return self

    def index(self, search: str):
        return self.index_of(search)

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
        self.__left__.min()
        return self

    def get_max(self):
        expect(self.__left__).to_be_truthy(NoneError)
        self.__left__.max()
        return self

    def get_count(self):
        expect(self.__left__).to_be_truthy(NoneError)
        self.__left__.count()
        return self

    def get_average(self):
        expect(self.__left__).to_be_truthy(NoneError)
        self.__left__.average()
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

    def skip(self, n):
        """Defines the number of records to skip on an expression which limits results

        Args:
            n (int): The number of records to skip

        Returns:
            QueryExpression
        """
        self.__skip__ = n
        return self

    def take(self, n):
        """Prepares an expression which limits the number of results

        Args:
            n (int): The number of records to return

        Returns:
            QueryExpression
        """
        self.__limit__ = n
        return self

    def update(self, collection):
        self.__select__ = None
        self.__insert__ = None
        self.___delete___ = None
        self.__set_collection__(collection)
        return self

    def set(self, source):
        self.__update__ = lambda: None
        if type(source) is dict:
            for key in source:
                setattr(self.__update__, key, source[key])
        else:
            for key, value in source.__dict__.items():
                setattr(self.__update__, key, value)
        return self

    def insert(self, source):
        self.__insert__ = lambda: None
        if type(source) is dict:
            for key in source:
                setattr(self.__insert__, key, source[key])
        else:
            for key, value in source.__dict__.items():
                setattr(self.__insert__, key, value)
        return self

    def into(self, collection):
        self.__select__ = None
        self.__update__ = None
        self.___delete___ = None
        self.__set_collection__(collection)
        return self

    def join(self, collection, alias: str = None, direction: JOIN_DIRECTION = JOIN_DIRECTION.INNER) -> Self:
        if isinstance(collection, QueryExpression):
            self.__joining__ = {
                '$lookup': {
                    'from': collection,
                    'direction': direction,
                    'as': alias or collection.alias
                }
            }
            return self

        if isinstance(collection, QueryEntity):
            self.__joining__ = {
                '$lookup': {
                    'from': collection.collection,
                    'direction': direction,
                    'as': collection.alias
                }
            }
        else:
            self.__joining__ = {
                '$lookup': {
                    'from': collection,
                    'direction': direction,
                    'as': alias
                }
            }
        return self

    def on(self, *args, **kwargs):
        expect(self.__joining__).to_be_truthy(Exception('Joining expression is empty'))
        lookup = self.__joining__['$lookup']
        if inspect.isfunction(args[0]):
            expr = self.get_closure_parser().parse_filter(*args, kwargs)
        else:
            query = args[0]
            expect(query).to_be_instance_of(QueryExpression, Exception('Expected an instance of query expression'))
            # get where statement
            expr = query.__where__
        # add pipeline
        lookup.__setitem__('pipeline', {
            '$match': {
                '$expr': expr
            }
        })
        # append join expression
        self.__lookup__.append(self.__joining__)
        # cleanup joining expression
        self.__joining__ = None
        return self

    def left_join(self, collection, local_field, foreign_field, alias=None):
        """Prepares a left join expression with the given collection

        Args:
            collection (str): The collection to join
            local_field (str): Specifies the local field in join expression
            foreign_field (str): Specifies the foreign field in join expression
            alias (str, optional): Specifies the alias of the given collection in join expression. Defaults to None.

        Returns:
            QueryExpression
        """
        self.__lookup__.append({
            '$lookup': {
                'from': collection,
                'localField': local_field,
                'foreignField': foreign_field,
                'direction': 'left',
                'as': alias
            }
        })
        return self

    def right_join(self, collection, local_field, foreign_field, alias=None):
        """Prepares a right join expression with the given collection

        Args:
            collection (str): The collection to join
            local_field (str): Specifies the local field in join expression
            foreign_field (str): Specifies the foreign field in join expression
            alias (str, optional): Specifies the alias of the given collection in join expression. Defaults to None.

        Returns:
            QueryExpression
        """
        self.__lookup__.append({
            '$lookup': {
                'from': collection,
                'localField': local_field,
                'foreignField': foreign_field,
                'direction': 'right',
                'as': alias
            }
        })
        return self

    def __append_order__(self, expr, direction) -> Self:
        if self.__order_by__ is None:
            self.__order_by__ = []
        if type(expr) is str:
            self.__order_by__.append({
                '$expr': format_field_reference(expr),
                'direction': direction
            })
        elif isinstance(expr, dict):
            for key in expr:
                value = expr[key]
                if type(value) is int and expr[key] == 1:
                    self.__order_by__.append({
                        '$expr': format_field_reference(key),
                        'direction': direction
                    })
                elif type(value) is int and expr[key] == 0:
                    break
                else:
                    self.__order_by__.append({
                        '$expr': expr,
                        'direction': direction
                    })
        else:
            raise TypeError('Order by expression must be a string or an instance of dictionary object.')
        return self

    @overload
    def order_by(self, expr: callable) -> Self:
        return self.order_by(expr)
    
    @overload
    def order_by(self, expr: str | QueryField) -> Self:
        return self.order_by(expr)

    def order_by(self, expr) -> Self:
        if inspect.isfunction(expr):
            arguments = self.get_closure_parser().parse_select(expr)
            if type(arguments) is dict:
                self.__append_order__(arguments, 1)
                return self
            for arg in arguments:
                self.__append_order__(arg, 1)
            return self
        if type(expr) is str:
            return self.__append_order__(QueryField(expr), 1)
        return self.__append_order__(expr, 1)

    @overload
    def order_by_descending(self, expr: callable) -> Self:
        return self.order_by_descending(expr)
    
    @overload
    def order_by_descending(self, expr: str | QueryField) -> Self:
        return self.order_by_descending(expr)

    def order_by_descending(self, expr) -> Self:
        if inspect.isfunction(expr):
            arguments = self.get_closure_parser().parse_select(expr)
            for arg in arguments:
                self.__append_order__(arg, -1)
            return self
        if type(expr) is str:
            return self.__append_order__(QueryField(expr), -1)
        return self.__append_order__(expr, -1)

    @overload
    def then_by(self, expr: callable) -> Self:
        return self.then_by(expr)
    
    @overload
    def then_by(self, expr: str | QueryField) -> Self:
        return self.then_by(expr)

    def then_by(self, expr) -> Self:
        # noqa: E501
        expect(self.__order_by__).to_be_truthy(Exception(
            'Order by expression has not been initialized yet. Use order_by() or order_by_descending() first.'
            ))
        if inspect.isfunction(expr):
            arguments = self.get_closure_parser().parse_select(expr)
            for arg in arguments:
                self.__append_order__(arg, -1)
            return self
        if type(expr) is str:
            return self.__append_order__(QueryField(expr), 1)
        return self.__append_order__(expr, 1)

    @overload
    def then_by_descending(self, expr: callable) -> Self:
        return self.then_by_descending(expr)
    
    @overload
    def then_by_descending(self, expr: str | QueryField) -> Self:
        return self.then_by_descending(expr)

    def then_by_descending(self, expr) -> Self:
        expect(self.__order_by__).to_be_truthy(Exception(
            'Order by expression has not been initialized yet. Use order_by() or order_by_descending() first.'
            ))
        if inspect.isfunction(expr):
            arguments = self.get_closure_parser().parse_select(expr)
            for arg in arguments:
                self.__append_order__(arg, -1)
            return self
        if type(expr) is str:
            return self.__append_order__(QueryField(expr), -1)
        return self.__append_order__(expr, -1)

    def group_by(self, *args, **kwargs):
        arguments = args
        if inspect.isfunction(args[0]):
            arguments = self.get_closure_parser().parse_select(*args, kwargs)
        self.__group_by__ = []
        for arg in arguments:
            if type(arg) is str:
                self.__group_by__.append(format_field_reference(arg))
            elif isinstance(arg, dict):
                for key in arg:
                    if arg[key] == 1:
                        self.__group_by__.append(format_field_reference(key))
                    elif arg[key] == 0:
                        break
                    else:
                        self.__group_by__.append({
                            '$expr': arg[key]
                        })
                    break
            else:
                raise Exception('Expected string or a dictionary object')
        return self

    def delete(self, collection):
        self.__select__ = None
        self.__update__ = None
        self.__insert__ = None
        self.___delete___ = True
        self.__set_collection__(collection)
        return self

    def __append(self, expr):
        if self.__where__ is None:
            self.__where__ = expr
        else:
            # get first property of current query
            prop = list(self.__where__.keys())[0]
            # get logical operator
            logical_operator = self.__last_logical
            # if last logical operator is equal with this property
            if prop == logical_operator:
                # append query expression
                expr[prop].append(expr)
            else:
                self.__where__ = {
                    logical_operator: [
                        self.__where__.copy(),
                        expr
                    ]
                }
        self.__left__ = None

    def distinct(self, value=True):
        self.__distinct__ = value
        return self

    def get_type(self) -> QueryExpressionType:
        """Returns the type of this expression

        Returns:
            QueryExpressionType: The type of this expression (SELECT, INSERT, UPDATE or DELETE)
        """
        if self.__update__ is not None:
            return QueryExpressionType.UPDATE
        elif self.__insert__ is not None:
            return QueryExpressionType.INSERT
        elif self.___delete___ is not None:
            return QueryExpressionType.DELETE
        return QueryExpressionType.SELECT


class SelectExpressionEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, QueryExpression):
            if obj.__select__ is not None:
                result = {
                    '$project': obj.__select__
                }
                if obj.__distinct__ is not None and obj.__distinct__ is True:
                    result['$distinct'] = True
                if obj.__limit__ > 0:
                    result['$limit'] = obj.__limit__
                    if obj.__skip__ > 0:
                        result['$skip'] = obj.__skip__
                if obj.__lookup__ is not None and len(obj.__lookup__) > 0:
                    result['$lookup'] = obj.__lookup__
                if obj.__order_by__ is not None:
                    result['$sort'] = obj.__order_by__
                if obj.__group_by__ is not None:
                    result['$group'] = obj.__group_by__
                if obj.__prepared__ is not None:
                    if obj.__where__ is not None:
                        result['$match'] = {
                            '$and': [
                                obj.__prepared__,
                                obj.__where__
                            ]
                        }
                    else:
                        result['$match'] = obj.__prepared__
                elif obj.__where__ is not None:
                    result['$match'] = obj.__where__
                return result
        return json.JSONEncoder.default(self, obj)
