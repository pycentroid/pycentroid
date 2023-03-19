
from .query_expression import QueryExpression
from ..common import NotImplementError
from .utils import SqlUtils

class SqlDialect:

    def eq(left, right):
        return ''
    
    def ne(left, right):
        return ''

class SqlFormatter:
    def __init__(self, dialect = None):
        self.__dialect__ = SqlDialect() if dialect is None else dialect
    
    def format_select(self, query:QueryExpression):
        raise NotImplementedError()

    def format_update(self, query:QueryExpression):
        raise NotImplementedError()
    
    def format_delete(self, query:QueryExpression):
        raise NotImplementedError()
    
    def format_insert(self, query:QueryExpression):
        raise NotImplementedError()