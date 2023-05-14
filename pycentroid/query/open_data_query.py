import inspect
from .query_expression import QueryExpression
from pycentroid.common import expect
import logging
from dill.source import getsource
from typing import List


def any(expr: callable):
    expect(inspect.isfunction(expr)).to_be_truthy(TypeError('Expected callable.'))
    logging.debug(getsource(expr).strip())
    # parse expression
    select = OpenDataQueryExpression().get_closure_parser().parse_select(expr)
    # get first argument
    keys = list(select.keys())
    expect(len(keys)).to_equal(1, Exception('Expected a single field select expression.'))
    select0 = keys[0]
    # and split
    return OpenDataQueryExpression(select0)


class OpenDataQueryExpression(QueryExpression):

    __expand__: List[QueryExpression]
    
    def __init__(self, collection=None):
        super().__init__(collection)
        self.__expand__ = []

    def expand(self, *args):
        for arg in args:
            if isinstance(arg, QueryExpression):
                self.__expand__.append(arg)
            elif inspect.isfunction(arg):
                self.__expand__.append(any(arg))
            else:
                raise TypeError('Invalid argument type.Expected closure or query expression')
        return self
