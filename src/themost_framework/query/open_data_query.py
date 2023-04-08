import inspect
import typing
from copy import deepcopy
from .query_expression import QueryExpression
from .query_field import get_first_key
from themost_framework.common import expect


def any(expr: callable):
    expect(inspect.isfunction(expr)).to_be_truthy(TypeError('Expected closure'))
    # parse expression
    select = OpenDataQueryExpression().get_closure_parser().parse_select(expr);
    # get first argument
    select0 = get_first_key(select)
    expand = select0.split('.')
    exprs = list(map(lambda x:OpenDataQueryExpression(x), expand))
    current = exprs[0]
    index = 1
    while index < len(expand):
        expr = exprs[index]
        current.__expand__.append(expr)
        current = expr
        index += 1
    # and split
    return exprs[0]


class OpenDataQueryExpression(QueryExpression):
    
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