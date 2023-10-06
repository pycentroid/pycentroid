from pycentroid.query import ClosureParser, select
from dill.source import getsource
import ast
import re


def get_closure_from_source(source: str, throw_error: bool = False):

    final_source = source if re.search('^(\s+)?def\s', source) is not None else ast.parse(f'func0({source})')
    module: ast.Module = ast.parse(final_source)
    if type(module.body[0]) is ast.FunctionDef:
        return module.body[0];
    expr: ast.Expr = module.body[0]
    if len(expr.value.args) > 0:
        for arg in expr.value.args:
            if type(arg) is ast.Lambda:
                return arg
    args = expr.value.keywords
    for arg in args:
        if type(arg.value) is ast.Lambda:
            return arg.value
    if throw_error:
        raise Exception('Invalid expression. Expected a lambda function.')
    return None
        
    
def test_find_lambda():
    str = "orderStatus = 'OrderPickup', where = lambda x, orderStatus: x.orderStatus.alternateName == orderStatus"
    arg0 = get_closure_from_source(str)
    assert type(arg0) is ast.Lambda
    str = "lambda x, orderStatus: x.orderStatus.alternateName == orderStatus"
    arg0 = get_closure_from_source(str)
    assert type(arg0) is ast.Lambda
    str = "where=lambda x, orderStatus: x.orderStatus.alternateName == orderStatus, orderStatus = 'OrderPickup'"
    arg0 = get_closure_from_source(str)
    assert type(arg0) is ast.Lambda
    str = "lambda x, orderStatus: x.orderStatus.alternateName == orderStatus, orderStatus = 'OrderPickup'"
    arg0 = get_closure_from_source(str)
    assert type(arg0) is ast.Lambda

def test_find_def():
    
    def where1(x, orderStatus): 
        return x.orderStatus.alternateName == orderStatus

    str = getsource(where1).strip()
    expr = get_closure_from_source(str);
    assert expr is not None


def test_select_func():

    func = lambda x: select(productName=x.name, productCategory=x.category)  # noqa:E731
    # get module
    module: ast.Module = ast.parse(getsource(func).strip())
    # get function
    body: ast.Assign = module.body[0]
    assert type(body.value.body) is ast.Call
    # get arguments
    selectFunc = body.value.body.func
    assert type(selectFunc) is ast.Name
    assert selectFunc.id == 'select'

    selectArgs = body.value.body.keywords
    assert type(selectArgs) is list

    arg0 = selectArgs[0]
    assert type(arg0) is ast.keyword
    assert arg0.arg == 'productName'
    assert type(arg0.value) is ast.Attribute


def test_func():

    func = lambda x, category: x.category == category and x.price + 100 > 900  # noqa:E731
    # get module
    module: ast.Module = ast.parse(getsource(func).strip())
    # get function
    body: ast.Assign = module.body[0]
    # get arguments
    args = body.value.args.args
    assert len(args) == 2
    params = {
        'category': 'Laptops'
    }
    assert params.get('category') == 'Laptops'
    assert 'category' in params


def test_sequence_list():

    func = lambda x: [x.id, x.name, x.category, x.releaseDate, x.price]  # noqa:E731
    # get module
    module: ast.Module = ast.parse(getsource(func).strip())
    # get function
    body: ast.Assign = module.body[0]
    # get attribute
    select: ast.List = body.value.body
    assert type(select) is ast.List
    for attribute in select.elts:
        assert type(attribute) is ast.Attribute


def test_substring_expr():

    func = lambda x:  [x.category[1:]]  # noqa:E731
    # get module
    module = ast.parse(getsource(func).strip())
    # get function
    body = module.body[0]
    # get attribute
    select: ast.List = body.value.body
    assert type(select) is ast.List
    substr = select.elts[0]
    assert type(substr) is ast.Subscript
    assert substr.slice.lower.n == 1

    func = lambda x:  [x.category[:5]]  # noqa:E731
    # get module
    module: ast.Module = ast.parse(getsource(func).strip())
    # get function
    body: ast.Assign = module.body[0]
    # get attribute
    select: ast.List = body.value.body
    assert type(select) is ast.List
    substr = select.elts[0]
    assert type(substr) is ast.Subscript
    assert substr.slice.upper.n == 5


def test_if_then_else_expr():

    func = lambda x: ['active' if x.active is True else 'inactive']  # noqa:E731
    # get module
    module: ast.Module = ast.parse(getsource(func).strip())
    # get function
    body: ast.Assign = module.body[0]
    # get attribute
    select: ast.List = body.value.body
    assert type(select) is ast.List

    if_expr = select.elts[0]
    assert type(if_expr) is ast.IfExp
    assert type(if_expr.test) is ast.Compare
    assert type(if_expr.body) is ast.Constant
    assert type(if_expr.orelse) is ast.Constant


def test_sequence_dict():

    func = lambda x: {'id': x.id, 'name': x.name, 'price': x.price}  # noqa:E731
    # get module
    module: ast.Module = ast.parse(getsource(func).strip())
    # get function
    body: ast.stmt = module.body[0]
    # get attribute
    select: ast.List = body.value.body
    assert type(select) is ast.Dict
    for attribute in select.keys:
        assert type(attribute) is ast.Constant
    for attribute in select.values:
        assert type(attribute) is ast.Attribute


def test_parse_filter():
    parser = ClosureParser()
    result = parser.parse_filter(lambda x: x.category == 'Laptops' and x.price > 900)
    assert result == {
        '$and': [
            {
                '$eq': [
                    '$category', 'Laptops'
                ]
            },
            {
                '$gt': [
                    '$price', 900
                ]
            }
        ]
    }


def test_parse_or():
    parser = ClosureParser()
    result = parser.parse_filter(lambda x: x.category == 'Laptops' or x.category == 'Desktops')
    assert result == {
        '$or': [
            {
                '$eq': [
                    '$category', 'Laptops'
                ]
            },
            {
                '$eq': [
                    '$category', 'Desktops'
                ]
            }
        ]
    }


def test_parse_with_params():
    parser = ClosureParser()
    result = parser.parse_filter(lambda x, category, otherCategory: x.category == category or x.category == otherCategory, {  # noqa:E501
        'category': 'Laptops',
        'otherCategory': 'Desktops'
    })
    assert result == {
        '$or': [
            {
                '$eq': [
                    '$category', 'Laptops'
                ]
            },
            {
                '$eq': [
                    '$category', 'Desktops'
                ]
            }
        ]
    }


def test_parse_sequence():
    parser = ClosureParser()
    result = parser.parse_select(lambda x: [x.id, x.name, x.price])
    assert result == {
        'id': 1,
        'name': 1,
        'price': 1
    }

    result = parser.parse_select(lambda x: [x.id, x.givenName, x.familyName, x.address.streetAddress])
    assert result == {
        'id': 1,
        'givenName': 1,
        'familyName': 1,
        'address.streetAddress': 1
    }
