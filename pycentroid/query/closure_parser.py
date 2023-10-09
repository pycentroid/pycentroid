import ast
import re
from dill.source import getsource
from pycentroid.common import expect, SyncSeriesEventEmitter, AnyObject
from .query_field import is_qualified_reference, format_any_field_reference
from .method_parser import MethodParserDialect, InstanceMethodParserDialect


def try_extract_closure_from(func: callable, throw_error: bool = False):
    source = getsource(func).strip()
    final_source = source if re.search('^(\s+)?def\s', source) is not None else ast.parse(f'func0({source})')
    module: ast.Module = ast.parse(final_source)
    if type(module.body[0]) is ast.FunctionDef:
        return module.body[0]
    expr: ast.Expr = module.body[0]
    if len(expr.value.args) > 0:
        for arg in expr.value.args:
            if type(arg) is ast.Lambda:
                return arg
    args = expr.value.keywords
    for arg in args:
        if type(arg.value) is ast.Lambda:
            return arg.value
    if throw_error is True:
        raise Exception('Invalid expression. Expected a lambda function.')
    return None


# noinspection PyUnusedLocal
def count(field):
    pass


class ClosureParser:
    def __init__(self):
        self.resolving_member = SyncSeriesEventEmitter()
        self.resolving_join_member = SyncSeriesEventEmitter()
        self.resolving_method = SyncSeriesEventEmitter()
        self.args = []
        self.params = {}
        # add method parsers
        self.__parsers__ = [
            MethodParserDialect(self),
            InstanceMethodParserDialect(self)
        ]

    def parse_filter(self, func, params: dict = None):

        expr = try_extract_closure_from(func)
        if expr is not None:
            self.params = params
            if type(expr) is ast.Lambda or type(expr) is ast.FunctionDef:
                self.args = expr.args.args
            if type(expr) is ast.FunctionDef and type(expr.body[0] is ast.Return):
                expect(type(expr.body[0].value)).to_equal(ast.Compare, Exception('Invalid def. Expected compare statement.'))
                return self.parse_common(expr.body[0].value)
            return self.parse_common(expr.body) 

        module: ast.Module = ast.parse(getsource(func).strip())
        # set params
        self.params = params
        if type(module.body[0].value) is ast.Lambda:
            # get func
            lambda_func: ast.Lambda = module.body[0].value
            # and args
            self.args = lambda_func.args.args
            # parse body
            return self.parse_common(lambda_func.body)
        if type(module.body[0].value) is ast.Call:
            # get function call
            call = module.body[0].value
            # the first argument is the lambda function
            lambda_func = call.args[0]
            # get arguments
            self.args = lambda_func.args.args
            # get expression
            return self.parse_common(lambda_func.body)
        raise TypeError('Invalid or unsupported lamda function')

    def parse_select(self, func, params: dict = None):
        module: ast.Module = ast.parse(getsource(func).strip())
        # set params
        self.params = params
        if type(module.body[0].value) is ast.Lambda:
            lambda_func: ast.Lambda = module.body[0].value
            self.args = lambda_func.args.args
            return self.parse_sequence(lambda_func.body)
        # get function call
        call = module.body[0].value
        # the first argument is the lambda function
        lambda_func = call.args[0]
        # get arguments
        self.args = lambda_func.args.args
        # get expression
        result = self.parse_sequence(lambda_func.body)
        return result

    def parse_logical(self, expr: ast.BoolOp):
        op = None
        if type(expr.op) is ast.And:
            op = '$and'
        if type(expr.op) is ast.Or:
            op = '$or'
        expect(op).to_be_truthy(Exception('Logical operator is invalid or has not been implemented yet'))
        result = dict()
        result[op] = []
        for value in expr.values:
            result[op].append(self.parse_common(value))
        return result

    def parse_comparison(self, expr: ast.Compare):
        ops_type = type(expr.ops[0])
        op = None
        if ops_type is ast.Eq or ops_type is ast.Is:
            op = '$eq'
        elif ops_type is ast.NotEq or ops_type is ast.IsNot:
            op = '$ne'
        elif ops_type is ast.Gt:
            op = '$gt'
        elif ops_type is ast.GtE:
            op = '$ge'
        elif ops_type is ast.Lt:
            op = '$lt'
        elif ops_type is ast.LtE:
            op = '$lte'
        # validate operator
        expect(op).to_be_truthy(Exception('Comparison operator is invalid or has not been implemented yet'))
        # format result
        result = dict()
        result[op] = [
            self.parse_common(expr.left),
            self.parse_common(expr.comparators[0])
        ]
        return result

    # noinspection PyMethodMayBeStatic
    def parse_literal(self, expr: ast.Constant) -> object:
        return expr.value

    def parse_member(self, expr: ast.Attribute):
        attr: str = '$' + expr.attr
        if type(expr.value) is ast.Name:
            # a simple member reference like x.category
            # expect object name to be the first argument of lamda function
            obj = expr.value
            if obj.id != self.args[0].arg:
                attr = '$' + obj.id + '.' + attr[1:]
            event = AnyObject(target=self, member=attr)
            if is_qualified_reference(attr):
                event.fully_qualified_name = attr
                self.resolving_join_member.emit(event)
            else:
                self.resolving_member.emit(event)
            if event.member != attr:
                return event.member
            return attr
        if type(expr.value) is ast.Attribute:
            # a nested member like x.address.streetAddress
            obj = expr.value
            while type(obj) is ast.Attribute:
                if type(obj.value) is ast.Name or type(obj.value) is ast.Attribute:
                    attr = '$' + obj.attr + '.' + attr[1:]
                # get next value
                obj = obj.value
            # emit event
            event = AnyObject(target=self, member=attr)
            if is_qualified_reference(attr):
                event.fully_qualified_name = attr
                self.resolving_join_member.emit(event)
            else:
                self.resolving_member.emit(event)
            if event.member != attr:
                return event.member
            return attr

    def parse_sequence(self, expr):
        sequence = {}
        if type(expr) is ast.Call:
            expect(type(expr.keywords)).to_equal(list, 'Sequence call expression must be an array of named params')
            for keyword in expr.keywords:
                keyword_expr = self.parse_common(keyword.value)
                if type(keyword_expr) is str:
                    if keyword_expr == '$' + keyword.arg:
                        # simplify expression
                        sequence[keyword.arg] = 1
                    else:
                        sequence[keyword.arg] = keyword_expr
                else:
                    sequence[keyword.arg] = keyword_expr
            return sequence
        if type(expr) is ast.List or type(expr) is ast.Tuple:
            for elt in expr.elts:
                attr = self.parse_common(elt)
                if type(attr) is str:
                    sequence.__setitem__(attr[1:], 1)
                elif type(attr) is dict:
                    for key in attr:
                        sequence.__setitem__(key, attr[key])
                        break
                else:
                    raise Exception('Invalid sequence attribute')
            return sequence
        if type(expr) is ast.Dict:
            dict_expr: ast.Dict = expr
            for i, key in dict_expr.keys:
                expect(type(key)).to_equal(ast.Constant, Exception('Expected constant'))
                attr = key.value
                value = self.parse_common(dict_expr.values[i])
                sequence.__setattr__(attr, value)
            return sequence
        else:
            # parse as sequence
            raise Exception('Invalid sequence attribute. Expected a list or tuple')

    def parse_binary(self, expr: ast.BinOp):
        # find binary operator
        op = None
        if type(expr.op) is ast.Add:
            op = '$add'
        if type(expr.op) is ast.Sub:
            op = '$subtract'
        if type(expr.op) is ast.Mult:
            op = '$multiply'
        if type(expr.op) is ast.Div:
            op = '$divide'
        # validate operator
        expect(op).to_be_truthy(Exception('Binary operator is invalid or has not been implemented yet'))
        result = dict()
        result[op] = [
            self.parse_common(expr.left),
            self.parse_common(expr.right)
        ]
        return result

    def parse_identifier(self, expr: ast.Name):
        expect(expr.id in self.params).to_be_truthy(Exception('The specified param cannot be found'))
        return self.params.get(expr.id)

    def parse_method_call(self, expr: ast.Call or ast.Attribute):
        arguments = []
        instance_method = False
        if type(expr.func) is ast.Attribute:
            method = expr.func.attr
            instance_method = True
            arguments.append(self.parse_common(expr.func.value))
        else:
            method = format_any_field_reference(expr.func.id)
        for arg in expr.args:
            arguments.append(self.parse_common(arg))
        event = AnyObject(target=self, method=method, instance_method=instance_method)
        self.resolving_method.emit(event)
        if event.resolve is not None:
            return event.resolve(*arguments)
        else:
            Exception(f'{event.method}[] method has not yet implemented.')

    def parse_subscript(self, expr: ast.Subscript):
        expr1 = self.parse_common(expr.value)
        # get start
        # noinspection PyUnresolvedReferences
        start = 0 if expr.slice.lower is None else expr.slice.lower.n
        result = {
            '$substr': [
                expr1,
                start
            ]
        }
        # get length
        # noinspection PyUnresolvedReferences
        length = 0 if expr.slice.upper is None else expr.slice.upper.n
        # and append param
        if length > 0:
            result['$substr'].append(length)
        return result

    def parse_if(self, expr: ast.IfExp):
        if_expr = self.parse_common(expr.test)
        then_expr = self.parse_common(expr.body)
        else_expr = self.parse_common(expr.orelse)
        return {
            '$cond': [
                if_expr,
                then_expr,
                else_expr
            ]
        }

    def parse_common(self, expr):
        # and try to parse it base on type
        if type(expr) is ast.Attribute:
            return self.parse_member(expr)
        if type(expr) is ast.Subscript:
            return self.parse_subscript(expr)
        if type(expr) is ast.IfExp:
            return self.parse_if(expr)
        if type(expr) is ast.Call:
            return self.parse_method_call(expr)
        if type(expr) is ast.BoolOp:
            return self.parse_logical(expr)
        if type(expr) is ast.Compare:
            return self.parse_comparison(expr)
        if type(expr) is ast.Constant:
            return self.parse_literal(expr)
        if type(expr) is ast.BinOp:
            return self.parse_binary(expr)
        if type(expr) is ast.Attribute:
            return self.parse_binary(expr)
        if type(expr) is ast.Name:
            return self.parse_identifier(expr)
