import ast
from dill.source import getsource
from ..common import expect, SyncSeriesEventEmitter

class LamdaParser:
    def __init__(self):
        self.resolving_member = SyncSeriesEventEmitter()
        self.resolving_join_member = SyncSeriesEventEmitter()
        self.resolving_method = SyncSeriesEventEmitter()
    
    def parse_filter(self, func, params:dict = None):
        module:ast.Module = ast.parse(getsource(func).strip())
        # set params
        self.params = params;
        # get function call
        call: ast.Call = module.body[0].value
        # the first argument is the lambda function
        lambda_func:ast.Lambda = call.args[0]
        # get arguments
        self.args = lambda_func.args.args
        # get expression
        result = self.parse_common(lambda_func.body)
        return result

    def parse_select(self, func, params:dict = None):
        module:ast.Module = ast.parse(getsource(func).strip())
        # set params
        self.params = params;
        # get function call
        call: ast.Call = module.body[0].value
        # the first argument is the lambda function
        lambda_func:ast.Lambda = call.args[0]
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
        result[op] = [];
        for value in expr.values:
            result[op].append(self.parse_common(value))
        return result

    def parse_comparison(self, expr: ast.Compare):
        ops_type = type(expr.ops[0])
        op = None
        if ops_type is ast.Eq or ops_type is ast.Is:
            op = '$eq'
        elif ops_type is ast.NotEq  or ops_type is ast.IsNot:
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

    def parse_literal(self, expr:ast.Constant):
        return expr.value;
    
    def parse_member(self, expr:ast.Attribute):
        attr:str = '$' + expr.attr
        if type(expr.value) is ast.Name:
            # a simple member reference like x.category
            # expect object name to be the first argument of lamda function
            obj:ast.Name = expr.value
            expect(obj.id).to_equal(self.args[0].arg, Exception('Invalid member expression. Expected an expression which uses a member of the first argument'))
            return attr
        if type(expr.value) is ast.Attribute:
            # a nested member like x.address.streetAddress
            obj:ast.Attribute = expr.value
            while type(obj) is ast.Attribute:
                if type(obj.value) is ast.Name:
                    attr = '$' + obj.attr + '.' + attr[1:]
                # get next value
                obj = obj.value
            return attr

    def parse_sequence(self, expr):
        sequence = {};
        if type(expr) is ast.List:
            for elt in expr.elts:
                attr = self.parse_common(elt)
                if (type(attr) is str):
                    sequence.__setitem__(attr[1:], 1)
                elif (type(attr) is dict):
                    for key in attr:
                        sequence.__setitem__(key, attr[key])
                        break
                else:
                    raise Exception('Invalid sequence attribute')
            return sequence
        if type(expr) is ast.Dict:
            dict_expr:ast.Dict = expr
            for i,key in dict_expr.keys:
                expect(type(key)).to_equal(ast.Constant, Exception('Expected constant'))
                attr = key.value
                value = self.parse_common(dict_expr.values[i])
                sequence.__setattr__(attr, value)
            return sequence
        raise Exception('Unsupported sequence expression')
       
    def parse_binary(self, expr:ast.BinOp):
        # find binary operator
        op = None
        if type(expr) is ast.Add:
            op = '$add'
        if type(expr) is ast.Sub:
            op = '$subtract'
        if type(expr) is ast.Mult:
            op = '$multiply'
        if type(expr) is ast.Div:
            op = '$divide'
        # validate operator
        expect(op).to_be_truthy(Exception('Binary operator is invalid or has not been implemented yet'))
        result = dict()
        result[op] = {
            self.parse_common(expr.left),
            self.parse_common(expr.right)
        }
        return result
    
    def parse_identifier(self, expr:ast.Name):
        expect(expr.id in self.params).to_be_truthy(Exception('The specified param cannot be found'))
        return self.params.get(expr.id);

    def parse_common(self, expr):
        # and try to parse it base on type
        if type(expr) is ast.Attribute:
            return self.parse_member(expr)
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

    





