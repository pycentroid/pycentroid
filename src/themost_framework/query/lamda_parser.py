import ast
from .query_expression import QueryExpression
from ..common import expect

class LamdaParser:
    
    def parseFilter(self, func, params:dict = None):
        module:ast.Module = ast.parse(getsource(func).strip())
        # set params
        self.params = params;
        # get function
        body: ast.Assign = module.body[0]
        # get function arguments
        self.args = body.value.args.args
        # get expression
        innerBody = body.value.body
        if innerBody is ast.BoolOp:
            return self.parseLogical(expr)
    
    def parseLogical(self, expr: ast.BoolOp):
        op = None
        op_type = type(expr.op)
        if type(op_type) is ast.And:
            op = '$and'
        if type(op_type) is ast.Or:
            op = '$or'
        expect(op).to_be_truthy('Logical operator is invalid or has not been implemented yet')
        result = dict()
        result[op] = [];
        for value in values:
            result[op].append(self.parseCommon(value))
        return result

    def parseComparison(self, expr: ast.Compare):
        ops_type = type(expr.ops[0])
        comparison = None
        if ops_type is ast.Eq or ops_type is ast.Is:
            comparison = '$eq'
        elif ops_type is ast.NotEq  or ops_type is ast.IsNot:
            comparison = '$ne'
        elif ops_type is ast.Gt:
            comparison = '$gt'
        elif ops_type is ast.GtE:
            comparison = '$ge'
        elif ops_type is ast.Lt:
            comparison = '$lt'
        elif ops_type is ast.LtE:
            comparison = '$lte'
        # validate operator
        expect(comparison).to_be_truthy('Comparison operator is invalid or has not been implemented yet')
        # format result
        result = dict()
        result[comparison] = {
            self.parseCommon(expr.left),
            self.parseCommon(expr.comparators[0])
        }

    def parseLiteral(self, expr:ast.Constant):
        return expr.value;

    def parseCommon(self, expr):
        # get expression type
        expr_type = type(expr)
        # and try to parse it base on type
        if expr_type is ast.BoolOp:
            return self.parseLogical(expr)
        if expr_type is ast.Compare:
            return self.parseComparison(expr)
        if expr_type is ast.Constant:
            return self.parseLiteral(expr)
    


