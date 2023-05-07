from sqlglot import exp, parse_one, Expression
from sqlglot.expressions import Table, Column, Where, Alias
from sqlglot import expressions
from centroid.query import QueryField, QueryEntity, format_field_reference, TokenOperator, OpenDataQueryExpression
from typing import List
from centroid.common import SyncSeriesEventEmitter


class PseudoSqlParser():

    resolving_member: SyncSeriesEventEmitter
    resolving_join_member: SyncSeriesEventEmitter
    resolving_method: SyncSeriesEventEmitter

    def __init__(self):
        self.resolving_member = SyncSeriesEventEmitter()
        self.resolving_join_member = SyncSeriesEventEmitter()
        self.resolving_method = SyncSeriesEventEmitter()

    def parse(self, sql: str) -> OpenDataQueryExpression:
        expr: Expression = parse_one(sql)
        if expr.key == 'select':
            return self.parse_select(expr)
        raise Exception(f'The parsing of {expr.key} expression is not yet implemented.')

    def parse_select(self, expr: Expression) -> OpenDataQueryExpression:
        result = OpenDataQueryExpression()
        select = expr.find(exp.Select)
        print(select)
        # get collection
        table: Table = select.find(exp.From).expressions[0]
        result.from_collection(self.parse_table(table))
        # get select
        exprs: List[QueryField] = []
        for expression in select.expressions:
            if expression.key == 'column' or expression.key == 'alias':
                exprs.append(self.parse_column(expression))
        result.select(*exprs)
        # get where
        where: Where = expr.find(exp.Where)
        if where is not None:
            result.__where__ = self.parse_where(where)
        return result

    def parse_table(self, table: Table) -> QueryEntity:
        if not table.alias:
            return QueryEntity(table.name)
        else:
            return QueryEntity(table.name, table.alias)

    def parse_column(self, column: Column | Alias) -> QueryField:
        if not column.alias:
            return QueryField(column.name)
        else:
            if isinstance(column.this, Column):
                return QueryField(column.this.name).asattr(column.alias)
            else:
                raise Exception('Unsupported column expression')

    def parse_where(self, where: Where):
        expr = where.args['this']
        return self.parse_common(expr)
    
    def parse_common(self, expr):
        if isinstance(expr, expressions.Column):
            return format_field_reference(expr.alias_or_name)
        elif isinstance(expr, expressions.Literal):
            return self.parse_literal(expr)
        elif isinstance(expr, expressions.Connector):
            return self.parse_logical(expr)
        elif isinstance(expr, expressions.Binary):
            return self.parse_comparison(expr)
        raise Exception(f'Expression of type {type(expr)} is not implemented yet.')

    def parse_logical(self, expr):
        oper: str = None
        if type(expr) is expressions.And:
            oper = TokenOperator.And.value
        elif type(expr) is expressions.Or:
            oper = TokenOperator.Or.value
        if oper is None:
            raise Exception(f'Expression of type {type(expr)} is not implemented yet.')
        return {
                oper: [
                    self.parse_common(expr.args['this']),
                    self.parse_common(expr.expression),
                ]
            }

    def parse_comparison(self, expr):
        oper: str = None
        if type(expr) is expressions.EQ:
            oper = TokenOperator.Eq.value
        elif type(expr) is expressions.NEQ:
            oper = TokenOperator.Ne.value
        elif type(expr) is expressions.LT:
            oper = TokenOperator.Lt.value
        elif type(expr) is expressions.LTE:
            oper = TokenOperator.Le.value
        elif type(expr) is expressions.GT:
            oper = TokenOperator.Gt.value
        elif type(expr) is expressions.GTE:
            oper = TokenOperator.Ge.value
        elif type(expr) is expressions.Add:
            oper = TokenOperator.Add.value
        elif type(expr) is expressions.Mul:
            oper = TokenOperator.Mul.value
        elif type(expr) is expressions.Sub:
            oper = TokenOperator.Sub.value
        elif type(expr) is expressions.Div:
            oper = TokenOperator.Div.value
        if oper is None:
            raise Exception(f'Expression of type {type(expr)} is not implemented yet.')
        return {
                oper: [
                    self.parse_common(expr.args['this']),
                    self.parse_common(expr.expression),
                ]
            }

    def parse_literal(self, expr: expressions.Literal):
        if type(expr) is expressions.Literal:
            if expr.is_int:
                return int(expr.this)
            elif expr.is_number:
                return float(expr.this)
            else:
                return expr.this
