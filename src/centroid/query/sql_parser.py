from sqlglot import exp, parse_one, Expression
from sqlglot.expressions import Table, Column, Where
from sqlglot import expressions
from .query_expression import QueryField, QueryEntity, QueryExpression, format_field_reference
from .open_data_parser import TokenOperator
from typing import List


class SqlParser():

    def __init__(self):
        pass

    def parse(self, sql: str) -> QueryExpression:
        expr: Expression = parse_one(sql)
        if expr.key == 'select':
            return self.parse_select(expr)
        raise Exception(f'The parsing of {expr.key} expression is not yet implemented.')

    def parse_select(self, expr: Expression) -> QueryExpression:
        result = QueryExpression()
        select = expr.find(exp.Select)
        print(select)
        # get collection
        table: Table = select.find(exp.From).expressions[0]
        result.from_collection(self.parse_table(table))
        # get select
        exprs: List[QueryField] = []
        for expression in select.expressions:
            if expression.key == 'column':
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

    def parse_column(self, column: Column) -> QueryField:
        if not column.alias:
            return QueryField(column.name)
        else:
            return QueryField(column.name).asattr(column.alias)

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
            oper = TokenOperator.Lte.value
        elif type(expr) is expressions.GT:
            oper = TokenOperator.Gt.value
        elif type(expr) is expressions.GTE:
            oper = TokenOperator.Gte.value
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
