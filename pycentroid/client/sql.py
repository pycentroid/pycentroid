from sqlglot import exp, parse_one, Expression
from sqlglot.expressions import Table, Column, Where, Condition, Func, Order, Group, Limit, Offset, From, Star
from sqlglot import expressions
from pycentroid.query import QueryField, QueryEntity, format_field_reference, TokenOperator, OpenDataQueryExpression
from typing import List, Any
from pycentroid.common import SyncSeriesEventEmitter
from types import SimpleNamespace


class ResolvingMemberEventArgs(SimpleNamespace):
    member: str
    original_member: str
    target: object


class ResolvingJoinMemberEventArgs(SimpleNamespace):
    member: str
    fully_qualified_name: str
    target: object


class ResolvingMethodEventArgs(SimpleNamespace):
    method: str
    args: List[object]
    target: object


PseudoSqlMethods = [
    [
        'day', 'dayOfMonth'
    ]
]


class PseudoSqlParserDialect:

    __methods__: dict

    def __init__(self):
        self.__methods__ = dict()
        for method in PseudoSqlMethods:
            self.__methods__[method[0]] = method[1]

    def resolving_method(self, event: ResolvingMethodEventArgs):
        if event.method in self.__methods__:
            event.method = self.__methods__[event.method]


class PseudoSqlParser:

    resolving_member: SyncSeriesEventEmitter
    resolving_join_member: SyncSeriesEventEmitter
    resolving_method: SyncSeriesEventEmitter

    def __init__(self):
        self.resolving_member = SyncSeriesEventEmitter()
        self.resolving_join_member = SyncSeriesEventEmitter()
        self.resolving_method = SyncSeriesEventEmitter()

        self.resolving_method.subscribe(PseudoSqlParserDialect().resolving_method)

    def parse(self, sql: str) -> OpenDataQueryExpression:
        expr: Expression = parse_one(sql)
        if expr.key == 'select':
            return self.parse_select(expr)
        raise Exception(f'The parsing of {expr.key} expression is not yet implemented.')

    def parse_select(self, expr: Expression) -> OpenDataQueryExpression:
        result = OpenDataQueryExpression()
        select = expr.find(exp.Select)
        # get collection
        table: From = select.find(exp.From)
        result.from_collection(self.parse_table(table))
        # get select
        exprs: List[QueryField | dict[Any, dict] | Any] = []
        index = 0
        for expression in select.expressions:
            if isinstance(expression, expressions.Star):
                exprs.append(self.parse_column(expression))
            elif expression.key == 'column' or expression.key == 'alias':
                exprs.append(self.parse_column(expression))
            else:
                col_expression = self.parse_common(expression)
                index += 1
                alias = f'field{index}'
                exprs.append({
                    alias: col_expression
                })
        result.select(*exprs)
        # get where
        where: Where = expr.find(exp.Where)
        if where is not None:
            result.__where__ = self.parse_where(where)
        order: Order = expr.find(exp.Order)
        if order is not None:
            result.__order_by__ = self.parser_order_by(order)
        group: Group = expr.find(exp.Group)
        if group is not None:
            result.__group_by__ = self.parser_group_by(group)
        limit: Limit = expr.find(exp.Limit)
        if limit is not None:
            result.__limit__ = self.parse_literal(limit.args['expression'])
        offset: Offset = expr.find(exp.Offset)
        if offset is not None:
            result.__skip__ = self.parse_literal(offset.args['expression'])
        return result

    # noinspection PyMethodMayBeStatic
    def parse_table(self, table: Table | From) -> QueryEntity:
        if not table.alias:
            return QueryEntity(table.name)
        else:
            return QueryEntity(table.name, table.alias)

    def parse_column(self, column: Condition | Star) -> QueryField | dict[Any, dict] | Any:
        if not column.alias:
            return QueryField(column.name)
        else:
            if isinstance(column.this, Column):
                return QueryField(column.this.name).asattr(column.alias)
            elif isinstance(column.this, Func):
                method: dict = self.parse_method(column.this)
                return {
                    column.alias: method
                }
            else:
                raise Exception('Unsupported column expression')

    def parse_where(self, where: Where):
        expr = where.args['this']
        return self.parse_common(expr)

    def parser_order_by(self, order: Order):
        exprs = []
        for expression in order.expressions:
            exprs.append({
                '$expr': self.parse_common(expression.args['this']),
                'direction': 'desc' if expression.args['desc'] else 'asc'
            })
        return exprs

    def parser_group_by(self, group: Group):
        exprs = []
        for expression in group.expressions:
            exprs.append(self.parse_common(expression))
        return exprs

    def parse_common(self, expr):
        if isinstance(expr, expressions.Column):
            event = ResolvingMemberEventArgs(
                member=format_field_reference(expr.alias_or_name), original_member=expr.alias_or_name, target=self
                )
            self.resolving_member.emit(event)
            return format_field_reference(event.member)
        elif isinstance(expr, expressions.Literal):
            return self.parse_literal(expr)
        elif isinstance(expr, expressions.Connector):
            return self.parse_logical(expr)
        elif isinstance(expr, expressions.Binary):
            return self.parse_comparison(expr)
        elif isinstance(expr, expressions.Func):
            return self.parse_method(expr)
        elif isinstance(expr, expressions.Between):
            return {
                '$and': [
                    {
                        '$ge': [
                            self.parse_common(expr.args['this']),
                            self.parse_common(expr.args['low']),
                        ]
                    },
                    {
                        '$le': [
                            self.parse_common(expr.args['this']),
                            self.parse_common(expr.args['high']),
                        ]
                    }
                ]
            }
        raise Exception(f'Expression of type {type(expr)} is not implemented yet.')

    def parse_method(self, expr: Func):
        args = []
        arg_types = list(filter(lambda x: x != 'expressions', expr.arg_types))
        for arg in arg_types:
            if arg in expr.args:
                args.append(self.parse_common(expr.args[arg]))
        event = ResolvingMethodEventArgs(
                method=expr.key, args=args, target=self
                )
        self.resolving_method.emit(event)
        method = format_field_reference(event.method)
        return {
            method: event.args
        }

    def parse_logical(self, expr):
        oper: str | None = None
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
        oper: str | None = None
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
        elif type(expr) is expressions.Mod:
            oper = TokenOperator.Mod.value
        elif type(expr) is expressions.Like:
            pattern = expr.expression.this
            pattern = pattern[1:] if pattern.startswith('%') else '^' + pattern
            pattern = pattern[:-1] if pattern.endswith('%') else pattern + '$'
            return {
                '$eq': [
                    {
                        '$regexMatch': {
                            'input': self.parse_common(expr.args['this']),
                            'regex': pattern
                        }
                    },
                    True
                ]
            }
        if oper is None:
            raise Exception(f'Expression of type {type(expr)} is not implemented yet.')
        return {
                oper: [
                    self.parse_common(expr.args['this']),
                    self.parse_common(expr.expression),
                ]
            }

    # noinspection PyMethodMayBeStatic
    def parse_literal(self, expr: expressions.Literal):
        if type(expr) is expressions.Literal:
            if expr.is_int:
                return int(expr.this)
            elif expr.is_number:
                return float(expr.this)
            else:
                return expr.this
