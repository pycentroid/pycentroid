import re
from enum import Enum

from pycentroid.common.datetime import isdatetime, getdatetime
from pycentroid.common.events import SyncSeriesEventEmitter
from pycentroid.common.objects import AnyDict
from .query_field import format_any_field_reference, get_first_key


class TokenOperator(Enum):
    Not = '$not'
    Mul = '$mul'
    Div = '$div'
    Mod = '$mod'
    Add = '$add'
    Sub = '$sub'
    Lt = '$lt'
    Gt = '$gt'
    Le = '$lte'
    Ge = '$gte'
    Eq = '$eq'
    Ne = '$ne'
    In = '$in'
    NotIn = '$nin'
    And = '$and'
    Or = '$or'

    @staticmethod
    def is_logical_operator(op):
        if op is None:
            return False
        return re.search(r'^(\$and|\$or|\$not|\$nor)$', op.value)

    @staticmethod
    def is_arithmetic_operator(op):
        if op is None:
            return False
        pattern = r'^(\$add|\$mul|\$div|\$sub|\$mod)$'
        if type(op) is str:
            return re.search(pattern, op)
        return re.search(pattern, op.value)

    @staticmethod
    def is_comparison_operator(op):
        if op is None:
            return False
        return re.search(r'^(\$lt|\$lte|\$gt|\$gte|\$eq|\$ne)$', op.value)


class TokenType(Enum):
    Literal = 'Literal'
    Identifier = 'Identifier'
    Syntax = 'Syntax'


class Token:
    source = None
    syntax = None
    """A string which represents a syntax token
    Returns:
        str
    """
    identifier = None
    """A string which represents an identifier token
    Returns:
        str
    """

    def __init__(self, type):
        self.type = type

    def is_paren_open(self):
        return self.type == TokenType.Syntax and self.syntax == '('

    def is_paren_close(self):
        return self.type == TokenType.Syntax and self.syntax == ')'

    def is_slash(self):
        return self.type == TokenType.Syntax and self.syntax == '/'

    def is_comma(self):
        return self.type == TokenType.Syntax and self.syntax == ','

    def is_equal(self):
        return self.type == TokenType.Syntax and self.syntax == '='

    def is_negative(self):
        return self.type == TokenType.Syntax and self.syntax == '-'

    def is_semicolon(self):
        return self.type == TokenType.Syntax and self.syntax == ';'

    def is_query_option(self):
        return self.type == TokenType.Identifier and self.identifier.startswith('$')

    def is_alias(self):
        return self.type == TokenType.Identifier and self.identifier is not None and self.identifier == 'as'

    def is_order_direction(self):
        return self.type == TokenType.Identifier and self.identifier is not None \
            and (self.identifier.lower() == 'asc' or self.identifier.lower() == 'desc')


class LiteralType(Enum):
    Null = 'Null'
    String = 'String'
    Boolean = 'Boolean'
    Single = 'Single'
    Double = 'Double'
    Decimal = 'Decimal'
    Int = 'Int'
    Long = 'Long'
    Binary = 'Binary'
    DateTime = 'DateTime'
    Guid = 'Guid'
    Duration: 'Duration'


class StringType(Enum):
    NoneString = 'None'
    Binary = 'Binary'
    DateTime = 'DateTime'
    Guid = 'Guid'
    Time = 'Time'
    DateTimeOffset = 'DateTimeOffset'


class LiteralToken(Token):
    value = None

    @staticmethod
    def NegativeInfinity():
        return LiteralToken(float('NaN'), LiteralType.Double)

    @staticmethod
    def PositiveInfinity():
        return LiteralToken(float('NaN'), LiteralType.Double)

    @staticmethod
    def TrueValue():
        return LiteralToken(True, LiteralType.Boolean)

    @staticmethod
    def FalseValue():
        return LiteralToken(False, LiteralType.Boolean)

    @staticmethod
    def NullValue():
        return LiteralToken(None, LiteralType.Null)

    def __init__(self, value, literal_type):
        super().__init__(TokenType.Literal)
        self.literal_type = literal_type
        self.value = value

    def __str__(self):

        if self.value is None:
            return 'null'

        if self.literal_type == LiteralType.String or self.literal_type == LiteralType.Guid:
            return 'guid\'' + self.value + '\''

        if self.literal_type == LiteralType.Binary:
            return 'binary\'' + str(self.value) + '\''

        if self.literal_type == LiteralType.Boolean:
            return 'false' if self.value is True else 'true'

        if self.literal_type == LiteralType.Duration:
            return 'duration\'' + str(self.value) + '\''

        if self.literal_type == LiteralType.DateTime:
            if isdatetime(self.value):
                return 'datetime\'' + getdatetime(self.value).isoformat() + '\''
            return 'datetime\'' + str(self.value) + '\''
        return str(self.value)


class IdentifierToken(Token):
    def __init__(self, name):
        super().__init__(TokenType.Identifier)
        self.identifier = name

    def __str__(self):
        return self.identifier


class SyntaxToken(Token):

    @staticmethod
    def ParenOpen():
        return SyntaxToken('(')

    @staticmethod
    def ParenClose():
        return SyntaxToken(')')

    @staticmethod
    def Slash():
        return SyntaxToken('/')

    @staticmethod
    def Comma():
        return SyntaxToken(',')

    @staticmethod
    def Negative():
        return SyntaxToken('-')

    @staticmethod
    def Equal():
        return SyntaxToken('=')

    @staticmethod
    def Semicolon():
        return SyntaxToken(';')

    @staticmethod
    def Colon():
        return SyntaxToken(':')

    def __init__(self, syntax):
        super().__init__(TokenType.Syntax)
        self.syntax = syntax

    def __str__(self):
        return self.syntax


class OpenDataParser:
    current = 0
    offset = 0
    source = None
    tokens = []
    resolving_member = SyncSeriesEventEmitter()
    resolving_method = SyncSeriesEventEmitter()

    __method__ = dict([
        ['count', '$count'],
        ['min', '$min'],
        ['max', '$max'],
        ['avg', '$avg'],
        ['sum', '$sum'],
        ['year', '$year'],
        ['month', '$month'],
        ['day', '$dayOfMonth'],
        ['hour', '$hour'],
        ['minute', '$minute'],
        ['second', '$second'],
        ['indexof', '$indexOfBytes'],
        ['round', '$round'],
        ['floor', '$floor'],
        ['ceiling', '$ceil'],
        ['tolower', '$toLower'],
        ['trim', '$trim'],
        ['length', '$size'],
        ['add', '$add'],
        ['sub', '$subtract'],
        ['multiply', '$multiply'],
        ['mul', '$multiply'],
        ['div', '$divide'],
        ['concat', '$concat'],
        ['substring', '$substr'],
        ['startswith', '$regexMatch'],
        ['endswith', '$regexMatch'],
        ['contains', '$regexMatch']
    ])

    def __init__(self):

        def resolve_method_regex_match(event):
            if event.method == '$regexMatch':
                input = event.args[0]
                regex = event.args[1]
                if event.original_method is not None and event.original_method == 'startswith':
                    regex = '^' + regex
                elif event.original_method is not None and event.original_method == 'endswith':
                    regex = regex + '$'
                event.expr = {
                    '$regexMatch': {
                        'input': input,
                        'regex': regex
                    }
                }

        self.resolving_method.subscribe(resolve_method_regex_match)

    @property
    def current_token(self):
        return self.tokens[self.offset] if self.offset < len(self.tokens) else None

    @property
    def next_token(self):
        return self.tokens[self.offset + 1] if self.offset < len(self.tokens) - 1 else None

    @property
    def previous_token(self):
        return self.tokens[self.offset - 1] if self.offset > 0 and len(self.tokens) > 0 else None

    def get_operator(self, token):
        if token is not None and token.type == TokenType.Identifier:
            if token.identifier == 'and':
                return TokenOperator.And
            if token.identifier == 'or':
                return TokenOperator.Or
            if token.identifier == 'eq':
                return TokenOperator.Eq
            if token.identifier == 'ne':
                return TokenOperator.Ne
            if token.identifier == 'lt':
                return TokenOperator.Lt
            if token.identifier == 'le':
                return TokenOperator.Le
            if token.identifier == 'gt':
                return TokenOperator.Gt
            if token.identifier == 'ge':
                return TokenOperator.Ge
            if token.identifier == 'in':
                return TokenOperator.In
            if token.identifier == 'nin':
                return TokenOperator.NotIn
            if token.identifier == 'add':
                return TokenOperator.Add
            if token.identifier == 'sub':
                return TokenOperator.Sub
            if token.identifier == 'mul':
                return TokenOperator.Mul
            if token.identifier == 'div':
                return TokenOperator.Div
            if token.identifier == 'mod':
                return TokenOperator.Mod
            if token.identifier == 'not':
                return TokenOperator.Not
        return None

    def parse(self, string):
        self.current = 0
        self.offset = 0
        self.source = string
        # get tokens
        self.tokens = self.to_list()
        # reset offset
        self.offset = 0
        self.current = 0
        # invoke callback
        return self.parse_common()

    def move_next(self):
        self.offset += 1

    def at_end(self):
        return self.offset >= len(self.tokens)

    def at_start(self):
        return self.offset == 0

    def expect(self, token):
        if token is None:
            raise Exception('Expected token.')
        if self.current_token.type == token.type and str(self.current_token) != str(token):
            raise Exception(f'Expected {str(token)}.')
        self.move_next()

    def expect_any(self):
        if self.at_end():
            Exception('Unexpected end.')

    def parse_common(self):
        if len(self.tokens) == 0:
            return None
        left = self.parse_common_item()
        if self.at_end():
            return left
        if SyntaxToken.is_comma(self.current_token) or SyntaxToken.is_paren_close(self.current_token):
            return left
        op = self.get_operator(self.current_token)
        if op is None:
            raise Exception(f'Expected operator at {self.current}.')
        self.move_next()
        if not self.at_end() and TokenOperator.is_logical_operator(op):
            right = self.parse_common()
            if right is None:
                raise Exception('Expected right operand')
            result = dict()
            result[op.value] = [
                left,
                right
            ]
            return result
        else:
            # parse right operand
            right = self.parse_common_item()
            # create expression
            expr = dict()
            expr[op.value] = [
                left,
                right
            ]
            # self an exception where the current token is a logical operator
            op = self.get_operator(self.current_token)
            if not self.at_end() and TokenOperator.is_logical_operator(op):
                # get operator
                op2 = self.get_operator(self.current_token)
                self.move_next()
                # read right operand
                right = self.parse_common()
                if right is None:
                    raise Exception('Expected right operand')
                # and create a complex logical expression
                result = dict()
                result[op2.value] = [
                    expr,
                    right
                ]
                return result
            # if current operator is a comparison operator like $eq, $gt etc
            if not self.at_end() and TokenOperator.is_comparison_operator(op):
                # check if expr is an arithmetic function e.g. Price mul 2.0
                prev_operator = get_first_key(expr)
                if TokenOperator.is_arithmetic_operator(prev_operator):
                    left = dict(expr)
                    self.move_next()
                    right = self.parse_common()
                    if right is None:
                        raise Exception('Expected right operand')
                    result = dict([
                        [
                            op.value,
                            [
                                left,
                                right
                            ]
                        ]
                    ])
                    return result
            else:
                return expr

    def parse_common_item(self):
        if len(self.tokens) == 0:
            return None
        if self.current_token.type == TokenType.Identifier:
            if self.next_token and self.next_token.syntax == SyntaxToken.ParenOpen().syntax and \
                    self.get_operator(self.current_token) is None:
                return self.parse_method_call()
            elif self.get_operator(self.current_token) == TokenOperator.Not:
                raise Exception('Not operator is not yet implemented.')
            else:
                result = self.parse_member()
                self.move_next()
                return result
        elif self.current_token.type == TokenType.Literal:
            value = self.current_token.value
            self.move_next()
            return value
        elif self.current_token.type == TokenType.Syntax:
            if self.current_token.syntax == SyntaxToken.Negative().syntax:
                raise Exception('Negative syntax is not yet implemented.')
            if self.current_token.syntax == SyntaxToken.ParenOpen().syntax:
                self.move_next()
                result = self.parse_common()
                self.expect(SyntaxToken.ParenClose())
                return result

    def parse_member(self):
        if len(self.tokens) == 0:
            return None
        if self.current_token.type != TokenType.Identifier:
            raise Exception('Expected identifier')
        identifier = self.current_token.identifier
        while self.next_token is not None and self.next_token.syntax == SyntaxToken.Slash().syntax:
            self.move_next()
            if self.next_token.type != TokenType.Identifier:
                raise Exception('Expected identifier')
            self.move_next()
            identifier += '/'
            identifier += self.current_token.identifier
        original_member = identifier
        if re.search(r'^\$it\/', identifier):
            identifier = re.sub(r'^\$it\/', '', identifier)
        # resolve member
        member = format_any_field_reference(re.sub(r'\/', '.', identifier))
        event = AnyDict(member=member, original_member=original_member)
        self.resolving_member.emit(event)
        return event.member

    def parse_method_call(self):
        if len(self.tokens) == 0:
            return None
        method = self.current_token.identifier
        self.move_next()
        self.expect(SyntaxToken.ParenOpen())
        if method == 'case':
            branches = []
            default_value = None
            switch_case = self.parse_switch_method_branches(branches, default_value)
            return {
                'branches': {
                    'case': switch_case.branches[0].case,
                    'then': switch_case.branches[0].then
                },
                'default': switch_case.default
            }
        args = self.parse_method_call_args([])

        name = method
        if method in self.__method__:
            name = self.__method__.get(method)
        # emit resolving event
        event = AnyDict(method=format_any_field_reference(name), original_method=method, args=args, expr=None)
        self.resolving_method.emit(event)
        if event.expr is None:
            return dict({
                event.method: event.args
            })
        else:
            return dict(event.expr)

    def parse_switch_method_branches(self, branches, default_value):
        current_token = self.current_token
        if current_token.type == TokenType.Literal and current_token.value is True:
            self.move_next()
            self.expect(SyntaxToken.Colon())
            default_value = self.parse_common()
            self.expect(SyntaxToken.ParenClose())
            return {
                'branches': branches,
                'default': default_value
            }

        case_expr = self.parse_common()
        self.expect(SyntaxToken.Colon())
        then_expr = self.parse_common()
        branch = {
            'case': case_expr,
            'then': then_expr
        }
        branches.append(branch)
        current_token = self.current_token
        if current_token.type == TokenType.Syntax and current_token.syntax == SyntaxToken.ParenClose().syntax:
            self.move_next()
            return {
                'branches': branches,
                'default': None
            }
        self.expect(SyntaxToken.Comma())
        return self.parse_switch_method_branches(branches, default_value)

    def parse_method_call_args(self, method_args):
        self.expect_any()
        if self.current_token.syntax == SyntaxToken.Comma().syntax:
            self.move_next()
            self.expect_any()
            self.parse_method_call_args(method_args)
        elif self.current_token.syntax == SyntaxToken.ParenClose().syntax:
            self.move_next()
        else:
            arg = self.parse_common()
            method_args.append(arg)
            self.parse_method_call_args(method_args)
        return method_args

    def parse_select_sequence(self, source):
        self.source = source
        self.tokens = self.to_list()
        self.offset = 0
        results = []
        while not self.at_end():
            # offset = self.offset
            result = self.parse_common_item()
            if self.current_token and self.current_token.type == TokenType.Identifier \
                    and self.current_token.identifier.lower() == 'as':
                # get next token
                self.move_next()
                # get alias
                if self.current_token is not None and self.current_token.type == TokenType.Identifier:
                    result = {
                        self.current_token.identifier: result
                    }
                    self.move_next()
                else:
                    raise Exception(f'Expected alias at offset {self.offset}')
            else:
                result = dict({
                    result: 1
                })
            results.append(result)
            if not self.at_end() and self.current_token.syntax == SyntaxToken.Comma().syntax:
                self.move_next()
        return results

    def parse_group_by_sequence(self, string):
        return self.parse_select_sequence(string)

    def parse_order_by_sequence(self, string):
        self.source = string
        self.tokens = self.to_list()
        self.offset = self.current = 0
        tokens = self.tokens
        results = []
        if len(tokens) == 0:
            return results
        while not self.at_end():
            # offset = self.offset
            expr = self.parse_common_item()
            direction = 'asc'
            if self.current_token is not None and self.current_token.type == TokenType.Identifier and \
                    (self.current_token.identifier.lower() == 'asc' or self.current_token.identifier.lower() == 'desc'):
                direction = self.current_token.identifier.lower()
                result = {
                    '$expr': expr,
                    'direction': direction
                }
                # go to next token
                self.move_next()
            else:
                result = {
                    '$expr': expr,
                    'direction': direction
                }
            results.append(result)
            if not self.at_end() and self.current_token.syntax == SyntaxToken.Comma().syntax:
                self.move_next()
        return results

    def parse_expand_sequence(self, string):
        self.source = string
        self.tokens = self.to_list()
        self.current = 0
        self.offset = 0
        results = []
        # if expression has only one token
        if len(self.tokens) == 1:
            # and token is an identifier
            if self.current_token.type == TokenType.Identifier:
                # append resul
                results.append({
                    'name': self.current_token.identifier,
                    'source': self.current_token.identifier
                })
                # and return
                return results
            else:
                raise Exception('Invalid expand token. Expected identifier')
        while not self.at_end():
            offset = self.offset
            result = self.parse_expand_item()
            # set source
            result['source'] = self.get_source(offset, self.offset)
            results.append(result)
            if not self.at_end() and self.current_token.syntax == SyntaxToken.Comma().syntax:
                self.move_next()
        return results

    def parse_expand_item_option(self):
        if self.current_token.type == TokenType.Identifier and self.current_token.identifier.indexOf('$') == 0:
            option = self.current_token.identifier
            self.move_next()
            if not self.current_token.is_equal():
                raise Exception('Invalid expand option expression. An option should be followed by an equal sign.')
            self.move_next()
            # move until parenClose e.g. ...$select=id,familyName,giveName)
            # or semicolon ...$select=id,familyName,giveName;
            offset = self.offset
            read = True
            paren_close = 0
            while read:
                if self.current_token.is_paren_open():
                    # wait for parenClose
                    paren_close += 1
                if self.current_token.is_paren_close():
                    paren_close -= 1
                    if paren_close < 0:
                        break
                if self.current_token.is_semicolon():
                    break
                self.move_next()
            result = {}
            # get string
            source = self.get_source(offset, self.offset)
            if option == '$top' or option == '$skip' or option == '$levels':
                value = int(source.strip())
            elif option == '$count':
                value = (source.strip() == 'true')
            else:
                value = source.strip()
            result[option] = value
            return result

    def parse_expand_item(self):
        if self.current_token.type == TokenType.Identifier:
            result = {
                'name': self.current_token.identifier,
                'options': {}
            }
            if self.next_token is None:
                result.update({
                    'source': self.current_token.identifier
                })
                self.move_next()
                # delete options
                return result
            self.move_next()
            if self.current_token.is_paren_open():
                self.move_next()
                # parse expand options
                while self.current_token and self.current_token.is_query_option():
                    option = self.parse_expand_item_option()
                    result['options'].update(option)
                    self.move_next()
            return result
        raise Exception('Invalid syntax. Expected identifier but got ' + self.current_token.type)

    def parse_query_options(self, query_options: dict):
        where = None
        select = None
        order_by = []
        group_by = []
        expand = []
        if '$filter' in query_options:
            where = self.parse(query_options['$filter'])
        if '$select' in query_options:
            select = self.parse_select_sequence(query_options['$select'])
        if '$orderby' in query_options:
            order_by = self.parse_order_by_sequence(query_options['$orderby'])
        if '$groupby' in query_options:
            group_by = self.parse_group_by_sequence(query_options['$groupby'])
        if '$expand' in query_options:
            expand = self.parse_expand_sequence(query_options['$expand'])
        # noqa: W391
        return AnyDict(__where__=where, __select__=select, _order_by__=order_by, __group_by__=group_by, __expand__=expand)  # noqa:E501

    def parse_syntax(self):
        token = None
        c = self.source[self.current]
        if c == '(':
            token = SyntaxToken.ParenOpen()
        elif c == ')':
            token = SyntaxToken.ParenClose()
        elif c == '/':
            token = SyntaxToken.Slash()
        elif c == ',':
            token = SyntaxToken.Comma()
        elif c == '=':
            token = SyntaxToken.Equal()
        elif c == ';':
            token = SyntaxToken.Semicolon()
        elif c == ':':
            token = SyntaxToken.Colon()
        if token is None:
            raise Exception('Unknown syntax token.')
        self.offset = self.current + 1
        return token

    def parse_identifier(self, minus=False):
        current = self.current
        source = self.source
        offset = self.offset
        for c in source[current:]:
            c = source[current]
            if not OpenDataParser.is_identifier_char(c):
                break
            current += 1
        name = source[offset: current].strip()
        last_offset = offset
        offset = current
        token = None
        self.current = current
        self.offset = offset
        if name == 'INF':
            token = LiteralToken.PositiveInfinity()
        elif name == '-INF':
            token = LiteralToken.NegativeInfinity()
        elif name == 'Nan':
            token = LiteralToken.PositiveInfinity()
        elif name == 'true':
            token = LiteralToken.TrueValue()
        elif name == 'false':
            token = LiteralToken.FalseValue()
        elif name == 'null':
            token = LiteralToken.NullValue()
        elif name == '-':
            token = SyntaxToken.Negative()
        elif minus:
            offset = last_offset + 1
            token = SyntaxToken.Negative()
        # move next
        self.current = current
        self.offset = offset
        if token is not None:
            return token
        if offset < len(source) and source[offset] == '\'':
            string_type = None
            if name == 'X':
                string_type = StringType.Binary
            elif name == 'binary':
                string_type = StringType.Binary
            elif name == 'datetime':
                string_type = StringType.DateTime
            elif name == 'guid':
                string_type = StringType.Guid
            elif name == 'time':
                string_type = StringType.Time
            elif name == 'datetimeoffset':
                string_type = StringType.DateTimeOffset
            else:
                string_type = StringType.NoneString
            if string_type != StringType.NoneString and source[offset] == '\'':
                content = self.parse_string()
                return self.parse_special_string(content.value, string_type)
        return IdentifierToken(name)

    def parse_guid(self, value):
        if type(value) is not str:
            raise Exception(f'Invalid argument at {self.offset}.')
        if re.search(OpenDataParser.GuidRegex, value) is None:
            raise Exception(f'Guid format is invalid at {self.offset}..')
        return LiteralToken(value, LiteralToken.LiteralType.Guid)

    def parse_time(self, value):
        if type(value) is None:
            return None
        if re.search(OpenDataParser.DurationRegex, value) is not None:
            return LiteralToken(value, LiteralToken.LiteralType.Duration)
        else:
            raise Exception(f'Duration format is invalid at {self.offset}.')

    def parse_datetime(self, value):
        if value is None:
            return None
        if isdatetime(value):
            return LiteralToken(getdatetime(value), LiteralType.DateTime)
        raise Exception(f'Datetime format is invalid at {self.offset}')

    def parse_datetime_offset(self, value):
        return self.parse_datetime(value)

    def parse_special_string(self, value, string_type):
        if string_type == StringType.Binary:
            return self.parse_binary_string(value)
        if string_type == StringType.DateTime:
            return self.parse_datetime(value)
        if string_type == StringType.DateTimeOffset:
            return self.parse_datetime_offset(value)
        if string_type == StringType.Guid:
            return self.parse_guid(value)
        if string_type == StringType.Time:
            return self.parse_time(value)
        raise Exception('The specified string type is invalid.')

    def parse_binary_string(self, value):
        raise Exception('Not implemented yet.')

    def parse_string(self):
        had_end = False
        current = self.current
        source = self.source
        offset = self.offset
        sb = ''
        while current < len(source):
            current += 1
            c = source[current]
            if c == '\'':
                if current < len(source) - 1 and source[current + 1] == '\'':
                    current += 1
                    sb += '\''
                else:
                    had_end = True
                    break
            else:
                sb += c
        if not had_end:
            raise Exception(f'Unterminated string starting at {offset}')
        self.current = current
        self.offset = current + 1
        return LiteralToken(sb, LiteralType.String)

    def parse_numeric(self):
        _current = self.current
        _source = self.source
        _offset = self.offset
        floating = False
        c = None
        while _current < len(_source):
            c = _source[_current]
            if c == '.':
                if floating:
                    break
                floating = True
            elif not OpenDataParser.is_digit(c):
                break
            _current += 1

        have_exponent = False
        if _current < len(_source):
            c = _source[_current]
            if c == 'E' or c == 'e':
                _current += 1
                if _source[_current] == '-':
                    _current += 1
                exponent_end = None if _current == len(_source) else self.skip_digits(_current)
                if exponent_end is None:
                    raise Exception(f'Expected digits after exponent at {_offset}.')
                _current = exponent_end
                have_exponent = True

                if _current < len(_source):
                    c = _source[_current]
                    if c == 'm' or c == 'M':
                        raise Exception(f'Unexpected exponent for decimal literal at {_offset}.')
                    elif c == 'l' or c == 'L':
                        raise Exception(f'Unexpected exponent for long literal at {_offset}.')

        text = _source[_offset: _current]
        value = None
        literal_type = None
        if _current < len(_source):
            c = _source[_current]
            if c == 'F' or c == 'f':
                value = float(text)
                literal_type = LiteralType.Single
                _current += 1
            elif c == 'D' or c == 'd':
                value = float(text)
                literal_type = LiteralType.Double
                _current += 1
            elif c == 'M' or c == 'm':
                value = float(text)
                literal_type = LiteralType.Decimal
                _current += 1
            elif c == 'L' or c == 'l':
                value = int(text)
                literal_type = LiteralType.Long
                _current += 1
            else:
                if floating or have_exponent:
                    value = float(text)
                    literal_type = LiteralType.Double
                else:
                    value = int(text)
                    literal_type = LiteralType.Int
        else:
            if floating or have_exponent:
                value = float(text)
                literal_type = LiteralType.Double
            else:
                value = int(text)
                literal_type = LiteralType.Int
        _offset = _current
        self.offset = _offset
        self.current = _current
        return LiteralToken(value, literal_type)

    def skip_digits(self, current):
        source = self.source
        if not OpenDataParser.is_digit(source[current]):
            return None
        current += 1
        while current < len(source) and OpenDataParser.is_digit(source[current]):
            current += 1
        return current

    def parse_sign(self):
        self.current += 1
        if OpenDataParser.is_digit(self.source[self.current]):
            return self.parse_numeric()
        else:
            return self.parse_identifier()

    @staticmethod
    def DurationRegex():
        return r'^(-)?P(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)D)?T?(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d*)?)S)?$'

    @staticmethod
    def GuidRegex():
        return r'^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$'

    @staticmethod
    def is_char(c):
        return re.search(r'[A-Za-z]', c) is not None

    @staticmethod
    def is_digit(c):
        return re.search(r'[0-9]', c) is not None

    @staticmethod
    def is_identifier_start(c):
        return re.search(r'[A-Za-z0-9_$]', c) is not None

    @staticmethod
    def is_whitespace(c):
        return re.search(r'\s', c) is not None

    @staticmethod
    def is_identifier_char(c):
        return re.search(r'[A-Za-z_$]', c) is not None

    @staticmethod
    def is_syntax(c):
        return re.search(r'[)(\/,\-=;:]', c[0]) is not None

    def next(self):
        _current = self.current
        _source = self.source
        _offset = self.offset
        if _offset >= len(_source):
            return None
        while _offset < len(_source) and OpenDataParser.is_whitespace(_source[_offset]):
            _offset += 1
        if _offset >= len(_source):
            return None
        _current = _offset
        self.current = _current
        c = _source[_current]
        if c == '-':
            return self.parse_sign()
        if c == '\'':
            return self.parse_string()
        if OpenDataParser.is_syntax(c):
            return self.parse_syntax()
        if OpenDataParser.is_digit(c):
            return self.parse_numeric()
        elif OpenDataParser.is_identifier_start(c):
            return self.parse_identifier(False)
        else:
            raise Exception(f'Unexpected character {c} at offset {_current}.')

    def to_list(self):
        if type(self.source) is not str:
            return []
        self.current = 0
        self.offset = 0
        result = []
        offset = 0
        token = self.next()
        while token is not None:
            token.source = self.source[offset:self.offset]
            offset = self.offset
            result.append(token)
            token = self.next()
        return result

    def get_source(self, start, end):
        source = ''
        for token in self.tokens[start:end]:
            source += token.source
        return source
