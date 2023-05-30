# noinspection PyUnresolvedReferences
from pycentroid.query.open_data_parser import OpenDataParser, SyntaxToken
from pycentroid.query.query_expression import QueryExpression
from pycentroid.query.sql_formatter import SqlFormatter


def test_get_tokens():
    parser = OpenDataParser()
    parser.source = 'id eq 1'
    tokens = parser.to_list()
    assert len(tokens) > 0


def test_use_parse():
    expr = OpenDataParser().parse('id eq 1')
    assert expr == {
        '$eq': [
            '$id',
            1
        ]
    }


def test_parse_logical_and():
    expr = OpenDataParser().parse('actionStatus eq 1 and owner eq 101')
    assert expr == {
        '$and': [
            {'$eq': ['$actionStatus', 1]},
            {'$eq': ['$owner', 101]},
        ]
    }


def test_parse_logical_or():
    expr = OpenDataParser().parse(
        'actionStatus/alternateName eq \'ActiveActionStatus\' or actionStatus/alternateName eq '
        '\'CompletedActionStatus\'')
    assert expr == {
        '$or': [
            {'$eq': ['$actionStatus.alternateName', 'ActiveActionStatus']},
            {'$eq': ['$actionStatus.alternateName', 'CompletedActionStatus']},
        ]
    }


def test_parse_logical_or_with_paren():
    expr = OpenDataParser().parse(
        '(actionStatus/alternateName eq \'ActiveActionStatus\') or (actionStatus/alternateName eq '
        '\'CompletedActionStatus\')')
    assert expr == {
        '$or': [
            {'$eq': ['$actionStatus.alternateName', 'ActiveActionStatus']},
            {'$eq': ['$actionStatus.alternateName', 'CompletedActionStatus']},
        ]
    }


def test_token_to_string():
    token = SyntaxToken.ParenOpen()
    assert str(token) == '('
    token = SyntaxToken.ParenClose()
    assert str(token) == ')'


def test_parse_arithmetic_add():
    expr = OpenDataParser().parse('price add 2.45 eq 5.00')
    assert expr == {
        '$eq': [
            {
                '$add': [
                    '$price',
                    2.45
                ]
            },
            5.0
        ]
    }


def test_parse_arithmetic_mul():
    expr = OpenDataParser().parse('Price mul 2.0 eq 5.10')
    assert expr == {
        '$eq': [
            {
                '$mul': [
                    '$Price',
                    2.0
                ]
            },
            5.10
        ]
    }
    query = QueryExpression()
    query.__where__ = expr
    sql = SqlFormatter().format_where(query.__where__)
    assert sql == '((Price*2.0)=5.1)'


def test_parse_arithmetic_sub():
    expr = OpenDataParser().parse('Price sub 0.55 eq 2.00')
    assert expr == {
        '$eq': [
            {
                '$sub': [
                    '$Price',
                    0.55
                ]
            },
            2.0
        ]
    }
    query = QueryExpression()
    query.__where__ = expr
    sql = SqlFormatter().format_where(query.__where__)
    assert sql == '((Price-0.55)=2.0)'


def test_parse_arithmetic_div():
    expr = OpenDataParser().parse('Rating div 2 eq 2')
    assert expr == {
        '$eq': [
            {
                '$div': [
                    '$Rating',
                    2
                ]
            },
            2
        ]
    }
    query = QueryExpression()
    query.__where__ = expr
    sql = SqlFormatter().format_where(query.__where__)
    assert sql == '((Rating/2)=2)'


def test_parse_arithmetic_mod():
    expr = OpenDataParser().parse('Rating mod 5 eq 0')
    assert expr == {
        '$eq': [
            {
                '$mod': [
                    '$Rating',
                    5
                ]
            },
            0
        ]
    }
    query = QueryExpression()
    query.__where__ = expr
    sql = SqlFormatter().format_where(query.__where__)
    assert sql == '((Rating%5)=0)'


def test_parse_substring():
    expr = OpenDataParser().parse('substring(CompanyName,1) eq \'lfreds Futterkiste\'')
    assert expr == {
        '$eq': [
            {
                '$substr': [
                    '$CompanyName',
                    1
                ]
            },
            'lfreds Futterkiste'
        ]
    }


def test_parse_startswith():
    expr = OpenDataParser().parse('startswith(CompanyName,\'Alfr\') eq true')
    assert expr == {
        '$eq': [
            {
                '$regexMatch': {
                    'input': '$CompanyName',
                    'regex': '^Alfr'
                }
            },
            True
        ]
    }
