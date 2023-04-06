import pytest
from unittest import TestCase
from themost_framework.query.open_data_parser import OpenDataParser, SyntaxToken

def test_get_tokens():
    parser = OpenDataParser()
    parser.source = 'id eq 1'
    tokens = parser.to_list()
    TestCase().assertGreater(len(tokens), 0)

def test_use_parse():
    expr = OpenDataParser().parse('id eq 1')
    TestCase().assertEqual(expr, {
        '$eq': [
            '$id',
            1
        ]
    })

def test_parse_logical_and():
    expr = OpenDataParser().parse('actionStatus eq 1 and owner eq 101')
    TestCase().assertEqual(expr, {
        '$and': [
            { '$eq': [ '$actionStatus',  1 ] },
            { '$eq': [ '$owner',  101 ] },
        ]
    })

def test_parse_logical_or():
    expr = OpenDataParser().parse('actionStatus/alternateName eq \'ActiveActionStatus\' or actionStatus/alternateName eq \'CompletedActionStatus\'')
    TestCase().assertEqual(expr, {
        '$or': [
            { '$eq': [ '$actionStatus.alternateName',  'ActiveActionStatus' ] },
            { '$eq': [ '$actionStatus.alternateName',  'CompletedActionStatus' ] },
        ]
    })

def test_parse_logical_or_with_paren():
    expr = OpenDataParser().parse('(actionStatus/alternateName eq \'ActiveActionStatus\') or (actionStatus/alternateName eq \'CompletedActionStatus\')')
    TestCase().assertEqual(expr, {
        '$or': [
            { '$eq': [ '$actionStatus.alternateName',  'ActiveActionStatus' ] },
            { '$eq': [ '$actionStatus.alternateName',  'CompletedActionStatus' ] },
        ]
    })

def test_token_to_string():
    token = SyntaxToken.ParenOpen()
    TestCase().assertEqual(str(token),'(')
    token = SyntaxToken.ParenClose()
    TestCase().assertEqual(str(token),')')