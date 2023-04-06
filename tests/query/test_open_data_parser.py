import pytest
from unittest import TestCase
from themost_framework.query.open_data_parser import OpenDataParser, SyntaxToken

def test_parse_string():
    tokens = OpenDataParser().parse('id eq 1').to_list()
    TestCase().assertGreater(len(tokens), 0)

def test_token_to_string():
    token = SyntaxToken.ParenOpen()
    TestCase().assertEqual(str(token),'(')
    token = SyntaxToken.ParenClose()
    TestCase().assertEqual(str(token),')')