import pytest
from centroid.query import SqlFormatter, OpenDataFormatter, OpenDataQueryExpression
from centroid.client import PseudoSqlParser


def test_parse_simple_select():

    parser = PseudoSqlParser()
    expr: OpenDataQueryExpression = parser.parse('SELECT id,name,description FROM Thing WHERE id=5')
    assert expr is not None
    sql = SqlFormatter().format(expr)
    assert sql == 'SELECT id,name,description FROM Thing WHERE (id=5)'
    params = OpenDataFormatter().format(expr)
    assert params == {
        '$filter': '(id eq 5)',
        '$select': 'id,name,description'
    }

    with pytest.raises(Exception) as e:
        expr: OpenDataQueryExpression = parser.parse('UPDATE Thing SET description=\'test\' WHERE id=50')
    assert type(e.value) is Exception


def test_select_with_alias():

    parser = PseudoSqlParser()
    expr: OpenDataQueryExpression = parser.parse('SELECT id,name as title,description FROM Thing WHERE id=5')
    params = OpenDataFormatter().format(expr)
    assert params == {
        '$filter': '(id eq 5)',
        '$select': 'id,name as title,description'
    }


def test_parse_logical_and():

    parser = PseudoSqlParser()
    expr: OpenDataQueryExpression = parser.parse('SELECT id FROM Action WHERE actionStatus = 1 AND owner = 10')
    assert expr is not None
    sql = SqlFormatter().format(expr)
    assert sql == 'SELECT id FROM Action WHERE ((actionStatus=1) AND (owner=10))'
    params = OpenDataFormatter().format(expr)
    assert params == {
        '$filter': '((actionStatus eq 1) and (owner eq 10))',
        '$select': 'id'
    }


def test_parse_logical_or():

    parser = PseudoSqlParser()
    expr: OpenDataQueryExpression = parser.parse('SELECT id FROM Action WHERE actionStatus = 1 OR actionStatus = 2')
    assert expr is not None
    sql = SqlFormatter().format(expr)
    assert sql == 'SELECT id FROM Action WHERE ((actionStatus=1) OR (actionStatus=2))'
    params = OpenDataFormatter().format(expr)
    assert params == {
        '$filter': '((actionStatus eq 1) or (actionStatus eq 2))',
        '$select': 'id'
    }


def test_parse_unary_expression():

    parser = PseudoSqlParser()
    expr: OpenDataQueryExpression = parser.parse('SELECT id FROM Product WHERE price + 50 > 100')
    params = OpenDataFormatter().format(expr)
    assert params == {
        '$filter': '((price add 50) gt 100)',
        '$select': 'id'
    }

    expr = parser.parse('SELECT id FROM Product WHERE price * 0.25 <= 500')
    params = OpenDataFormatter().format(expr)
    assert params == {
        '$filter': '((price mul 0.25) le 500)',
        '$select': 'id'
    }


    
