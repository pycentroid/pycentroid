import pytest
from centroid.query import SqlParser, QueryExpression, SqlFormatter


def test_parse_simple_select():

    parser = SqlParser()
    expr: QueryExpression = parser.parse('SELECT id,name,description FROM Thing WHERE id=5')
    assert expr is not None
    sql = SqlFormatter().format(expr)
    assert sql == 'SELECT id,name,description FROM Thing WHERE (id=5)'

    with pytest.raises(Exception) as e:
        expr: QueryExpression = parser.parse('UPDATE Thing SET description=\'test\' WHERE id=50')
    assert type(e.value) is Exception


def test_parse_logical_and():

    parser = SqlParser()
    expr: QueryExpression = parser.parse('SELECT id FROM Action WHERE actionStatus = 1 AND owner = 10')
    assert expr is not None
    sql = SqlFormatter().format(expr)
    assert sql == 'SELECT id FROM Action WHERE ((actionStatus=1) AND (owner=10))'


def test_parse_logical_or():

    parser = SqlParser()
    expr: QueryExpression = parser.parse('SELECT id FROM Action WHERE actionStatus = 1 OR actionStatus = 2')
    assert expr is not None
    sql = SqlFormatter().format(expr)
    assert sql == 'SELECT id FROM Action WHERE ((actionStatus=1) OR (actionStatus=2))'


    
