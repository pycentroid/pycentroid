import unittest

import pytest
from pycentroid.query import SqlFormatter, OpenDataFormatter, OpenDataQueryExpression
from pycentroid.client import PseudoSqlParser


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


def test_parse_function_expression():

    parser = PseudoSqlParser()
    expr: OpenDataQueryExpression = parser.parse('SELECT id,name,FLOOR(price) as price FROM Product WHERE category=\'Laptops\'')  # noqa:E501
    params = OpenDataFormatter().format(expr)
    assert params == {
        '$filter': '(category eq \'Laptops\')',
        '$select': 'id,name,floor(price) as price'
    }


def test_parse_function_expression_with_args():

    parser = PseudoSqlParser()
    expr: OpenDataQueryExpression = parser.parse('SELECT id,name,ROUND(price,2) as price FROM Product WHERE category=\'Laptops\'')  # noqa:E501
    params = OpenDataFormatter().format(expr)
    assert params == {
        '$filter': '(category eq \'Laptops\')',
        '$select': 'id,name,round(price,2) as price'
    }


def test_parse_order_by_statement():

    parser = PseudoSqlParser()
    expr: OpenDataQueryExpression = parser.parse('SELECT id,name,price FROM Product WHERE category=\'Laptops\' ORDER BY price')  # noqa:E501
    params = OpenDataFormatter().format(expr)
    assert params == {
        '$filter': '(category eq \'Laptops\')',
        '$select': 'id,name,price',
        '$orderby': 'price asc'
    }

    expr: OpenDataQueryExpression = parser.parse('SELECT id,name,price FROM Product WHERE category=\'Laptops\' '
                                                 'ORDER BY price,dateReleased DESC')
    params = OpenDataFormatter().format(expr)
    assert params == {
        '$filter': '(category eq \'Laptops\')',
        '$orderby': 'price asc,dateReleased desc',
        '$select': 'id,name,price'
    }


def test_parse_order_by_statement_with_func():

    parser = PseudoSqlParser()
    expr: OpenDataQueryExpression = parser.parse('SELECT id,name,price FROM Product WHERE category=\'Laptops\' '
                                                 'ORDER BY FLOOR(price),dateReleased DESC')
    params = OpenDataFormatter().format(expr)
    assert params == {
        '$filter': '(category eq \'Laptops\')',
        '$select': 'id,name,price',
        '$orderby': 'floor(price) asc,dateReleased desc'
    }


def test_parse_aggregate_func():
    parser = PseudoSqlParser()
    expr: OpenDataQueryExpression = parser.parse('SELECT MAX(price) as price FROM Product WHERE category=\'Desktops\'')
    params = OpenDataFormatter().format(expr)
    assert params == {
        '$filter': '(category eq \'Desktops\')',
        '$select': 'max(price) as price'
    }

    expr: OpenDataQueryExpression = parser.parse('SELECT COUNT(id) as total FROM Product WHERE category=\'Desktops\'')
    params = OpenDataFormatter().format(expr)
    assert params == {
        '$filter': '(category eq \'Desktops\')',
        '$select': 'count(id) as total'
    }


def test_parse_group_by_statement():

    parser = PseudoSqlParser()
    expr: OpenDataQueryExpression = parser.parse('SELECT category,COUNT(id) as total FROM Product GROUP BY category')
    params = OpenDataFormatter().format(expr)
    assert params == {
        '$select': 'category,count(id) as total',
        '$groupby': 'category'
    }


def test_parse_where_with_like():

    parser = PseudoSqlParser()
    expr: OpenDataQueryExpression = parser.parse('SELECT * FROM Product WHERE name LIKE \'%Apple%\'')
    params = OpenDataFormatter().format(expr)
    assert params == {
        '$select': '*',
        '$filter': '(contains(name,\'Apple\') eq true)'
    }

    expr: OpenDataQueryExpression = parser.parse('SELECT * FROM Product WHERE name LIKE \'Lenovo%\'')
    params = OpenDataFormatter().format(expr)
    assert params == {
        '$select': '*',
        '$filter': '(startswith(name,\'Lenovo\') eq true)'
    }


def test_parse_date_function():

    parser = PseudoSqlParser()
    expr: OpenDataQueryExpression = parser.parse('SELECT * FROM Product WHERE YEAR(Product.releaseDate) = 2019'
                                                 ' AND MONTH(Product.releaseDate) = 4'
                                                 ' AND DAY(Product.releaseDate) = 24')
    params = OpenDataFormatter().format(expr)
    assert params == {
        '$select': '*',
        '$filter': '(((year(releaseDate) eq 2019) and (month(releaseDate) eq 4)) and (day(releaseDate) eq 24))'
    }


def test_parse_limit_select():

    parser = PseudoSqlParser()
    expr: OpenDataQueryExpression = parser.parse('SELECT * FROM Product LIMIT 10')
    params = OpenDataFormatter().format(expr)
    assert params == {
        '$select': '*',
        '$count': True,
        '$top': 10
    }

    expr: OpenDataQueryExpression = parser.parse('SELECT * FROM Product LIMIT 20, 10')
    params = OpenDataFormatter().format(expr)
    assert params == {
        '$select': '*',
        '$count': True,
        '$top': 10,
        '$skip': 20
    }


def test_parse_between():

    parser = PseudoSqlParser()
    expr: OpenDataQueryExpression = parser.parse('SELECT name,price FROM Product WHERE price BETWEEN 500 AND 1000')
    params = OpenDataFormatter().format(expr)
    assert params == {
        '$select': 'name,price',
        '$filter': '((price ge 500) and (price le 1000))'
    }


def test_parse_select_with_between_expr():

    parser = PseudoSqlParser()
    expr: OpenDataQueryExpression = parser.parse("""
    SELECT name, price
    FROM Product
    WHERE year(releaseDate) = 2019
    AND month(releaseDate) BETWEEN 1 AND 6
    ORDER BY price ASC
    LIMIT 5
    """)
    params = OpenDataFormatter().format(expr)
    assert params == {
        '$select': 'name,price',
        '$filter': '((year(releaseDate) eq 2019) and ((month(releaseDate) ge 1) and (month(releaseDate) le 6)))',
        '$orderby': 'price asc',
        '$count': True,
        '$top': 5
    }


def test_parse_select1():

    parser = PseudoSqlParser()
    expr: OpenDataQueryExpression = parser.parse("SELECT AVG(grade) FROM StudentGrade WHERE name = 'Artificial Intelligence'")  # noqa:E501
    params = OpenDataFormatter().format(expr)
    assert params is not None
    assert params == {
        '$select': 'avg(grade) as field1',
        '$filter': '(name eq \'Artificial Intelligence\')'
    }
