import pytest
from unittest import TestCase

from themost_framework.query.query_expression import QueryExpression
from themost_framework.query.query_field import QueryField

def test_create_expr():
    q = QueryExpression('Person')
    TestCase().assertEqual(q.__collection__, {
        '$collection': 'Person'
    })
    q.select('id', 'familyName', {
        'givenName': 1
    }, QueryField('dateCreated'))
    TestCase().assertEqual(q.__select__, [
        { 'id': 1 },
        { 'familyName': 1 },
        { 'givenName': 1 },
        { 'dateCreated': 1 }
    ])
    q.where('familyName').equal('Rees')
    TestCase().assertEqual(q.__query__, {
        '$eq': [
            '$familyName',
            'Rees'
        ]
    })
    