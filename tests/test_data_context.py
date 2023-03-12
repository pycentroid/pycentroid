import pytest

from themost_framework.data.data_application import DataApplication

__author__ = "Kyriakos Barbounakis"
__copyright__ = "Kyriakos Barbounakis"
__license__ = "BSD-3-Clause"


def test_create_context():
    app = DataApplication()
    context = app.create_context()
    assert context.application is app

