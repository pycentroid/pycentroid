import pytest
from os.path import abspath, join, dirname
from centroid.data.application import DataApplication
from centroid.data.context import DataContext
from centroid.data.listeners import MinLengthValidator, MaxLengthValidator,\
    MinValueValidator, MaxValueValidator, RangeValidator, DataTypeValidator

APP_PATH = abspath(join(dirname(__file__), '..'))


@pytest.fixture()
def context() -> DataContext:
    app = DataApplication(cwd=APP_PATH)
    return app.create_context()


def test_min_length(context: DataContext):
    validator = MinLengthValidator(length=4, context=context)
    assert validator.validate(None) is None
    assert validator.validate('Hello') is None
    validation = validator.validate('Yes')
    assert validation.message == 'The value is too short.'


def test_max_length(context: DataContext):
    validator = MaxLengthValidator(length=12, context=context)
    assert validator.validate(None) is None
    assert validator.validate('Hello World!') is None
    validation = validator.validate('Hello World!!!')
    assert validation.message == 'The value is too long.'


def test_min_value(context: DataContext):
    validator = MinValueValidator(value=24, context=context)
    assert validator.validate(None) is None
    assert validator.validate(32) is None
    validation = validator.validate(20)
    assert validation.message == 'The value should be greater than or equal to 24.'


def test_max_value(context: DataContext):
    validator = MaxValueValidator(value=24, context=context)
    assert validator.validate(None) is None
    assert validator.validate(20) is None
    validation = validator.validate(32)
    assert validation.message == 'The value should be lower than or equal to 24.'


def test_range_value(context: DataContext):
    validator = RangeValidator(min_value=24, max_value=32, context=context)
    assert validator.validate(None) is None
    assert validator.validate(25) is None
    assert validator.validate(32) is None
    validation = validator.validate(3)
    assert validation.message == 'The value should be between 24 to 32.'


def test_type_validator(context: DataContext):
    validator = DataTypeValidator(data_type='PositiveInteger', context=context)
    assert validator.validate(None) is None
    assert validator.validate(4) is None
    validation = validator.validate(-3)
    assert validation.message == 'The value should be an integer greater than zero.'

    validator = DataTypeValidator(data_type='Float', context=context)
    assert validator.validate(None) is None
    assert validator.validate(4.25) is None
    validation = validator.validate('Hello')
    assert validation.message == 'The value seems to be invalid.'
