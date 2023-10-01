from ..types import DataContextBase, DataEventArgs, DataObjectState, DataModelBase, DataField
from typing import Optional, Any, List
from ..configuration import DataConfiguration
from ..data_types import DataTypes, DataType
from abc import abstractmethod
from pycentroid.common import DataError
import re


class ValidationError(Exception):

    def __init__(self, code: str, message: str, inner_message: str = None):
        super().__init__()
        self.message = message
        self.code = code
        self.inner_message = inner_message


class DataValidator:
    context: DataContextBase

    def __init__(self, context: DataContextBase = None):
        self.context = context

    @abstractmethod
    def validate(self, value: Any) -> Optional[ValidationError]:
        pass


class AsyncDataValidator:
    context: DataContextBase

    def __init__(self, context: DataContextBase = None):
        self.context = context

    @abstractmethod
    async def validate(self, value: Any) -> Optional[ValidationError]:
        pass


class RequiredValidator(DataValidator):

    message = 'A value is required.'

    def validate(self, val) -> Optional[ValidationError]:
        if val is None:
            return ValidationError(
                code='ERR_REQUIRED', message=self.message
                )
        return None


class PatternValidator(DataValidator):
    pattern: str
    message: str = 'The value seems to be invalid.'

    def __init__(self, pattern: str, message: str = None, context: DataContextBase = None):
        super().__init__(context)
        self.pattern = pattern
        if message is not None:
            self.message = message

    def validate(self, val) -> Optional[ValidationError]:
        if val is None:
            return None
        if re.search(self.pattern, str(val)) is None:
            return ValidationError(
                code='ERR_PATTERN', message=self.message
                )
        return None


class MinLengthValidator(DataValidator):
    min_length: int
    message: str = 'The value is too short.'

    def __init__(self, length: int = 0, context: DataContextBase = None):
        super().__init__(context)
        self.min_length = length

    def validate(self, val) -> Optional[ValidationError]:
        if val is None:
            return None
        if type(val) is str and len(val) < self.min_length:
            return ValidationError(
                code='ERR_MIN_LENGTH', message=self.message
                )
        return None


class MaxLengthValidator(DataValidator):
    min_length: int
    message: str = 'The value is too long.'

    def __init__(self, length: int = 0, context: DataContextBase = None):
        super().__init__(context)
        self.min_length = length

    def validate(self, val) -> Optional[ValidationError]:
        if val is None:
            return None
        if type(val) is str and len(val) > self.min_length:
            return ValidationError(
                code='ERR_MAX_LENGTH', message=self.message
                )
        return None


class MinValueValidator(DataValidator):
    value: Any
    message: str = 'The value should be greater than or equal to {value}.'

    def __init__(self, value: Any, context: DataContextBase = None):
        super().__init__(context)
        self.value = value

    def validate(self, val) -> Optional[ValidationError]:
        if val is None:
            return None
        if val < self.value:
            return ValidationError(
                code='ERR_MIN_VALUE', message=self.message.format(value=self.value)
                )
        return None


class MaxValueValidator(DataValidator):
    value: Any
    message: str = 'The value should be lower than or equal to {value}.'

    def __init__(self, value: Any, context: DataContextBase = None):
        super().__init__(context)
        self.value = value

    def validate(self, val) -> Optional[ValidationError]:
        if val is None:
            return None
        if val > self.value:
            return ValidationError(
                code='ERR_MAX_VALUE', message=self.message.format(value=self.value)
                )
        return None


class RangeValidator(DataValidator):
    min_value: Any
    max_value: Any
    message: str = 'The value should be between {min_value} to {max_value}.'

    def __init__(self, min_value: Any, max_value: Any, context: DataContextBase = None):
        super().__init__(context)
        self.min_value = min_value
        self.max_value = max_value

    def validate(self, val) -> Optional[ValidationError]:
        if val is None:
            return None
        if val < self.min_value or val > self.max_value:
            return ValidationError(
                code='ERR_RANGE_VALUE', message=self.message.format(
                    min_value=self.min_value, max_value=self.max_value
                    ), inner_message=None
                )
        return None


class DataTypeValidator(DataValidator):
    data_type: str
    message: str = 'The value seems to be invalid.'

    def __init__(self, data_type: str, context: DataContextBase = None):
        super().__init__(context)
        self.data_type = data_type

    def validate(self, val) -> Optional[ValidationError]:
        if val is None:
            return None
        configuration: DataConfiguration = self.context.application.services.get(DataConfiguration)
        data_types: DataTypes = configuration.getstrategy(DataTypes)
        typ: DataType = data_types.get(self.data_type)
        if typ is None:
            return None
        if typ.properties is not None and typ.properties.pattern is not None:
            validator = PatternValidator(pattern=typ.properties.pattern, context=self.context)
            if typ.properties.patternMessage is not None:
                validator.message = typ.properties.patternMessage
            validation = validator.validate(val)
            if validation is not None:
                return validation

        if typ.properties is not None and typ.properties.minValue is not None:
            validator = MinValueValidator(typ.properties.minValue, self.context)
            validation = validator.validate(val)
            if validation is not None:
                return validation

        if typ.properties is not None and typ.properties.maxValue is not None:
            validator = MaxValueValidator(typ.properties.maxValue, self.context)
            validation = validator.validate(val)
            if validation is not None:
                return validation

        if typ.properties is not None and typ.properties.minLength is not None:
            validator = MinLengthValidator(typ.properties.minLength, self.context)
            validation = validator.validate(val)
            if validation is not None:
                return validation

        if typ.properties is not None and typ.properties.maxLength is not None:
            validator = MaxLengthValidator(typ.properties.maxLength, self.context)
            validation = validator.validate(val)
            if validation is not None:
                return validation


class ValidationListener:

    @staticmethod
    async def before_save(event: DataEventArgs):
        # get model
        model: DataModelBase = event.model
        # and context
        context: DataContextBase = model.context
        attributes: List[DataField] = []
        if event.state == DataObjectState.INSERT:
            # get attributes for insert
            attributes: List[DataField] = filter(
                lambda x: x.model == model.properties.name and x.primary is False and x.many is False, model.attributes
                )
        elif event.state == DataObjectState.UPDATE:
            # get attributes
            attributes: List[DataField] = filter(
                lambda x: x.model == model.properties.name and x.editable is False and x.primary is False and x.many is False,   # noqa:E501
                model.attributes
                )
        # enumerate attributes
        for attribute in attributes:
            name = attribute.property or attribute.name
            # if target object has attribute
            if hasattr(event.target, name):
                # get value and start validation process by using common validators
                value = getattr(event.target, name)
                # validate required value
                if attribute.nullable is False:
                    validation = RequiredValidator(context).validate(value)
                    if validation is not None:
                        raise DataError(
                            validation.message, validation.inner_message, model.properties.name, name, validation.code
                            )
                if attribute.validation is not None:
                    if attribute.validation.minValue is not None and attribute.validation.maxValue is not None:
                        # validate range
                        validation = RangeValidator(
                            attribute.validation.minValue, attribute.validation.maxValue, context
                            ).validate(value)
                    elif attribute.validation.minValue is not None:
                        # validate min value
                        validation = MinValueValidator(
                            attribute.validation.minValue, context
                            ).validate(value)
                    elif attribute.validation.maxValue is not None:
                        # validate max value
                        validation = MaxValueValidator(
                            attribute.validation.maxValue, context
                            ).validate(value)
                    elif attribute.validation.minLength is not None:
                        # validate min length
                        validation = MinLengthValidator(
                            attribute.validation.minLength, context
                            ).validate(value)
                    elif attribute.validation.maxLength is not None:
                        # validate max length
                        validation = MaxLengthValidator(
                            attribute.validation.maxLength, context
                            ).validate(value)
                    elif attribute.validation.pattern is not None:
                        # validate pattern
                        validation = PatternValidator(
                            attribute.validation.pattern, attribute.validation.patternMessage, context
                            ).validate(value)
                    elif attribute.validation.type is not None:
                        # validate data type
                        validation = DataTypeValidator(
                            attribute.validation.type, context
                            ).validate(value)
                    if validation is not None:
                        raise DataError(
                            validation.message, validation.inner_message, model.properties.name, name, validation.code
                            )
                # validate value based on attribute data type
                validation = DataTypeValidator(attribute.type).validate(value)
                if validation is not None:
                    raise DataError(
                        validation.message, validation.inner_message, model.properties.name, name, validation.code
                        )
            # if value is missing and object is going to be inserted
            elif event.state == DataObjectState.INSERT:
                # raise error for a required value
                raise DataError('A value is required', None, model.properties.name, name, 'ERR_REQUIRED')
