from typing import List
from .types import DataContextBase, DataModelBase, DataField, DataModelProperties,\
    UpgradeEventArgs, DataEventArgs, DataObjectState, ExecuteEventArgs, DataFieldAssociationMapping
from .queryable import DataQueryable
from .configuration import DataConfiguration
from .data_types import DataTypes
from pydash import assign
from pycentroid.query import QueryExpression, QueryEntity
from pycentroid.common import DataError, expect, AnyDict
from .upgrade import DataModelUpgrade
from .listeners.expand import ExpandListener
from .listeners.validator import ValidationListener
import inflect
import re

pluralize = inflect.engine()


def is_plural(text: str) -> bool:
    # an exception for inflect package (word ends with double 's')
    if text.endswith('ss'):
        return False
    # if word is singular
    if pluralize.singular_noun(text) is False:
        # return false
        return False
    # split camel case string e.g. orderStatus
    if re.search(r'[A-Z]', text) is not None:
        matches = re.finditer('.+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)', text)
        segments = [m.group(0).lower() for m in matches]
        # and check the last segment
        return pluralize.singular_noun(segments[-1]) is not False
    # split snake case string e.g. order_status
    # trim underscore from the beginning or the end of txt
    s = re.sub('(^_+)|(_+$)', '', text)
    # check if word is snake case
    if re.search(r'_', s) is not None:
        # split and check the last segment only
        return pluralize.singular_noun(s.split('_')[-1]) is not False
    p = pluralize.plural(pluralize.singular_noun(text))
    return p == text


class DataModelAttribute(DataField):
    model: str


class DataModel(DataModelBase):

    __silent__ = False
    __attributes__: List[DataModelAttribute]

    def __init__(self, context: DataContextBase = None, properties: DataModelProperties = None, **kwargs):
        super().__init__(context, properties)
        self.before.upgrade.subscribe(DataModelUpgrade.before)
        self.after.upgrade.subscribe(DataModelUpgrade.after)
        # append execute listeners
        self.after.execute.subscribe(ExpandListener.after_execute)
        self.before.save.subscribe(ValidationListener.before_save)

    def silent(self, value: bool = True):
        self.__silent__ = value
        return self

    def base(self) -> DataModelBase | None:
        if self.properties is not None and self.properties.inherits is not None:
            return self.context.model(self.properties.inherits)
        return None

    @property
    def attributes(self) -> List[DataModelAttribute]:
        if hasattr(self, '__attributes__'):
            return self.__attributes__
        base_model = self.base()
        attributes: List[DataModelAttribute] = []
        if base_model is not None:
            attributes = base_model.attributes
            # get base model key
            key = base_model.key()
            # if it's an auto increment identity
            if key.type == 'Counter':
                # revert type to int
                attr = next(filter(lambda x: x.name == key.name, attributes), None)
                attr.type = 'Integer'
        else:
            implements = self.context.model(self.properties.implements)if self.properties.implements is not None else None  # noqa:E501
            if implements is not None:
                attributes = list(map(lambda x: assign(DataModelAttribute(**x), {
                    'model': self.properties.name
                }), implements.attributes))
        for field in self.properties.fields:
            found = next(filter(lambda x: x.name == field.name, attributes), None)
            if found is None:
                attr = DataModelAttribute(**field, model=self.properties.name)
            else:
                clone = assign(found, field)
                attr = DataModelAttribute(**clone)
                attributes.remove(found)
            # # check many attribute
            if attr.many is None and is_plural(attr.name):
                attr.many = True
            if attr.many is None and attr.multiplicity == 'ZeroOrOne':
                attr.many = True
            attributes.append(attr)
        self.__attributes__ = attributes
        return self.__attributes__

    def as_queryable(self):
        return DataQueryable(self)

    def where(self, *args, **kwargs):
        return DataQueryable(self).where(*args, **kwargs)

    def find(self, obj: object):
        return DataQueryable(self).find(obj)

    def get_super_types(self) -> List[str]:
        results = []
        model = self.base()
        while model is not None:
            results.append(model.properties.name)
            model = model.base()
        return results

    def infermapping(self, name: str) -> DataFieldAssociationMapping | None:
        attribute: DataModelAttribute = self.getattr(name)
        expect(attribute).to_be_truthy(
            DataError(message='Attribute not found.', model=self.properties.name, field=name, code='ERR_ATTR')
        )
        if attribute is None:
            return None
        data_types: DataTypes = self.context.application.services.get(DataConfiguration).getstrategy(DataTypes)
        # get many flag
        many = attribute.many is not None and attribute.many
        # get name
        name: str = attribute.name
        original_mapping = attribute.mapping or {}
        # if attribute type is primitive e.g. Integer, Text, Boolean etc
        if data_types.has(attribute.type):
            # # set name to its plural e.g. tag => tags
            # if many is True and pluralize.singular_noun(name) is False:
            #     name = pluralize.plural(name)
            # if many is false
            if many is False:
                # exit
                return None
            # set many-to-many association (for a primitive types)
            mapping = assign({
                'associationType': 'junction',
                'associationAdapter': f'{self.properties.name}{name.capitalize()}',
                'associationObjectField': 'object',
                'associationValueField': 'value',
                'childModel': None,
                'childField': None,
                'cascade': 'delete'
            }, original_mapping, {
                'parentModel': self.properties.name,
                'parentField': self.key().name
            })
            return DataFieldAssociationMapping(**mapping)
        # get associated model
        model: DataModel = self.context.model(attribute.type)
        if model is not None:
            if many is False:
                # set many-to-one association
                mapping = assign({
                    'associationType': 'association',
                    'parentModel': model.properties.name,
                    'parentField': model.key().name,
                    'cascade': 'none'
                }, original_mapping, {
                    'childModel': self.properties.name,
                    'childField': attribute.name,
                })
                return DataFieldAssociationMapping(**mapping)

            # find fields with type equal to current model
            fields = list(filter(lambda x: x.type == self.properties.name and x.many is False, model.attributes))
            if len(fields) == 0:
                # set many-to-many association (between models)
                mapping = assign({
                    'associationType': 'junction',
                    'associationAdapter': f'{self.properties.name}{name.capitalize()}',
                    'associationObjectField': 'object',
                    'associationValueField': 'value',
                    'childModel': model.properties.name,
                    'childField': model.key().name,
                    'cascade': 'delete',
                    'parentModel': self.properties.name,
                    'parentField': self.key().name
                }, original_mapping)
                # get super types
                super_types = self.get_super_types()
                if 'childModel' in original_mapping:
                    child_model_type = getattr(original_mapping, 'childModel')
                    # if the defined child model is inherited by the current model
                    if child_model_type in super_types:
                        # update association mapping and set child model to current model
                        mapping['childModel'] = self.properties.name
                elif 'parentModel' in original_mapping:
                    parent_model_type = getattr(original_mapping, 'parentModel')
                    # if the defined parent model is inherited by the current model
                    if parent_model_type in super_types:
                        # update association mapping and set parent model to current model
                        mapping['parentModel'] = self.properties.name

                return DataFieldAssociationMapping(**mapping)
            elif len(fields) == 1:
                # set one-to-many association
                mapping = assign({
                    'associationType': 'association',
                    'childModel': model.properties.name,
                    'childField': fields[0].name,
                    'cascade': 'none'
                }, original_mapping, {
                    'parentModel': self.properties.name,
                    'parentField': self.key().name,
                })
                return DataFieldAssociationMapping(**mapping)
            elif attribute.mapping is not None:
                mapping = assign({
                    'associationType': attribute.mapping.associationType,
                    'childModel': attribute.mapping.childModel,
                    'childField': attribute.mapping.childField,
                    'cascade': 'none'
                }, original_mapping, {
                    'parentModel': self.properties.name,
                    'parentField': self.key().name
                })
                return DataFieldAssociationMapping(**mapping)
            else:
                raise DataError('Association mapping cannot be determined due to multiple associations')
        return None

    async def migrate(self):
        async def execute():
            await self.before.upgrade.emit(UpgradeEventArgs(model=self))
        await self.context.execute_in_transaction(execute)

    async def inferstate(self, item) -> DataObjectState:
        exists = await DataQueryable(self).find(item).take(1).count()
        return DataObjectState.INSERT if exists == 0 else DataObjectState.UPDATE

    async def upsert(self, o: object or List[object]):
        pass

    async def remove(self, o: object or List[object]):
        # try to find object
        if isinstance(o, list):

            async def remove_many():
                results = AnyDict(value=[])
                for item in o:
                    result = await self.remove(item)
                    results.value.append(result)

            # remove items
            await self.context.db.execute_in_transaction(remove_many)
        else:

            async def remove_one():
                # infer state
                item = await self.silent().find(o)
                if item is not None:
                    # emit before remove event
                    event = DataEventArgs(model=self, state=DataObjectState.DELETE, target=item)
                    await self.before.remove.emit(event)
                    # get collection
                    collection = QueryEntity(self.properties.get_source())
                    # get primary key
                    key = self.key()
                    value = getattr(o, key.name)
                    # prepare delete query
                    query = QueryExpression().delete(collection).where(key.name).equal(value)
                    # raise before execute event
                    execute_event = ExecuteEventArgs(model=self, emitter=query)
                    # emit before execute event
                    await self.before.execute.emit(execute_event)
                    # execute
                    await self.context.db.execute(query)
                    # raise after execute event
                    await self.after.execute.emit(execute_event)
                    # emit after remove query
                    await self.before.remove.emit(event)
                    # get base model
                    base = self.base()
                    if base is not None:
                        # and try to remove base item
                        await base.remove(o)
                    return AnyDict(value={
                        key.name: value
                    })

            # remove items
            await self.context.db.execute_in_transaction(remove_one)

    def __pre_insert__(self, obj: object) -> dict:
        result = {}
        attributes = list(filter(lambda x: bool(x.many) is False and x.model == self.properties.name, self.attributes))
        # get primary key
        key = self.key()
        # if primary key is not auto increment
        if key.type != 'Counter':
            # add attribute
            attributes.insert(0, key)
        # enumerate attributes
        for attribute in attributes:
            # get property name
            prop = attribute.property or attribute.name
            if hasattr(obj, prop):
                # get mapping
                mapping = self.infermapping(attribute.name)
                if mapping is None:
                    result[attribute.name] = getattr(obj, prop)
                else:
                    # todo: resolve value
                    result[attribute.name] = getattr(obj, prop)
        return result

    def __pre_update__(self, obj: object) -> dict:
        result = {}
        attributes = list(
            # noqa: E501
            filter(
                lambda x: bool(x.primary) is False and bool(x.many) is False and x.model == self.properties.name,
                self.attributes
                )
            )
        # enumerate attributes
        for attribute in attributes:
            name = attribute.property or attribute.name
            if hasattr(obj, name):
                result[attribute.name] = getattr(obj, name)
        return result

    async def __insert__(self, o: object):

        async def execute():
            # ensure that current model has been upgraded
            await self.migrate()
            # get base model
            base = self.base()
            if base is not None:
                await base.insert(o)
            # emit before save event
            event = DataEventArgs(model=self, state=DataObjectState.INSERT, target=o)
            await self.before.save.emit(event)
            # create query
            collection = QueryEntity(self.properties.get_source())
            # get object for insert
            data = self.__pre_insert__(o)
            query = QueryExpression().insert(data).into(collection)
            execute_event = ExecuteEventArgs(model=self, emitter=query)
            # emit before execute event
            await self.before.execute.emit(execute_event)
            # execute insert
            await self.context.db.execute(query)
            key = self.key()
            if key.type == 'Counter':
                last_insert_id = await self.context.db.last_identity()
                if last_insert_id is not None:
                    setattr(o, key.name, last_insert_id)
            # emit after execute event
            await self.after.execute.emit(execute_event)
            # emit after save event
            await self.after.save.emit(event)

        await self.context.execute_in_transaction(execute)

    async def __update__(self, o: object):

        async def execute():
            # ensure that current model has been upgraded
            await self.migrate()
            # get base model
            base = self.base()
            if base is not None:
                await base.update(o)
            # emit before save event
            event = DataEventArgs(model=self, state=DataObjectState.UPDATE, target=o)
            await self.before.save.emit(event)
            # create query
            collection = QueryEntity(self.properties.get_source())
            # get object for insert
            source = self.__pre_update__(o)
            primary = self.key().name
            identifier = source[primary]
            if identifier is None:
                raise DataError('Primary key cannot be empty during update', None, self.properties.name, primary)
            query = QueryExpression().update(collection).set(source).where(primary).equal(identifier)
            execute_event = ExecuteEventArgs(model=self, emitter=query)
            # emit before execute event
            await self.before.execute.emit(execute_event)
            # execute insert
            await self.context.db.execute(query)
            # emit after execute event
            await self.after.execute.emit(execute_event)
            # emit after save event
            await self.after.save.emit(event)

        await self.context.execute_in_transaction(execute)

    async def insert(self, o: object or List[object]):
        async def execute():
            if isinstance(o, list):
                for item in o:
                    await self.__insert__(item)
            else:
                await self.__insert__(o)
        await self.context.execute_in_transaction(execute)

    async def save(self, o: object or List[object]):
        pass

    async def update(self, o: object or List[object]):
        async def execute():
            if isinstance(o, list):
                for item in o:
                    await self.__update__(item)
            else:
                await self.__update__(o)
        await self.context.execute_in_transaction(execute)
