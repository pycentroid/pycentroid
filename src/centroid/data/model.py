from typing import List
from .types import DataContextBase, DataModelBase, DataField, DataModelProperties,\
    UpgradeEventArgs, DataEventArgs, DataObjectState, ExecuteEventArgs, DataFieldAssociationMapping
from .queryable import DataQueryable
from .configuration import DataConfiguration
from .data_types import DataTypes
from pydash import assign
from centroid.query import QueryExpression, QueryEntity
from centroid.common import DataError
from .upgrade import DataModelUpgrade
import inflect

pluralize = inflect.engine()


class DataModelAttribute(DataField):
    model: str


class DataModel(DataModelBase):

    __silent__ = False
    __attributes__: List[DataModelAttribute]

    def __init__(self, context: DataContextBase = None, properties: DataModelProperties = None, **kwargs):
        super().__init__(context, properties)
        self.before.upgrade.subscribe(DataModelUpgrade.before)
        self.after.upgrade.subscribe(DataModelUpgrade.after)

    def silent(self, value: bool = True):
        self.__silent__ = value
        return self

    def base(self) -> DataModelBase:
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
        else:
            implements = self.context.model(self.properties.implements) if self.properties.implements is not None else None
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
            # # check many attribute
            # if attr.many is None and pluralize.singular_noun(attr.name) is not False:
            #     attr.many = True
            if attr.many is None and attr.multiplicity == 'ZeroOrOne':
                attr.many = True
            attributes.append(attr)
        self.__attributes__ = attributes
        return self.__attributes__
    
    def as_queryable(self):
        return DataQueryable(self)

    def get_super_types(self) -> List[str]:
        results = []
        model = self.base()
        while model is not None:
            results.append(model.properties.name)
            model = model.base()
        return results

    def infermapping(self, name: str) -> DataFieldAssociationMapping:
        attribute: DataModelAttribute = self.get_attribute(name)
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
            fields = list(filter(lambda x: x.type == self.properties.name, model.attributes))
            if len(fields) == 0:
                # set many-to-many association (between models)
                mapping = assign({
                    'associationType': 'junction',
                    'associationAdapter': f'{self.properties.name}{name.capitalize()}',
                    'associationObjectField': 'object',
                    'associationValueField': 'value',
                    'childModel': model.properties.name,
                    'childField': model.key().name,
                    'cascade': 'delete'
                }, original_mapping, {
                    'parentModel': self.properties.name,
                    'parentField': self.key().name,
                })
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

    async def upsert(self, o: object or List[object]):
        pass

    async def remove(self, o: object or List[object]):
        pass

    def __pre_insert__(self, obj: object) -> dict:
        result = {}
        attributes = list(filter(lambda x: bool(x.many) is False and x.model == self.properties.name, self.attributes))
        # get primary key
        key = self.key()
        # if primary key is not auto increment
        if key.type != 'Counter':
            # add attribte
            attributes.insert(0, key)
        # enumerate attributes
        for attribute in attributes:
            property = attribute.property or attribute.name
            if hasattr(obj, property):
                result[attribute.name] = obj[property]
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
            property = attribute.property or attribute.name
            if hasattr(obj, property):
                result[attribute.name] = obj[property]
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
            # emit after execute event
            await self.after.execute.emit(execute_event)
            # emit after save event
            await self.before.after.emit(event)

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
            id = source[primary]
            if id is None:
                raise DataError('Primary key cannot be empty during update', None, self.properties.name, primary)
            query = QueryExpression().update(collection).set(source).where(primary).equal(id)
            execute_event = ExecuteEventArgs(model=self, emitter=query)
            # emit before execute event
            await self.before.execute.emit(execute_event)
            # execute insert
            await self.context.db.execute(query)
            # emit after execute event
            await self.after.execute.emit(execute_event)
            # emit after save event
            await self.before.after.emit(event)

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
