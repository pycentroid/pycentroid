from .types import DataModelBase, UpgradeEventArgs, ExecuteEventArgs,\
    DataField, DataFieldAssociationMapping, DataAssociationType
from pycentroid.query import JOIN_DIRECTION, OpenDataQueryExpression, QueryExpression, QueryField,\
     QueryEntity, ResolvingJoinMemberEvent, ResolvingMemberEvent, trim_field_reference
from pycentroid.common import expect, DataError, is_object_like
from typing import List
from types import SimpleNamespace


class DataQueryable(OpenDataQueryExpression):

    __model__: DataModelBase
    __silent__ = False
    __levels__ = 2

    def __init__(self, model: DataModelBase):
        super().__init__(model.properties.view)
        self.__model__ = model
        self.__collection__ = QueryEntity(model.properties.get_view())
        self.resolving_member.subscribe(self.__on_resolving_member__)
        self.resolving_join_member.subscribe(self.__on_resolving_join_member__)
        # set silent mode
        if hasattr(model, '__silent__'):
            self.__silent__ = getattr(model, '__silent__')
        else:
            self.__silent__ = False
        # set default expand levels
        self.__levels__ = 2

    def __on_resolving_member__(self, event: ResolvingMemberEvent):
        attribute = self.model.getattr(trim_field_reference(event.member))
        expect(attribute).to_be_truthy(
            DataError(message='Attribute not found.', model=self.model.properties.name, field=event.member, code='ERR_ATTR')  # noqa:E501
            )

    def __on_resolving_join_member__(self, event: ResolvingJoinMemberEvent):
        # split member expression e.g. [ 'customer', 'address', 'addressLocality' ]
        members = event.member.split('.')
        # get current model
        model: DataModelBase = self.model
        local_entity = QueryEntity(model.properties.get_view())
        index = 0
        while index < len(members) - 1:
            member = trim_field_reference(members[index])
            mapping: DataFieldAssociationMapping = model.infermapping(member)
            expect(mapping).to_be_truthy(
                Exception('The data association mapping cannot be empty while resolving nested attributes.')
                )
            # noinspection PyUnusedLocal
            join_model: DataModelBase | None = None
            # get local field
            local_field: DataField = model.getattr(member)
            # noinspection PyUnusedLocal
            foreign_field: DataField | None = None
            # get join model and foreign field
            if mapping.parentModel != model.properties.name:
                join_model = self.model.context.model(mapping.parentModel)
                foreign_field = join_model.getattr(mapping.parentField)
            else:
                join_model = self.model.context.model(mapping.childModel)
                foreign_field = join_model.getattr(mapping.childField)

            filter_lookup = lambda x: x['$lookup'] is not None and x['$lookup']['as'] == member
            if next(filter(filter_lookup, self.__lookup__), None) is not None:
                return
            if mapping.many is True:
                self.distinct()
            if mapping.associationType == DataAssociationType.ASSOCIATION:
                # create join entity
                join_entity = QueryEntity(join_model.properties.get_view(), alias=member)
                local_collection = local_entity.alias or local_entity.collection
                # todo: find if lookup has been already defined
                self.join(join_entity, direction=JOIN_DIRECTION.LEFT).on(
                        QueryExpression().where(
                            QueryField(local_field.name).from_collection(local_collection)
                            ).equal(
                                QueryField(foreign_field.name).from_collection(join_entity.alias)
                                )
                    )
            elif mapping.associationType == DataAssociationType.JUNCTION:
                # get junction entity
                junction_entity = QueryEntity(mapping.associationAdapter, alias='_' + member + '_')
                # get join entity
                join_entity = QueryEntity(join_model.properties.get_view(), alias=member)
                local_collection = local_entity.alias or local_entity.collection
                self.join(junction_entity, direction=JOIN_DIRECTION.LEFT).on(
                    QueryExpression().where(
                        QueryField(local_field.name).from_collection(local_collection)
                        ).equal(
                            QueryField(mapping.associationObjectField).from_collection(junction_entity.alias)
                            )
                )
                associated_object = QueryField(mapping.associationObjectField).from_collection(junction_entity.alias)
                associated_value = QueryField(mapping.associationValueField).from_collection(join_entity.alias)
                self.join(join_entity, direction=JOIN_DIRECTION.INNER).on(
                    QueryExpression().where(associated_object).equal(associated_value)
                )
            else:
                raise Exception('Invalid or unsupported association type.')
            # set model attribute (for future use)
            lookup: dict = self.__lookup__[-1]
            if lookup is not None:
                lookup.update({
                    'model': join_model.properties.name
                })
            # set current model
            model = join_model
            index += 1

    def silent(self, value: bool = True):
        self.__silent__ = value
        return self

    def levels(self, value: int):
        self.__levels__ = value if value >= 1 else 1
        return self

    @property
    def model(self) -> DataModelBase:
        return self.__model__

    def find(self, obj: object):
        # get attributes
        attributes = list(filter(lambda x: bool(x.many) is False, self.model.attributes))
        key = self.model.key()
        source = obj
        if isinstance(obj, dict):
            source = SimpleNamespace(**obj)
        # reset query
        self.__where__ = None
        # check if object has primary key
        if hasattr(source, key.name):
            value = getattr(source, key.name)
            if value is not None:
                return self.where(key.name).equal(value)
        # enumerate unique constraints
        constraints = self.model.properties.constraints
        if constraints is not None:
            for constraint in constraints:
                use_constraint = False
                where = {
                    '$and': []
                }
                for field in constraint.fields:
                    # get attribute
                    attribute = self.model.get_attribute(field)
                    if attribute is not None:
                        # get property name
                        prop = attribute.property or attribute.name
                        use_constraint = hasattr(source, prop)
                        if use_constraint is False:
                            break
                        # get value
                        value = getattr(source, key.name)
                        # if value is empty
                        if value is None:
                            use_constraint = False
                            # exit
                            break
                        if value is not None:
                            # append query expression e.g. { '$eq': [ '$category', 'Laptops' ] }
                            where['$and'].append({
                                '$eq': [
                                    '$' + attribute.name,
                                    value
                                ]
                            })
                if use_constraint:
                    self.__where__ = where
                    return self
        # otherwise, build query expression from object properties
        where = {
            '$and': []
        }
        index = 0
        filtered = False
        for attribute in attributes:
            prop = attribute.property or attribute.name
            if hasattr(source, prop):
                # get value
                value = getattr(source, prop)
                # check if value is object-like
                if is_object_like(value):
                    mapping = self.model.infermapping(prop)
                    # mapping should be defined
                    expect(mapping).to_be_truthy(
                        DataError('Data association mapping cannot be determined.', model=self.model.properties.name, field=prop)  # noqa:E501
                        )
                    associated_model = self.model.context.model(attribute.type)
                    expect(associated_model).to_be_truthy(
                        DataError('Associated model cannot be found', model=self.model.properties.name, field=prop)
                    )
                    assoc_entity = QueryEntity(associated_model.properties.get_view())
                    assoc_collection = assoc_entity.alias or assoc_entity.collection
                    # prepare join query of associated object
                    q: QueryExpression = associated_model.find(value).select(
                        QueryField(mapping.parentField).from_collection(assoc_collection)
                    ).take(1)
                    index += 1
                    alias = f'{assoc_collection}{index}'
                    local_entity = QueryEntity(self.model.properties.get_view())
                    local_collection = local_entity.alias or local_entity.collection
                    self.join(q, alias).on(
                        QueryExpression().where(
                            QueryField(mapping.childField).from_collection(local_collection)
                        ).equal(
                            QueryField(mapping.parentField).from_collection(alias)
                        )
                    )
                    filtered = True
                else:
                    # append comparison expression
                    where['$and'].append({
                        '$eq': [
                            '$' + attribute.name,
                            value
                        ]
                    })
                    filtered = True
        if filtered is False:
            return self.where(key.name).equal(None)
        if len(where['$and']) > 0:
            self.__where__ = where
        return self

    async def count(self) -> int:
        key = self.model.key()
        self.take(0).skip(0)
        result = await self.select(
                QueryField(key.name).count().asattr('length')
            ).get_item()
        # noinspection PyUnresolvedReferences
        return result.length

    async def get_item(self) -> object:
        # force take one
        self.take(1).skip(0)
        if self.__select__ is None:
            # get attributes
            attributes = self.__model__.attributes
            self.select(*list(map(lambda x: x.name, filter(lambda x: x.many is not True, attributes))))
        # stage #1 emit before upgrade
        await self.model.before.upgrade.emit(UpgradeEventArgs(model=self.model))
        # stage #2 emit before execute
        event = ExecuteEventArgs(model=self.model, emitter=self)
        await self.model.before.execute.emit(event)
        # execute query
        results = await self.model.context.db.execute(self)
        if len(results) == 0:
            return None
        else:
            event = ExecuteEventArgs(model=self.model, emitter=self, results=results)
            # stage #3 emit after execute
            await self.model.after.execute.emit(event)
            return results[0]

    async def get_items(self) -> List[object]:
        if self.__select__ is None:
            # get attributes
            attributes = self.__model__.attributes
            self.select(*list(map(lambda x: x.name, filter(lambda x: x.many is not True, attributes))))
        # stage #1 emit before upgrade
        await self.model.before.upgrade.emit(UpgradeEventArgs(model=self.model))
        # stage #2 emit before execute
        event = ExecuteEventArgs(model=self.model, emitter=self)
        await self.model.before.execute.emit(event)
        # execute query
        results = await self.model.context.db.execute(self)
        # stage #3 emit after execute
        event = ExecuteEventArgs(model=self.model, emitter=self, results=results)
        await self.model.after.execute.emit(event)
        return results

    async def get_list(self):
        pass
