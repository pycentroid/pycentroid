from pycentroid.common import expect, DataError
from pycentroid.query import JOIN_DIRECTION, QueryExpression, QueryField, QueryEntity
from ..queryable import DataQueryable
from ..types import DataModelBase, ExecuteEventArgs, DataFieldAssociationMapping, DataAssociationType


class ExpandListener:

    @staticmethod
    async def after_execute(event: ExecuteEventArgs):
        # get current query
        query: DataQueryable = event.emitter
        if query.__select__ is None:
            return
        if event.results is None:
            return
        if isinstance(event.results, list) and len(event.results) == 0:
            return
        model: DataModelBase = event.model
        # get expand collection already defined in query
        expands = query.__expand__.copy()
        # get auto expand attributes
        attributes = list(filter(lambda x: x.expandable is True, event.model.attributes))
        for attribute in attributes:
            found = next(filter(lambda x: x.__collection__.collection == attribute.name, expands), None)
            if found is None and query.__select__[attribute.name] == 1:
                expands.append(QueryExpression(collection=attribute.name))
        for expand in expands:
            # get attribute from expand collection
            attribute = model.getattr(expand.__collection__.collection)
            expect(attribute).to_be_truthy(
                DataError('Attribute not found', model=model.properties.name, field=expand.__collection__.collection)
            )
            mapping: DataFieldAssociationMapping = model.infermapping(attribute.name)
            expect(mapping).to_be_truthy(
                DataError('Data association cannot be determined', model=model.properties.name, field=attribute.name)
            )
            if mapping.associationType == DataAssociationType.ASSOCIATION:
                # one-to-many
                if attribute.many is True:
                    # get associated model
                    child_model = model.context.model(attribute.type)
                    # get current query in order to use it as sub-query in join expression
                    join_query = query
                    # query children after joining collection with current query
                    children = await child_model.as_queryable().distinct().join(join_query, 'q0').on(
                        QueryExpression().where(
                            QueryField(mapping.childField)
                        ).equal(
                            QueryField(mapping.parentField).from_collection('q0')
                        )
                    ).get_items()
                    if isinstance(event.results, list):
                        # copy children to its parent
                        for result in event.results:
                            # get parent
                            value = getattr(result, mapping.parentField)
                            # filter children
                            items = list(filter(lambda x: getattr(x, mapping.childField) == value, children))
                            # and set property value (an array of items)
                            # todo: copy items before set (needs analysis)
                            setattr(result, attribute.name, items)
                # many-to-one
                else:
                    # get parent model
                    parent_model = model.context.model(attribute.type)
                    # get current query
                    join_query = query
                    # todo: optimize sub-query
                    parents = await parent_model.as_queryable().distinct().join(join_query, 'q0').on(
                        QueryExpression().where(
                            QueryField(mapping.parentField).from_collection(parent_model.properties.get_view())
                        ).equal(
                            QueryField(mapping.childField).from_collection('q0')
                        )
                    ).get_items()
                    if isinstance(event.results, list):
                        for result in event.results:
                            # get parent
                            value = getattr(result, mapping.childField)
                            parent = next(filter(lambda x: getattr(x, mapping.parentField) == value, parents), None)
                            # and set property value
                            # todo: copy items before set (needs analysis)
                            setattr(result, attribute.name, parent)

            elif mapping.associationType == DataAssociationType.JUNCTION:
                # get current query
                join_query = query
                if mapping.parentModel == event.model.properties.name:
                    # get child model
                    child_model = model.context.model(attribute.type)
                    # get child entity
                    child_entity = QueryEntity(child_model.properties.get_view())
                    # get junction entity
                    junction_entity = QueryEntity(mapping.associationAdapter, alias='_' + attribute.name + '_')
                    # get child collection
                    child_collection = child_entity.alias or child_entity.collection
                    # prepare query
                    q = child_model.as_queryable().select(
                        *list(map(lambda x: x.name, filter(lambda x: x.many is not True, child_model.attributes)))
                        )
                    q.__select__.update(
                        QueryField(mapping.associationObjectField).from_collection(junction_entity.alias).asattr('__ref__')  # noqa:E501
                        )
                    # q.__select__.update({
                    #     '__object__' : f'${junction_entity.alias}.{mapping.associationObjectField}'
                    # })
                    # add attribute
                    children = await q.join(
                        junction_entity, direction=JOIN_DIRECTION.LEFT
                    ).on(
                        QueryExpression().where(
                            QueryField(mapping.childField).from_collection(child_collection)
                        ).equal(
                            QueryField(mapping.associationValueField).from_collection(junction_entity.alias)
                            )
                    ).join(join_query, 'q0').on(
                        QueryExpression().where(
                            QueryField(mapping.parentField).from_collection('q0')
                        ).equal(
                            QueryField(mapping.associationObjectField).from_collection(junction_entity.alias)
                            )
                    ).get_items()
                    if isinstance(event.results, list):
                        # copy children to its parent
                        for result in event.results:
                            # get parent
                            value = getattr(result, mapping.parentField)
                            # filter children
                            items = list(filter(lambda x: getattr(x, '__ref__') == value, children))
                            for item in items:
                                delattr(item, '__ref__')
                            # and set property value (an array of items)
                            setattr(result, attribute.name, items)
                elif mapping.childModel == event.model.properties.name:
                    # get child model
                    parent_model = model.context.model(attribute.type)
                    # get child entity
                    parent_entity = QueryEntity(parent_model.properties.get_view())
                    # get junction entity
                    junction_entity = QueryEntity(mapping.associationAdapter, alias='_' + attribute.name + '_')
                    # get child collection
                    parent_collection = parent_entity.alias or parent_entity.collection
                    # prepare query
                    q = parent_model.as_queryable().select(
                        *list(map(lambda x: x.name, filter(lambda x: x.many is not True, parent_model.attributes)))
                        )
                    q.__select__.update(
                        QueryField(mapping.associationValueField).from_collection(junction_entity.alias).asattr('__ref__')  # noqa:E501
                        )
                    # add attribute
                    parents = await q.join(
                        junction_entity, direction=JOIN_DIRECTION.LEFT
                    ).on(
                        QueryExpression().where(
                            QueryField(mapping.parentField).from_collection(parent_collection)
                        ).equal(
                            QueryField(mapping.associationObjectField).from_collection(junction_entity.alias)
                            )
                    ).join(join_query, 'q0').on(
                        QueryExpression().where(
                            QueryField(mapping.childField).from_collection('q0')
                        ).equal(
                            QueryField(mapping.associationValueField).from_collection(junction_entity.alias)
                            )
                    ).get_items()
                    if isinstance(event.results, list):
                        # copy children to its parent
                        for result in event.results:
                            # get parent
                            value = getattr(result, mapping.childField)
                            # filter children
                            items = list(filter(lambda x: hasattr(x, '__ref__') and getattr(x, '__ref__') == value, parents))  # noqa:E501
                            for item in items:
                                delattr(item, '__ref__')
                            # and set property value (an array of items)
                            # todo: copy items before set (needs analysis)
                            setattr(result, attribute.name, items)
