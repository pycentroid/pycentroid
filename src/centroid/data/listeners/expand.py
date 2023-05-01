from ..types import DataModelBase, ExecuteEventArgs, DataFieldAssociationMapping, DataAssociationType
from ..queryable import DataQueryable
from centroid.query import QueryExpression
from centroid.common import expect, DataError


class ExpandListener:

    async def after_execute(event: ExecuteEventArgs):
        # get current query
        query: DataQueryable = event.emitter
        if query.__select__ is None:
            return
        model: DataModelBase = event.model
        # get expand collection alredy defined in query
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
                if attribute.many is False:
                    # get associated model
                    parent = model.context.model(attribute.type)
                else:
                    # one-to-many
                    child = model.context.model(attribute.type)
                    pass
            elif mapping.associationType == DataAssociationType.JUNCTION:
                pass

