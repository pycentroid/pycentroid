# pycentroid.client

A client library of [@themost-framework](https://github.com/themost-framework/) application for python

[pycentroid](https://github.com/pycentroid/pycentroid) implements a full-featured [OData v4](https://www.odata.org/documentation/) client for connecting with any service which supports OData protocol e.g. [@themost-framework](https://github.com/themost-framework/express) OData-first api servers.

e.g. get name and price of products with category equals to 'Laptops'

```python
from pycentroid.query import select


async def get_items(context):
    items = await context.model('Products').as_queryable().select(
        lambda x: select(name=x.name, price=x.price)
    ).where(
        lambda x: x.category == 'Laptops'
    ).get_items()
    return items;

```
> `/Products?$select=name,price&$filter=(category+eq+'Laptops')`
