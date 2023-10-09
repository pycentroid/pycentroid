# pycentroid.client

A client library of [@themost-framework](https://github.com/themost-framework/) application for python

[pycentroid](https://github.com/pycentroid/pycentroid) implements a full-featured [OData v4](https://www.odata.org/documentation/) client for connecting with any service which supports OData protocol.

e.g. get name and price of products with category equals to 'Laptops'

```python
from pycentroid.query import select


async def get_items(context):
    items = await context.model('Products').as_queryable().select(
        lambda x: select(name=x.name, price=x.price)
    ).where(
        lambda x: x.category == 'Laptops'
    ).get_items()
    return items

```
> `/Products?$select=name,price&$filter=(category+eq+'Laptops')`

## Use client context

Create an instance of `ClientDataContext` class:

```python
from pycentroid.client import ClientDataContext, ClientContextOptions

context = ClientDataContext(ClientContextOptions('http://localhost:3000/api/'))
```

and start getting or pushing data

```python
def get_items(context):
    context.model('Products').as_queryable().order_by(
        lambda x: x.price
    ).take(25).get_items()
```

`ClientDataContext.model(entity_set).as_queryable()` returns an instance of `ClientDataQueryable` class for further processing:

- [select attributes](#select)
- [sort results](#sorting)
- [use paging](#paging)
- [grouping results](#grouping) 

## System query options

`pycentroid` introduces the usage of lambda functions while querying or selecting data. Query lambda expressions might be also parameterized by passing arguments:

```python
async def get_items(context):
    items = await context.model('Orders').as_queryable().where(
        where=lambda x, order_status: x.orderStatus.alternateName == order_status, order_status='OrderPickup'
    ).take(10).get_items()
    return items
```
> `/Orders?$filter=(orderStatus/alternateName+eq+'OrderPickup')&$count=True&$top=10`

### Select
Use `$select` query option to decorate response and get only the specified attributes or nested attributes

```python
from pycentroid.query import select


async def select_items(context):
    items = await context.model('Orders').as_queryable().select(
        lambda x: select(id=x.id, product=x.orderedItem.name, orderStatus=x.orderStatus.alternateName, orderDate=x.orderDate)
    ).where(
        where = lambda x, order_status: x.orderStatus.alternateName == order_status, order_status = 'OrderPickup'
        ).take(10).get_items()
    return items
```
> `/Orders?$select=id,orderedItem/name+as+product,orderStatus/alternateName+as+orderStatus,orderDate&$filter=(orderStatus/alternateName+eq+'OrderPickup')&$count=True&$top=10`

The response will contain only the selected attributes:

```json
[
    {
        "id": 12,
        "product": "Digital Storm ODE Level 3",
        "orderStatus": "OrderPickup",
        "orderDate": "2019-02-11T14:16:25.000Z"
    },
    {
        "id": 121,
        "product": "Apple iMac (27-Inch, 2013 Version)",
        "orderStatus": "OrderPickup",
        "orderDate": "2019-01-20T02:43:26.000Z"
    }
]
```

You can select attributes by using a lambda function with returns a list of them:

`lambda x: [x.id, x.name, x.model, x.category]`


```python
async def select_attributes(context):
    items = await context.model('Products').as_queryable().select(
        lambda x: [x.id, x.name, x.model, x.category]
    ).where(
        where = lambda x, category: x.category == category, category = 'Laptops'
        ).take(10).get_items()
    return items
```

### Sorting

`$orderby` query option specifies the order in which items are returned from the service.

[http://docs.oasis-open.org/odata/odata/v4.01/odata-v4.01-part1-protocol.html#sec_SystemQueryOptionorderby](http://docs.oasis-open.org/odata/odata/v4.01/odata-v4.01-part1-protocol.html#sec_SystemQueryOptionorderby)

Use `ClientDataQyeryable.order_by(expr)` to define an order by expression:

```python
async def order_results(context):
    items = await context.model('Products').as_queryable().select(
        lambda x: (x.id, x.name, x.model, x.price)
    ).where(
        lambda x: x.category == 'Laptops'
        ).order_by(
            lambda x: (round(x.price, 2),)
        ).take(10).get_items()
    return items
```
> `/Products?$select=id,name,model,price&$filter=(category+eq+'Laptops')&$orderby=round(price,2)+asc&$count=True&$top=10`

It can a lambda function which returns a tuple `lambda x: (round(x.price, 2),)` or a list `lambda x: [round(x.price, 2)]`.


`ClientDataQyeryable.order_by_descending(expr)` prepares a descending order by expression, while `ClientDataQyeryable.then_by(expr)` and `ClientDataQyeryable.then_by_descending(expr)` are available to append extra expressions.

```python
async def order_results(context):
    items = await context.model('Products').as_queryable().select(
        lambda x: (x.id, x.name, x.model, x.price, x.releaseDate)
    ).where(
        lambda x: x.category == 'Laptops'
        ).order_by(
            lambda x: (round(x.price, 2),)
        ).then_by_descending(
            lambda x: (x.releaseDate,)
        ).take(10).get_items()
    return items
```
> `/Products?$select=id,name,model,price,releaseDate&$filter=(category+eq+'Laptops')&$orderby=round(price,2)+asc,releaseDate+desc&$count=True&$top=10`

### Paging

The `$top` system query option specifies an integer that limits the number of items returned from a collection.

[http://docs.oasis-open.org/odata/odata/v4.01/odata-v4.01-part1-protocol.html#sec_SystemQueryOptiontop](http://docs.oasis-open.org/odata/odata/v4.01/odata-v4.01-part1-protocol.html#sec_SystemQueryOptiontop)

The `$skip` system query option specifies a non-negative integer n that excludes the first n items of the queried collection from the result. 

[http://docs.oasis-open.org/odata/odata/v4.01/odata-v4.01-part1-protocol.html#sec_SystemQueryOptionskip](http://docs.oasis-open.org/odata/odata/v4.01/odata-v4.01-part1-protocol.html#sec_SystemQueryOptionskip)

Use `ClientDataQyeryable.take(n)` to define `$top` query option:

```python
async def limit_results(context):
    items = await context.model('Products').as_queryable().where(
        lambda x: x.category == 'Laptops'
        ).order_by(
            lambda x: (round(x.price, 2),)
        ).take(10).get_items()
    return items
```
> `/Products?$select=id,name,model,price,releaseDate&$filter=(category+eq+'Laptops')&$orderby=round(price,2)+asc,releaseDate+desc&$count=True&$top=10`

Use `ClientDataQyeryable.skip(n)` to define `$skip` query option:

```python
async def limit_results(context):
    items = await context.model('Products').as_queryable().select(
        lambda x: (x.id, x.name, x.model, x.price, x.releaseDate)
    ).where(
        lambda x: x.category == 'Laptops'
        ).order_by(
            lambda x: (round(x.price, 2),)
        ).take(5).skip(5).get_items()
    return items
```

> `/Products?$select=id,name,model,price,releaseDate&$filter=(category+eq+'Laptops')&$orderby=round(price,2)+asc&$count=True&$top=5&$skip=5`

Use `ClientDataQyeryable.get_list()` to pass `$count` system query option and get a result set which will contain the count of items that fulfill the given query params.

```python
async def test_count_results(context):
    result_set = await context.model('Products').as_queryable().select(
        lambda x: (x.id, x.name, x.model, x.price, x.releaseDate)
    ).where(
        lambda x: x.category == 'Laptops'
        ).order_by(
            lambda x: (round(x.price, 2),)
        ).take(5).get_list()
    return result_set
```

> `/Products?$select=id,name,model,price,releaseDate&$filter=(category+eq+'Laptops')&$orderby=round(price,2)+asc&$count=true&$top=5`

A result set contains:

> `total`: An integer which represents the number of items which fulfill the given query

> `skip`: An integer which represents the number of items skipped during current paging operation

> `value`: An array of items

an example of a result set:

```json
{
    "total": 16,
    "skip": 0,
    "value": [
    ]
}
```

### Grouping

`pycentroid` introduces `$groupby` query option for using aggregated function while getting data.

```python
from pycentroid.query import select, count


async def group_result(context):
    items = await context.model('Products').as_queryable().select(
        lambda x: select(category=x.category, total=count(x.id))
    ).group_by(
        lambda x: (x.category,)
    ).get_items()
    return items
```

> `/Products?$select=category,count(id)+as+total&$groupby=category`

