
# pycentroid

![pycentroid logo](https://avatars.githubusercontent.com/u/131147072?s=200&v=4)

@themost-framework for Python

[@themost-framework](https://github.com/themost-framework) is a fully-featured end-to-end framework for building scalable data-driven web applications and services. It consists of a wide-set of server-side libraries and client-side tools for helping developers creating scalable and configuratable production environments. `pycentroid` is a `@themost-framework` alternative for using its specification under python.

## pycentroid.client

A client-side library of [@themost-framework](https://github.com/themost-framework) application for python

```
from typing import List
from pycentroid.client import ClientDataContext


context = ClientDataContext(ClientContextOptions('http://localhost:3000/api/'))
// get products
items: List = await context.model('Products').as_queryable().select(
        lambda x: select(id=x.id, name=x.name, product_model=x.model,)
    ).where(
        lambda x: x.category == 'Laptops'
    ).get_items()
```

## pycentroid.query

A database-agnostic query module which for writing SQL expressions of any kind

```python
from pycentroid.query import QueryExpression, QueryEntity, SqlFormatter


products = new QueryEntity('ProductData')
query = QueryExpression(products).select(
        lambda x: select(id=x.id, name=x.name, price=round(x.price, 2))
    ).where(
        lambda x: round(x.price, 2) < 800 and x.category == 'Laptops'
    )
sql = SqlFormatter().format(query)
# sql > SELECT id,name,ROUND(price,2) AS price FROM ProductData WHERE ((ROUND(price,2)<800) AND (category='Laptops'))
```

## pycentroid.data

Next-generation ORM data module of pycentroid for developing data-driven application and services.

```python
from pycentroid.data.application import DataApplication
from pycentroid.data.context import DataContext


app = DataApplication(cwd=APP_PATH)
context = app.create_context()
results = await context.model('Order').where(
    lambda x: x.orderedItem.category == 'Desktops'
    ).get_items()
```


