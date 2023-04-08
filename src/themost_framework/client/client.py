from themost_framework.common.objects import object
from themost_framework.query import OpenDataQueryExpression, OpenDataFormatter
import requests
from requests.structures import CaseInsensitiveDict
from urllib.parse import urljoin


class ClientContextOptions():
    remote=None
    def __init__(self, remote):
        self.remote = remote


class ClientService():
    def __init__(self, options):
        self.options = options
        self.headers = CaseInsensitiveDict()
    
    def set(self, key, value):
        self.headers.update([
            [
                key,
                value
            ]
        ])

    def pop(self, key):
        self.headers.pop(key)
        

class ClientDataModel():
    service = None
    def __init__(self, name):
        self.name = name
    
    def as_queryable(self):
        return ClientDataQueryable(self)

    @property
    def url(self):
        return urljoin(self.service.options.remote, self.name)

    def save(self, data:dict):
        # get url e.g. /Orders
        url = self.url
        # get headers
        headers = self.model.service.headers.copy()
        # make request and send data
        response = requests.post(url, json=data, headers=headers)
        return response.json()
    
    def remove(self, item):
        # get url e.g. /Orders/1234000
        url = urljoin(self.url, item)
        # get service headers
        headers = self.model.service.headers.copy()
        # make request
        response = requests.delete(url, headers=headers)
        return response.json()


class ClientDataQueryable(OpenDataQueryExpression):

    model = None
    def __init__(self, model: ClientDataModel):
        super().__init__(model.name)
        self.model = model

    @property
    def params(self):
        return OpenDataFormatter().format(self)

    @property
    def url(self):
        return urljoin(self.model.service.options.remote, self.model.name)

    def get_items(self):
        # get url
        url = self.url
        # get headers
        headers = self.model.service.headers.copy()
        # get query params
        params = self.params
        # add accept header
        if not 'Accept' in headers:
            headers.update([
                [
                    'Accept',
                    'application/json'
                ]
            ])
        # make request
        response = requests.get(url, params, headers = headers)
        return response.json()

    def get_item(self):
        url = self.url
        headers = self.model.service.headers.copy()
        params = self.params
        params.update([
            [
                '$top', 1
            ],
            [
                '$skip', 0
            ]
        ])
        if not 'Accept' in headers:
            headers.update([
                [
                    'Accept',
                    'application/json'
                ]
            ])
        response = requests.get(url, params, headers = headers)
        return response.json()



class ClientContext():
    def __init__(self, options):
        self.service = ClientService(options)
    
    def model(self, name):
        """Returns an of data model for further processing

        Args:
            name (_type_): The name of the remote data model

        Returns:
            ClientDataModel: The instance of data model
        """
        model = ClientDataModel(name)
        model.service = self.service
        return model
