# -*- coding: utf-8 -*-
"""
Zegami Ltd.

Apache 2.0
"""
import io
from urllib.parse import urlparse

from azure.storage.blob import (
    ContainerClient,
    ContentSettings,
)

import magic

from .collection import Collection


class Workspace():
    def __init__(self, client, workspace_dict):
        self._client = client
        self._data = workspace_dict
        self._check_data()

    @property
    def id():
        pass

    @id.getter
    def id(self):
        assert 'id' in self._data.keys(), 'Workspace\'s data didn\'t have an \'id\' key'
        return self._data['id']

    @property
    def name():
        pass

    @name.getter
    def name(self):
        assert 'name' in self._data.keys(), 'Workspace\'s data didn\'t have a \'name\' key'
        return self._data['name']

    @property
    def collections():
        pass

    @collections.getter
    def collections(self):
        c = self._client
        assert c, 'Workspace had no client set when obtaining collections'

        url = '{}/{}/project/{}/collections/'.format(c.HOME, c.API_0, self.id)
        collection_dicts = c._auth_get(url)
        if not collection_dicts:
            return []
        collection_dicts = collection_dicts['collections']
        return [Collection._construct_collection(c, self, d) for d in collection_dicts]

    def get_collection_by_name(self, name):
        cs = self.collections
        for c in cs:
            if c.name.lower() == name.lower():
                return c
        raise ValueError('Couldn\'t find a collection with the name \'{}\''.format(name))

    def get_collection_by_id(self, id):
        cs = self.collections
        for c in cs:
            if c.id == id:
                return c
        raise ValueError('Couldn\'t find a collection with the ID \'{}\''.format(id))

    def show_collections(self):
        cs = self.collections
        assert cs, 'Workspace obtained invalid collections'

        print('\nCollections in \'{}\' ({}):'.format(self.name, len(cs)))
        for c in cs:
            print('{} : {}'.format(c.id, c.name))

    def _check_data(self) -> None:
        assert self._data, 'Workspace had no self._data set'
        assert type(self._data) == dict, 'Workspace didn\'t have a dict for '\
            'its data ({})'.format(type(self._data))

    def get_storage_item(self, storage_id):
        c = self._client
        url = '{}/{}/project/{}/storage/{}'.format(c.HOME, c.API_1, self.id, storage_id)
        resp = c._auth_get(url, return_response=True)
        return io.BytesIO(resp.content), resp.headers.get('content-type')

    def create_storage_item(self, data, mime_type=None):
        if not mime_type:
            try:
                mime_type = magic.from_buffer(data, mime=True)
            except TypeError:
                mime_type = 'application/octet-stream'

        # get signed url to use signature
        client = self._client
        url = '{}/{}/project/{}/storage/signedurl'.format(client.HOME, client.API_1, self.id)
        resp = client._auth_get(url)

        blob_id = 'storage/' + resp['id']
        url = resp['signedurl']

        url_object = urlparse(url)
        sas_token = url_object.query
        account_url = url_object.scheme + '://' + url_object.netloc
        container_name = url_object.path.split('/')[1]

        container_client = ContainerClient(account_url, container_name, credential=sas_token)
        container_client.upload_blob(
            blob_id,
            data,
            blob_type='BlockBlob',
            content_settings=ContentSettings(content_type=mime_type)
        )

        return resp['id']

    def delete_storage_item(self, storage_id):
        c = self._client
        url = '{}/{}/project/{}/storage/{}'.format(c.HOME, c.API_1, self.id, storage_id)
        resp = c._auth_delete(url)
        return resp.ok

    def __len__(self):
        len(self.collections)

    def __repr__(self):
        return "<Workspace id={} name={}>".format(self.id, self.name)
