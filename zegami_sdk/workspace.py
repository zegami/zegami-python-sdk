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

from .collection import Collection
from .helper import guess_data_mimetype


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

    def create_storage_item(self, data, mime_type=None, item_name=None):

        if not mime_type:
            mime_type = guess_data_mimetype(data)

        # get signed url to use signature
        client = self._client
        url = '{}/{}/project/{}/storage/signedurl'.format(client.HOME, client.API_1, self.id)
        if item_name:
            url += '?name={}'.format(item_name)
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

    # Version should be used once https://github.com/zegami/zegami-cloud/pull/1103/ is merged
    def _create_empty_collection(self, coll_name, desc='', dynamic=False, version=2, **kwargs):
        """Crete an empty collection that will be updated with images and data."""

        post_data = {
            'name': coll_name,
            'description': desc,
            'dynamic': dynamic,
            'upload_dataset': {'source': {'upload': {}}},
            'version': version,
            **kwargs,
        }

        create_url = f'{self._client.HOME}/{self._client.API_0}/project/{self.id}/collections'

        resp = self._client._auth_post(create_url, body=None, json=post_data)

        return resp['collection']

    def _get_all_sources_names(self, sources):
        """Get the names of all sources in a list."""
        sources_names = []
        for source in sources:
            sources_names.append({'name': source['source_name']})
        return sources_names

    def create_collection(self, coll_name, sources_list, data=None, desc='', dynamic=False, version=2, **kwargs):
        """
        Create a collection with provided images and data.
        Image data should be provided as a list of sources, each item there must contain a dict
        with the source name and configuration for the source a path to the image directory:
            [{
                'source_name': 'name',
                'image_dir: 'some/path',
                'recurse_dirs': True,  // whether to upload files from subfolders. Defaults to False
                'mime_type': 'image/jpg',  // optionally specify the mime type rather than inferring
            }]
        """
        if version == 2:
            sources_names = self._get_all_sources_names(sources_list)
            kwargs['image_sources'] = sources_names
        else:
            # Make sure the mock source has the correct name
            sources_list[0]['source_name'] = coll_name

        coll_details = self._create_empty_collection(
            coll_name, desc, dynamic=dynamic, version=version, **kwargs)
        coll = self.get_collection_by_id(coll_details['id'])

        if data:
            coll.replace_data(data)

        for source in sources_list:
            coll.upload_images(source)
        print(f'Collection [id: {coll.id}] created successfully.')
        return coll

    def __len__(self):
        len(self.collections)

    def __repr__(self):
        return "<Workspace id={} name={}>".format(self.id, self.name)
