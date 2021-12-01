# -*- coding: utf-8 -*-
# Copyright 2021 Zegami Ltd

"""collection functionality."""

import io
import os
from urllib.parse import urlparse

from azure.storage.blob import (
    ContainerClient,
    ContentSettings,
)
import pandas as pd

from .collection import Collection
from .helper import guess_data_mimetype
from .source import UploadableSource


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
    def client():
        pass

    @client.getter
    def client(self):
        return self._client

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
        if not c:
            raise ValueError('Workspace had no client set when obtaining collections')

        url = '{}/{}/project/{}/collections/'.format(c.HOME, c.API_0, self.id)
        collection_dicts = c._auth_get(url)
        if not collection_dicts:
            return []
        collection_dicts = collection_dicts['collections']
        return [Collection._construct_collection(c, self, d) for d in collection_dicts]

    def get_collection_by_name(self, name) -> Collection:
        """Obtains a collection by name (case-insensitive)."""
        matches = list(filter(lambda c: c.name.lower() == name.lower(), self.collections))
        if len(matches) == 0:
            raise IndexError('Couldn\'t find a collection with the name \'{}\''.format(name))
        return matches[0]

    def get_collection_by_id(self, id) -> Collection:
        """Obtains a collection by ID."""
        matches = list(filter(lambda c: c.id == id, self.collections))
        if len(matches) == 0:
            raise IndexError('Couldn\'t find a collection with the ID \'{}\''.format(id))
        return matches[0]

    def show_collections(self) -> None:
        """Prints this workspace's available collections."""
        cs = self.collections
        if not cs:
            print('No collections found')
            return

        print('\nCollections in \'{}\' ({}):'.format(self.name, len(cs)))
        for c in cs:
            print('{} : {}'.format(c.id, c.name))

    def _check_data(self) -> None:
        """This object should have a populated self._data, runs a check."""
        if not self._data:
            raise ValueError('Workspace has no self._data set')
        if type(self._data) is not dict:
            raise TypeError('Workspace didn\'t have a dict for its data ({})'.format(type(self._data)))

    def get_storage_item(self, storage_id) -> io.BytesIO:
        """Obtains an item in online-storage by its ID."""
        c = self._client
        url = '{}/{}/project/{}/storage/{}'.format(c.HOME, c.API_1, self.id, storage_id)
        resp = c._auth_get(url, return_response=True)
        return io.BytesIO(resp.content), resp.headers.get('content-type')

    def create_storage_item(self, data, mime_type=None, item_name=None) -> str:
        """Creates and uploads data into online-storage. Returns its storage ID."""
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

    def delete_storage_item(self, storage_id) -> bool:
        """Deletes a storage item by ID. Returns the response's OK signal."""
        c = self._client
        url = '{}/{}/project/{}/storage/{}'.format(c.HOME, c.API_1, self.id, storage_id)
        resp = c._auth_delete(url)
        return resp.ok

    # Version should be used once https://github.com/zegami/zegami-cloud/pull/1103/ is merged
    def _create_empty_collection(self, name, uploadable_sources, description='', **kwargs):
        """Create an empty collection, ready for images and data."""
        defaults = {
            'version': 2,
            'dynamic': False,
            'upload_dataset': {'source': {'upload': {}}}
        }

        for k, v in defaults.items():
            if k not in kwargs.keys():
                kwargs[k] = v

        # Don't let the user provide these
        reserved = ['name', 'description', 'image_sources']
        for k in reserved:
            if k in kwargs.keys():
                del kwargs[k]

        # Data to generate the collection, including sparse sources with no data
        post_data = {
            'name': name,
            'description': description,
            'image_sources': [{'name': s.name} for s in uploadable_sources],
            **kwargs
        }

        url = '{}/{}/project/{}/collections'.format(
            self.client.HOME, self.client.API_0, self.id)

        resp = self.client._auth_post(url, body=None, json=post_data)

        return resp['collection']

    def create_collection(self, name, uploadable_sources, data=None, description='', **kwargs):  # noqa: C901
        """
        Create a collection with provided images and data.

        A list of image sources (or just one) should be provided, built using
        Source.create_uploadable_source(). These instruct the SDK where to
        look for images to populate the collection.

        - name:
            The name of the collection.

        - uploadable_sources:
            A list of [UploadableSource()] built using:

                from zegami_sdk.source import UploadableSource
                sources = [ UploadableSource(params_0),
                            UploadableSource(params_1),
                            ... ]

        - data:
            Uploading data is optional when uploading a single source, but
            required for multi-source collections to match sibling images
            together.

            Each UploadableSource has a filename_column that should
            point to a column in the data. This column should contain the
            filename of each image for that source.

            Multiple sources may share the same column if all images of
            different sources have the same names.

            Provide a pandas.DataFrame() a filepath to a .csv.

        - description:
            A description for the collection.
        """
        # Parse for a list of UploadableSources
        print('- Parsing uploadable source list')
        uploadable_sources = UploadableSource._parse_list(uploadable_sources)

        # If using multi-source, must provide data
        if data is None and len(uploadable_sources) > 1:
            raise ValueError(
                'If uploading more than one image source, data '
                'is required to correctly join different images from each'
            )

        # Parse data
        if type(data) is str:
            if not os.path.exists(data):
                raise FileNotFoundError('Data file "{}" doesn\'t exist'.format(data))
            # Check the file extension
            if data.split('.')[-1] == 'tsv':
                data = pd.read_csv(data, delimiter='\t')
            elif data.split('.')[-1] in ['xls', 'xlsx']:
                data = pd.read_excel(data)
            else:
                data = pd.read_csv(data)

        # Check that all source filenames exist in the provided data
        if data is not None:
            print('- Checking data matches uploadable sources')
            for s in uploadable_sources:
                s._check_in_data(data)

        # Create an empty collection
        print('- Creating blank collection "{}"'.format(name))
        blank_resp = self._create_empty_collection(
            name, uploadable_sources, description=description, **kwargs)
        blank_id = blank_resp['id']
        blank = self.get_collection_by_id(blank_id)

        # If uploading data, do it now from the DataFrame
        if data is not None:
            print('- Uploading data')
            blank.replace_data(data)

        # Fill in UploadableSource information with empty generated sources
        print('- Registering collection sources to uploadable sources')
        for i, us in enumerate(uploadable_sources):
            us._register_source(i, blank.sources[i])

        # Upload source data
        for us in uploadable_sources:
            us._upload()

        # Format output string
        plural_str = '' if len(uploadable_sources) < 2 else 's'
        data_str = 'no data' if data is None else\
            'data of shape {} rows x {} columns'\
            .format(len(data), len(data.columns))

        print(
            '- Finished collection "{}" upload using {} image source{} with {}'
            .format(name, len(uploadable_sources), plural_str, data_str)
        )

    def __len__(self):
        len(self.collections)

    def __repr__(self):
        return "<Workspace id={} name={}>".format(self.id, self.name)
