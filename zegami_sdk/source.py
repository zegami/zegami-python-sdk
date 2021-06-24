# -*- coding: utf-8 -*-
"""
Zegami Ltd.

Apache 2.0
"""


class Source():
    def __init__(self, collection, source_dict):
        self._collection = collection
        self._data = source_dict

    @property
    def name():
        pass

    @name.getter
    def name(self):
        assert self._data, 'Source had no self._data set'
        assert 'name' in self._data.keys(), 'Source\'s data didn\'t have a \'name\' key'
        return self._data['name']

    @property
    def id():
        pass

    @id.getter
    def id(self):
        assert self._data, 'Source had no self._data set'
        assert 'source_id' in self._data, 'Source\'s data didn\'t have a \'source_id\' key'
        return self._data['source_id']

    @property
    def _imageset_id():
        pass

    @_imageset_id.getter
    def _imageset_id(self):
        assert self._data, 'Source had no self._data set'
        assert 'imageset_id' in self._data, 'Source\'s data didn\'t have an \'imageset_id\' key'
        return self._data['imageset_id']

    @property
    def _imageset_dataset_join_id():
        pass

    @_imageset_dataset_join_id.getter
    def _imageset_dataset_join_id(self):
        k = 'imageset_dataset_join_id'
        assert self._data, 'Source had no self._data set'
        assert k in self._data, 'Source\'s data didn\'t have an \'{}\' key'.format(k)
        return self._data[k]
