# -*- coding: utf-8 -*-
"""
@author: Zegami Ltd
"""

import os
from pathlib import Path
import pandas as pd
import time
    
    
def list_collections(self, workspace_id=None, return_dictionaries=False, suppress_message=False):
    '''
    Displays collections in the workspace, or currently active one if not
    provided.
    Use return_dictionaries to return a list of collection objects.
    '''
    
    workspace_id = workspace_id or self.active_workspace_id
    
    url = '{}/{}/project/{}/collections/'.format(
        self.HOME, self.API_0, workspace_id)
    
    collections = self._auth_get(url, allow_bad=True)
    if not collections:
        return [] if return_dictionaries else None
    collections = collections['collections']
        
    all_collection_ids = [c['id'] for c in collections]
    all_collection_names = [c['name'] for c in collections]
    
    if not suppress_message:
        print('')
        print('Collections in workspace \'{}\':'.format(workspace_id))
        for id, name in zip(all_collection_ids, all_collection_names):
            print('{} : {}'.format(id, name))
        print('')
    
    if return_dictionaries:
        return collections
    
    
def get_collection_by_name(self, name, workspace_id=None):
    '''
    Gets a collection by name, belonging to a workspace ID, or the active one
    by default.
    '''
    
    workspace_id = workspace_id or self.active_workspace_id
    
    # Get all the available ones
    collections = [c for c in self.list_collections(workspace_id,
                    return_dictionaries=True, suppress_message=True)]
    
    # Return the matching one
    for c in collections:
        if c['name'].lower() == name.lower():
            return c
        
    # Or return None if not found
    print('Couldn\'t find collection by name \'{}\''.format(name))
    return None


def get_collection_by_id(self, id, workspace_id=None):
    '''
    Gets a collection by its ID, belonging to a workspace ID, or the active
    one by default.
    '''
    
    workspace_id = workspace_id or self.active_workspace_id
    
    # Get all of the available ones
    collections = [c for c in self.list_collections(workspace_id,
                    return_dictionaries=True, suppress_message=True)]
    
    # Return the matching one
    for c in collections:
        if c['id'] == id:
            return c
        
    # Or return None if not found
    print('Couldn\'t find collection by ID \'{}\''.format(id))
    return None


def extract_collection_url(self, collection):
    '''
    Gets the URL of the collection, as seen in the browser when you navigate
    to it.
    '''
    
    wid = self._extract_workspace_id(collection)
    cid = self._extract_id(collection)
    
    return '{}/collections/{}-{}'.format(self.HOME, wid, cid)
        
    
# -- Collection dictionary information extractors --

def _extract_id(collection):
    
    assert type(collection) == dict,\
        'Expected collection to be a dict, not {}'.format(type(collection))
        
    key = 'id'
    assert key in collection.keys(), 'Couldn\'t find \'{}\' in '\
        'dict: {}'.format(key, collection)
        
    return collection[key]


def _extract_workspace_id(self, collection):
    '''
    If this cannot be derived from the dictionary, the client will attempt to
    locate it from all workspaces in the user's scope. If THIS fails, it will
    just assume its from the active workspace (shouldn't ever reach here).
    '''
    
    assert type(collection) == dict,\
        'Expected collection to be a dict, not {}'.format(type(collection))
        
    key = 'collectionThumbnailUrl'
    
    if key in collection.keys():
        wid = collection[key].split('/', 4)[-1].split('/', 1)[0]
        
    else:
        wid = self._lookup_workspace_id(collection)
        
        if not wid:   
            wid = self.active_workspace_id
            print('Assuming workspace from active_workspace_id \'{}\''.format(wid))
        
    return wid


def _lookup_workspace_id(self, collection):
    '''
    Fallback for _extract_workspace_id to use if it can't be reliably
    obtained from the dictionary. Searches through user_info workspaces,
    trying the active workspace first.
    '''
    
    # Look in the active workspace first
    cols = self.list_collections(return_dictionaries=True, suppress_message=True)
    for c in cols:
        if c['id'] == collection['id']:
            return self.active_workspace_id
    
    # Then try looking in all scoped workspaces
    ws = self.user_info['projects']
    for w in ws:
        cols = self.list_collections(w['id'], return_dictionaries=True, suppress_message=True)
        for c in cols:
            if c['id'] == collection['id']:
                print('Looked up collection \'{}\' in (not-active) workspace \'{}\''.format(c['name'], w['name']))
                return w['id']


def _extract_dataset_id(collection):
    
    assert type(collection) == dict,\
        'Expected collection to be a dict, not {}'.format(type(collection))
        
    key = 'dataset_id'
    assert key in collection.keys(), 'Couldn\'t find \'{}\' in '\
        'dict: {}'.format(key, collection)
        
    return collection[key]


def _extract_imageset_id(ZC, collection, source=None):
    '''
    Extracts the imageset_id from a collection. Provide a source index to get
    the imageset_id of a specific source. If not provided and it is a
    multi-source collection, gets the first source's imageset_id.
    '''
    
    assert type(collection) == dict,\
        'Expected collection to be a dict, not {}'.format(type(collection))
        
    assert source is None or (type(source) is int and source >= 0),\
        'Expected source to be None or a positive integer, not: {}'.format(source)
        
    version = ZC._extract_version(collection)
    key = 'imageset_id'
    
    # Old collections have the imageset ID in the root of the dictionary
    if version < 2:
        
        assert key in collection.keys(), 'Couldn\'t find \'{}\' in '\
            'dict: {}'.format(key, collection)
        
        return collection[key]
    
    # Multi-source collections have lists of imageset info
    source = source or 0
    s = ZC._extract_image_source(collection, source)
    
    return s[key]
        
        
def _get_imageset(self, collection, source=None):
    '''
    Gets a collection's imageset.
    
    If it is a multi-source collection, provide a source index to choose
    which source to retrieve, or leave alone to just get the first source.
    '''
    
    workspace_id = self._extract_workspace_id(collection)
    imageset_id = self._extract_imageset_id(collection, source)
    
    url = '{}/{}/project/{}/imagesets/{}'.format(
        self.HOME, self.API_0, workspace_id, imageset_id)
        
    return self._auth_get(url)['imageset']


def _get_image_meta_lookup(self, collection, source=None):
    '''
    The order of the image urls and metadata rows is not matched. This
    lookup lets you map one to the other.
    
    If it is a multi-source collection, provide a source index to choose
    which source to retrieve, or leave alone to just get the first source.
    '''
    
    # Get the workspace ID
    workspace_id = self._extract_workspace_id(collection)
    version = self._extract_version(collection)
    key = 'imageset_dataset_join_id'
    
    if version < 2:
        assert key in collection.keys(),\
            'Expected to find \'{}\' in {}'.format(key, collection)
            
        imageset_dataset_join_id = collection[key]
    
    else:
        source = source or 0
        s = self._extract_image_source(collection, source)
        
        assert key in s.keys(),\
            'Expected to find \'{}\' in {}'.format(key, s)
            
        imageset_dataset_join_id = s[key]
    
    
    # Get the join dataset
    url = '{}/{}/project/{}/datasets/{}'.format(
        self.HOME, self.API_0, workspace_id, imageset_dataset_join_id)
    
    imageset_dataset_joiner = self._auth_get(url)
    
    return imageset_dataset_joiner['dataset']['imageset_indices']


def _extract_version(collection):
    '''
    Returns the version of the collection. Older versions do not support
    multiple image sources. If a collection has no 'version' key, it is
    assumed to be version 1.
    '''
    
    return collection['version'] if 'version' in collection.keys() else 1


def _extract_image_source(ZC, collection, source):
    '''
    Extracts an image_source dictionary from a v >= 2 collection.
    '''
    
    v = ZC._extract_version(collection)
    assert v >= 2,\
        'Trying to extract image source from an old-style collection (v{})'.format(v)
    
    assert type(source) is int,\
        'Expected source to be an int, not {}'.format(type(source))
        
    assert source >= 0, 'Expected source to be >= 0'
    
    assert 'image_sources' in collection.keys(),\
        'Expected to find \'image_sources\' in collection {}'.format(collection)
        
    srcs = collection['image_sources']
        
    assert len(srcs) > source,\
        'Tried to get source index {} which is greater than len(sources) ({})'\
        .format(source, len(srcs))
        
    return srcs[source]
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    