# -*- coding: utf-8 -*-
"""
@author: Zegami Ltd
"""

import os
from pathlib import Path

from _collection_methods import (
    list_collections,
    get_collection_by_name,
    get_collection_by_id,
    
    extract_collection_url,
    _extract_id,
    _extract_workspace_id,
    _lookup_workspace_id,
    _extract_imageset_id,
    _extract_dataset_id,
    _extract_version,
    _extract_image_source,
    _get_imageset,
    _get_image_meta_lookup
)

from _row_methods import (
    get_rows,
    get_rows_by_tag,
    get_rows_by_filter,
)

from _image_methods import (
    get_image_urls,
    list_image_sources,
    download_image,
    download_image_batch,
)

from _annotation_methods import (
    get_annotations_for_collection,
    get_annotations_for_image,
    post_annotation,
    create_mask_annotation,
    _reconstitute_mask,
)

from _util_methods import (
    _get_token,
    _check_status,
    _auth_get,
    _auth_post,
)


class ZegamiClient():
    
    HOME  = 'https://zegami.com'
    API_0 = 'api/v0'
    API_1 = 'api/v1'
    
    # Collection methods
    list_collections = list_collections
    get_collection_by_name = get_collection_by_name
    get_collection_by_id = get_collection_by_id
    
    # Collection information processors
    extract_collection_url = extract_collection_url
    _get_imageset = _get_imageset
    _get_image_meta_lookup = _get_image_meta_lookup
    _extract_workspace_id = _extract_workspace_id
    _lookup_workspace_id = _lookup_workspace_id # Fallback for extract
    _extract_id = staticmethod(_extract_id)
    _extract_imageset_id = classmethod(_extract_imageset_id)
    _extract_dataset_id = staticmethod(_extract_dataset_id)
    _extract_version = staticmethod(_extract_version)
    _extract_image_source = classmethod(_extract_image_source)
    
    # Row data methods
    get_rows = get_rows
    get_rows_by_tag = get_rows_by_tag
    get_rows_by_filter = get_rows_by_filter
    
    # Image methods
    get_image_urls = get_image_urls
    list_image_sources = list_image_sources
    download_image = download_image
    download_image_batch = download_image_batch
    
    # Annotation methods
    get_annotations_for_collection = get_annotations_for_collection
    get_annotations_for_image = get_annotations_for_image
    post_annotation = post_annotation
    create_mask_annotation = staticmethod(create_mask_annotation)
    _reconstitute_mask = staticmethod(_reconstitute_mask)
    
    # Utilities
    _auth_get = _auth_get
    _auth_post = _auth_post
    _get_token = classmethod(_get_token)
    _check_status = staticmethod(_check_status)
    
    
    def __init__(self, username=None, password=None, active_workspace_id=None,
                 token=None, allow_save_token=True):
        '''
        Creates a ZegamiClient to interact with the API. Must be initialised
        using either an already acquired token, or a valid username/password.
        
        'token' can be either a token string, or the path to a locally stored
        token.
        
        If you've already logged in previously with 'allow_save_token', you
        should be able to get started without providing any arguments.
        
        Assign an 'active_workspace' to make this client assume you're dealing
        with collections in that workspace. This is automatically set to the
        first workspace assigned to your account, but feel free to change it
        for conveniently access collections in an alternate workspace.
        '''
        
        # Make sure we have a token
        self._ensure_token(username, password, token, allow_save_token)
        
        # Create the async session
        self.headers = { 'Authorization' : 'Bearer {}'.format(self.token) }
        
        # Get user info
        self._refresh_user_info()
        
        # Set the default workspace ID to use for collection operations
        self.active_workspace_id = active_workspace_id or self.user_info['tenant_id']
        
        if not active_workspace_id:
            print('\nNote: To use a different workspace, simply set zc.active_workspace_id=\'8-character-workspace-id\'. '\
                  'For a list of these 8-character IDs try zc.list_workspaces(), or copy the ID from the site.')
        
        # Welcome message
        try:
            print('\nInitialized successfully, welcome {}.'.format(
                self.user_info['name'].split(' ')[0]))
        except:
            pass
        
        
    @property
    def active_workspace_id():
        pass
    @active_workspace_id.getter
    def active_workspace_id(self):
        return self._active_workspace_id
    @active_workspace_id.setter
    def active_workspace_id(self, new_active_workspace_id):
        assert type(new_active_workspace_id) is str, 'Set the active workspace ID using a string'
        assert len(new_active_workspace_id) == 8, 'Expected active workspace ID to be 8 characters long'
        
        # Validate that this workspace is visible
        name = None
        for w in self.user_info['projects']:
            if w['id'] == new_active_workspace_id:
                name = w['name']
                break
        assert name is not None, 'Invalid workspace ID - not among list of workspaces available to the user.'
        
        self._active_workspace_id = new_active_workspace_id
            
        print('\nSet new active workspace ID to \'{}\', {}.'\
              .format(self._active_workspace_id, name))
        
    
    def list_workspaces(self, return_dictionaries=False, suppress_message=False):
        '''
        Displays workspaces available to the user.
        Use return_dictionaries to return a list of workspace objects.
        '''
        
        if not self.user_info:
            self._refresh_user_info()
            
        tenant_id = self.user_info['tenant_id']
        
        ws = self.user_info['projects']
            
        all_workspace_ids = [w['id'] for w in ws]
        all_workspace_names = [w['name'] for w in ws]
        
        if not suppress_message:
            print('')
            print('Your tenant (primary) workspace ID: {}'.format(tenant_id))
            print('Your current active workspace ID:   {}'.format(self.active_workspace_id))
            print('\nExhaustive list of available workspaces:')
            for id, name in zip(all_workspace_ids, all_workspace_names):
                print('{} : {}'.format(id, name))
            print('')
        
        if return_dictionaries:
            return ws
    
    
    def _ensure_token(self, username, password, token, allow_save_token):
        '''
        Tries the various logical steps to ensure a login token is set.
        Will use username/password if given, but will fallback on potentially
        saved token files.
        '''
        
        # Potential location of locally saved token
        local_token_path = os.path.join(Path.home(), 'zegami.token')
        
        if token:
            if os.path.exists(token):
                with open(token, 'r') as f:
                    self.token = f.read()
            else:
                self.token = token
            
        elif username and password:
            self.token = self._get_token(username, password)
            if allow_save_token:
                with open(local_token_path, 'w') as f:
                    f.write(self.token)
                print('Token saved locally to \'{}\'.'.format(local_token_path))
            
        else:
            # Potentially use local token
            local_token_path = os.path.join(Path.home(), 'zegami.token')
            if os.path.exists(local_token_path):
                with open(local_token_path, 'r') as f:
                    self.token = f.read()
                print('Used token from \'{}\'.'.format(local_token_path))
            else:
                raise ValueError('No username & password or token was given, '
                                 'and no locally saved token was found.')
    
    
    def _refresh_user_info(self):
        '''
        Sets the user info dictionary detailing user details and
        workspaces.
        '''
        
        url = '{}/oauth/userinfo/'.format(self.HOME)
        
        self.user_info = self._auth_get(url)
    
    
    def _get_tagged_indices(self, collection):
        '''
        Gets a list of dictionaries of the form { 'key': x, 'tag': tag } from
        the provided collection.
        '''
        
        # Get the workspace ID
        workspace_id = self._extract_workspace_id(collection)
        
        url = '{}/{}/project/{}/collections/{}/tags'.format(
            self.HOME, self.API_1, workspace_id, collection['id'])
        
        return self._auth_get(url)['tagRecords']
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
