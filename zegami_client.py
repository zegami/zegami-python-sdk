# -*- coding: utf-8 -*-

import os
from pathlib import Path
from io import BytesIO
import pandas as pd
import requests
from PIL import Image
import concurrent.futures
import time


class ZegamiClient():
    
    HOME  = 'https://zegami.com'
    API_0 = 'api/v0'
    API_1 = 'api/v1'
    
    def __init__(self, username=None, password=None, token=None, allow_save_token=True):
        '''
        Creates a ZegamiClient to interact with the API. Must be initialised
        using either an already acquired token, or a valid username/password.
        
        'token' can be either a token string, or the path to a locally stored
        token.
        
        If you've already logged in previously with 'allow_save_token', you
        should be able to get started without providing any arguments.
        '''
        
        # Make sure we have a token
        self._ensure_token(username, password, token, allow_save_token)
        
        # Create the async session
        self.headers = { 'Authorization' : 'Bearer {}'.format(self.token) }
        
        # (sync) Get user info
        self._refresh_user_info()
        
        # Welcome message
        try:
            print('\nInitialized successfully, welcome {}.'.format(
                self.user_info['name'].split(' ')[0]))
        except:
            pass
    
    
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
    
    
    def _get_token(self, username, password):
        '''
        Gets the client's token using a username and password.
        '''
        
        url = '{}/oauth/token/'.format(self.HOME)
        data = { 'username' : username, 'password' : password, 'noexpire' : True }
        
        r = requests.post(url, json=data)
        
        if r.status_code != 200:
            raise Exception(f'Couldn\'t set token, bad response ({r.status_code})'
                            '\nWas your username/password correct?')
            
        j = r.json()
        
        return j['token']
        
        
    @staticmethod
    def _check_status(response, allow_bad=False, is_async_request=True):
        '''
        Checks the response for a valid status code. If allow is set to True,
        doesn't throw an exception.
        '''
        
        code = response.status if is_async_request else response.status_code
        
        if code != 200:
            if not allow_bad:
                raise Exception('Bad request response ({}): {}'.format(code, response.reason))
            else:
                print('Warning - bad response ({}) allowed through: {}'.format(code, response.reason))
                return False
        
        return True
    
    
    @staticmethod
    def _get_workspace_id(collection):
        s = collection['collectionThumbnailUrl']
        return s.split('/', 4)[-1].split('/', 1)[0]
        
        
    async def _auth_get_async(self, session, url, allow_bad=False):
        '''
        (async) Makes a GET request and returns the result promise.
        NOTE - This isn't being used, Python async is too unreliable
        '''
        
        async with session.get(url) as r:
            j = await r.json()
                
            if not ZegamiClient._check_status(r, allow_bad):
                return None
        
        return j
    
    
    def _auth_get(self, url, allow_bad=False, return_response=False):
        '''
        Syncronous GET request. Used as standard over async currently.
        
        If allow_bad == True, only a warning will be printed on bad responses.
        
        If return_response == True, the response object is returned rather
        than its .json() output.
        '''
        
        r = requests.get(url, headers=self.headers)
        
        if not ZegamiClient._check_status(r, allow_bad or return_response, is_async_request=False):
            if not return_response:
                return None
        
        return r.json() if not return_response else r
    
    
    def _refresh_user_info(self):
        '''
        Sets the user info dictionary detailing user details and
        workspaces.
        '''
        
        url = '{}/oauth/userinfo/'.format(self.HOME)
        
        self.user_info = self._auth_get(url)
        
        
    def _get_imageset(self, collection):
        '''
        Gets a collection's imageset.
        '''
        
        workspace_id = self._get_workspace_id(collection)
        
        id = collection['imageset_id']
        
        url = '{}/{}/project/{}/imagesets/{}'.format(
            self.HOME, self.API_0, workspace_id, id)
        
        return self._auth_get(url)['imageset']
    
    
    def _get_image_meta_lookup(self, collection):
        '''
        The order of the image urls and metadata rows is not matched. This
        lookup lets you map one to the other.
        '''
        
        # Get the workspace ID
        workspace_id = self._get_workspace_id(collection)
        
        # Get the join ID
        imageset_dataset_join_id = collection['imageset_dataset_join_id']
        
        # Get the join dataset
        url = '{}/{}/project/{}/datasets/{}'.format(
            self.HOME, self.API_0, workspace_id, imageset_dataset_join_id)
        
        imageset_dataset_joiner = self._auth_get(url)
        
        return imageset_dataset_joiner['dataset']['imageset_indices']
    
    
    def _get_tagged_indices(self, collection):
        '''
        Gets a list of dictionaries of the form { 'key': x, 'tag': tag } from
        the provided collection.
        '''
        
        # Get the workspace ID
        workspace_id = self._get_workspace_id(collection)
        
        url = '{}/{}/project/{}/collections/{}/tags'.format(
            self.HOME, self.API_1, workspace_id, collection['id'])
        
        return self._auth_get(url)['tagRecords']
    
    
    ### === API === ###
    
    def get_collections(self, workspace_id=None):
        '''
        Gets the collections belonging in a workspace ID, or the user's
        original one by default.
        '''
        
        workspace_id = workspace_id or self.user_info['tenant_id']
        
        url = '{}/{}/project/{}/collections/'.format(
            self.HOME, self.API_0, workspace_id)
        
        collections = self._auth_get(url)['collections']
        
        return collections
    
    
    def get_collection_by_id(self, id, workspace_id=None):
        '''
        Gets a collection by its ID, belonging to a workspace ID, or the user's
        original one by default.
        '''
        
        # Get all of the available ones
        collections = [c for c in self.get_collections(workspace_id)]
        
        # Return the matching one
        for c in collections:
            if c['id'] == id:
                return c
            
        # Or return None if not found
        print('Couldn\'t find collection by ID \'{}\''.format(id))
        return None
        
        
    def get_collection_by_name(self, name, workspace_id=None):
        '''
        Gets a collection by name, belonging to a workspace ID, or the user's
        original one by default.
        '''
        
        # Get all the available ones
        collections = [c for c in self.get_collections(workspace_id)]
        
        # Return the matching one
        for c in collections:
            if c['name'].lower() == name.lower():
                return c
            
        # Or return None if not found
        print('Couldn\'t find collection by name \'{}\''.format(name))
        return None
    
    
    def get_rows(self, collection, separator='\t'):
        '''
        Gets the rows in a collection as a Dataframe.
        '''
        
        # Get the workspace ID
        workspace_id = self._get_workspace_id(collection)
        
        url = '{}/{}/project/{}/datasets/{}/file'.format(
            self.HOME, self.API_0, workspace_id, collection['dataset_id'])
        
        rows_bytes = self._auth_get(url, return_response=True).content
        tsv_bytes = BytesIO(rows_bytes)
        
        try:
            df = pd.read_csv(tsv_bytes, sep=separator)
        except:
            try:
                df = pd.read_excel(tsv_bytes)
            except:
                print('Warning - failed to open metadata as a dataframe, returned '
                      'the tsv bytes instead.')
                return tsv_bytes
    
        return df
    
    
    def get_rows_by_tag(self, collection, tags=None):
        '''
        Gets the rows of metadata in a collection by their tag as a Dataframe.
        '''
        
        # Get all the metadata
        rows = self.get_rows(collection)
        
        # Cast tags to list
        t = type(tags)
        if t == list: tags = [str(tag) for tag in tags]   # list
        else: tags = [str(tags)]                          # str/int
        
        key_tag_dicts = self._get_tagged_indices(collection)
        kt_keys = [int(dic['key']) for dic in key_tag_dicts]
        kt_tags = [dic['tag'] for dic in key_tag_dicts]
        
        indices = list(range(len(rows)))
        final_indices = indices.copy()
        
        for i in indices:
            
            is_valid = False
            for key, tag in zip(kt_keys, kt_tags):
                if key == i and tag in tags:
                    is_valid = True
                    break
                
            if not is_valid:
                final_indices.remove(i)
                
        return rows.iloc[final_indices,:]
    
    
    def get_rows_by_filter(self, collection, filters):
        '''
        Gets rows of metadata in a collection by a flexible filter.
        
        The filter should be a dictionary describing what to permit through
        any specified columns.
        
        Example:
            row_filter = { 'breed': ['Cairn', 'Dingo'] }
            
            This would only return rows whose 'breed' column matches 'Cairn'
            or 'Dingo'.
        '''
        
        assert type(filters) == dict, 'Filters should be a dict.'
        
        # Get all the metadata
        rows = self.get_rows(collection)
        
        for fk, fv in filters.items():
            
            if not type(fv) == list:
                fv = [fv]
            
            rows = rows[rows[fk].isin(fv)]
            
        return rows
        
        
    def get_image_urls(self, collection, rows):
        '''
        Gets the URLs of every row of metadata provided. Can be a subset of
        all metadata obtained with 'get_rows_by_tag()', etc.
        Rows can be a pd.DataFrame, a list of ints, or an int.
        '''
        
        # Turn the provided 'rows' into a list of ints
        if type(rows) == pd.DataFrame:
            indices = list(rows.index)
            
        elif type(rows) == list:
            indices = [int(r) for r in rows]
            
        elif type(rows) == int:
            indices = [rows]
            
        else:
            raise ValueError('Invalid rows argument, \'{}\' not supported'\
                             .format(type(rows)))
            
        # Get the workspace ID
        workspace_id = self._get_workspace_id(collection)
            
        # Get the imageset
        imageset = self._get_imageset(collection)
            
        # Convert from imageset space to rowspace
        lookup = self._get_image_meta_lookup(collection)
            
        # Convert rows into imageset indices
        imageset_indices = [lookup[i] for i in indices]
        
        # Build the urls
        urls = ['{}/{}/project/{}/imagesets/{}/images/{}/data'.format(
            self.HOME, self.API_0, workspace_id, imageset['id'], i)
            for i in imageset_indices]
        
        return urls
    
    
    def download_image(self, url):
        '''
        Downloads an image into memory (as a PIL.Image).
        
        To obtain a URL, search for rows using 'get_rows()' (or a filtered
        version of this) and then pass these into 'get_image_urls()'
        '''
        
        r = requests.get(url, headers=self.headers, stream=True)
        r.raw.decode = True
        
        return Image.open(r.raw)
        
        
    def download_image_batch(self, urls, max_workers=50, show_time_taken=True):
        '''
        Downloads multiple images into memory (each as a PIL.Image)
        concurrently.
        
        Please be aware that these images are being downloaded into memory,
        if you download a huge collection of images you may eat up your RAM!
        '''
        
        def download_single(index, url):
            return (index, self.download_image(url))
        
        t = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
            
            futures = [ex.submit(download_single, i, u) for i, u in enumerate(urls)]
        
            images = {}
            for f in concurrent.futures.as_completed(futures):
                i, img = f.result()
                images[i] = img
        
        if show_time_taken:
            print('\nDownloaded {} images in {:.2f} seconds.'.format(
                len(images), time.time() - t))
        
        # Results are a randomly ordered dictionary of results, so reorder them
        ordered = []
        for i in range(len(images)):
            ordered.append(images[i])
                
        return ordered
        
        
        
        
        
        
        
        
        
        
