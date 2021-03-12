# -*- coding: utf-8 -*-
"""
@author: Zegami Ltd
"""

import os
from pathlib import Path
import pandas as pd
import requests
from PIL import Image
import concurrent.futures
import time

from _collection_methods import (
    get_collections,
    get_collection_by_name,
    get_collection_by_id,
    create_collection,
)

from _row_methods import (
    get_rows,
    get_rows_by_tag,
    get_rows_by_filter,
)


class ZegamiClient():
    
    HOME  = 'https://zegami.com'
    API_0 = 'api/v0'
    API_1 = 'api/v1'
    
    # Collection methods
    get_collections = get_collections
    get_collection_by_name = get_collection_by_name
    get_collection_by_id = get_collection_by_id
    create_collection = create_collection
    
    # Row data methods
    get_rows = get_rows
    get_rows_by_tag = get_rows_by_tag
    get_rows_by_filter = get_rows_by_filter
    
    
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
    def _extract_workspace_id(collection):
        
        assert type(collection) == dict,\
            'Expected collection to be a dict, not {}'.format(type(collection))
            
        key = 'collectionThumbnailUrl'
        assert key in collection.keys(), 'Couldn\'t find \'{}\' in '\
            'dict: {}'.format(key, collection)
        
        s = collection[key]
        return s.split('/', 4)[-1].split('/', 1)[0]
    
    
    @staticmethod
    def _extract_imageset_id(collection):
        
        assert type(collection) == dict,\
            'Expected collection to be a dict, not {}'.format(type(collection))
            
        key = 'imageset_id'
        assert key in collection.keys(), 'Couldn\'t find \'{}\' in '\
            'dict: {}'.format(key, collection)
            
        s = collection[key]
        return s
    
    
    @staticmethod
    def _extract_dataset_id(collection):
        
        assert type(collection) == dict,\
            'Expected collection to be a dict, not {}'.format(type(collection))
            
        key = 'dataset_id'
        assert key in collection.keys(), 'Couldn\'t find \'{}\' in '\
            'dict: {}'.format(key, collection)
            
        s = collection[key]
        return s
        
        
    async def _auth_get_async(self, session, url, allow_bad=False):
        '''
        (async) Makes a GET request and returns the result promise.
        NOTE - This isn't being used, Python async is too unreliable.
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
        
        workspace_id = self._extract_workspace_id(collection)
        imageset_id = self._extract_imageset_id(collection)
        
        url = '{}/{}/project/{}/imagesets/{}'.format(
            self.HOME, self.API_0, workspace_id, imageset_id)
        
        return self._auth_get(url)['imageset']
    
    
    def _get_image_meta_lookup(self, collection):
        '''
        The order of the image urls and metadata rows is not matched. This
        lookup lets you map one to the other.
        '''
        
        # Get the workspace ID
        workspace_id = self._extract_workspace_id(collection)
        
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
        workspace_id = self._extract_workspace_id(collection)
        
        url = '{}/{}/project/{}/collections/{}/tags'.format(
            self.HOME, self.API_1, workspace_id, collection['id'])
        
        return self._auth_get(url)['tagRecords']
    
    
    ### === API === ###
        
        
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
        workspace_id = self._extract_workspace_id(collection)
            
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
    
"""
    def create_collection(self, images, data, image_column, name, description='', workspace_id=None, fail_on_missing_files=True):
        '''
        Creates a Zegami collection under the specified workspace (or your
        default workspace if not specified).
        
            images          - A list of filepaths of images or directories containing images.
                              Each of these will be recursively scanned for valid images (jpg,
                              jpeg, png, bmp, dcm).
                                                
            data            - Filepath to the data that goes with the images. Will also accept
                              a pandas DataFrame.
                              
            image_column    - The name of the column in the data containing the filenames to
                              the row's associated image. Typically 'Filename', 'Image', etc.
                              
            name            - The name of the collection.
            
            description     - A description of the collection.
        '''
        
        # == Parse the inputs ==
        
        # Images
        img_fps = []
        
        # Image file-type checker
        def _is_img(fp):
            return os.path.exists(fp) and\
                fp.lower().rsplit('.', 1)[-1]\
                in ['jpg', 'jpeg', 'png', 'bmp', 'dcm']
        
        # Cast 'images' to a list
        if type(images) != list:
            images = [images]
        
        # Check every entry in the provided images, build the img_fps list
        for entry in images:
            
            # Check it exists
            if not os.path.exists(entry):
                print('Warning - \'images\' filepath \'{}\' was not found!'\
                      .format(entry))
                    
                if fail_on_missing_files:
                    raise Exception('Missed file \'{}\''.format(entry))
                    
            # If its a directory, recursively scan it for images
            if os.path.isdir(entry):
                img_fps += [fp for fp in Path(entry).rglob('*.*') if _is_img(fp)]
                
            elif _is_img(entry):
                img_fps.append(entry)
                
            else:
                print('Warning - \'images\' entry \'{}\' was invalid!'\
                      .format(entry))
                    
                if fail_on_missing_files:
                    raise Exception('Missed file \'{}\''.format(entry))
            
        # Data
        if type(data) != pd.DataFrame:
            
            assert type(data) == str, 'Expected \'data\' argument to be a '\
                'path to a file or pd.DataFrame, not a {}.'.format(type(data))
            
            assert os.path.exists(data), '\'data\' arg \'{}\' was not a '\
                'valid filepath.'.format(data)
            
            try:
                ext = data.rsplit('.', 1)[-1].lower()
                if ext in ['csv', 'tsv']:
                    data = pd.read_csv(data)
                elif ext in ['xlsx']:
                    data = pd.read_excel(data)
                else:
                    raise ValueError('Unsure how to read \'{}\', expected a '\
                                     'tsv, csv or xlsx.'.format(data))
                        
            except Exception as e:
                raise Exception('Failed to load data: \'{}\''.format(e))
                    
        # Config
        assert type(name) == str,\
            'Expected \'name\' argument to be a str, not a {}'.format(type(name))
            
        assert type(description) == 'str',\
            'Expected \'description\' argument to be a str, not a {}'.format(type(description))
            
        assert type(image_column) == 'str',\
            'Expected \'image_column\' argument to be a str, not a {}'.format(type(image_column))
            
        
        # Get the start time of this operation
        t0 = time.time()
        
        # Ensure a workspace ID (possibly default)
        workspace_id = workspace_id or self.user_info['tenant_id']
        
        # Generate the collection creation URL
        url = '{}/{}/project/{}/collections/'\
            .format(self.HOME, self.API_0, workspace_id)
            
        # Generate the config that describes the collection
        config = {
            'name'          : name,
            'description'   : description,
            'dataset_column': image_column,
            'dataset_type'  : 'file',
            'imageset_type' : 'file',
            'file_config'   : {
                'path' : ''
            }
        }
        """
        
"""
def create(log, session, args):
    time_start = datetime.now()
    url = "{}collections/".format(
        http.get_api_url(args.url, args.project),)
    log.debug('POST: {}'.format(url))

    # parse config
    configuration = config.parse_args(args, log)
    if "name" not in configuration:
        log.error('Collection name missing from config file')
        sys.exit(1)

    # use name from config
    coll = {
        "name": configuration["name"],
    }
    # use description from config
    for key in ["description"]:
        if key in configuration:
            coll[key] = configuration[key]

    # replace empty description with an empty string
    if 'description' in coll and coll["description"] is None:
        coll["description"] = ''

    # create the collection
    response_json = http.post_json(session, url, coll)
    log.print_json(response_json, "collection", "post", shorten=False)
    coll = response_json["collection"]

    dataset_config = dict(
        configuration, id=coll["upload_dataset_id"]
    )
    if 'file_config' in dataset_config:
        if 'path' in dataset_config['file_config'] or 'directory' in dataset_config['file_config']:
            datasets.update_from_dict(log, session, dataset_config)

    imageset_config = dict(
        configuration, id=coll["imageset_id"]
    )
    imageset_config["dataset_id"] = coll["dataset_id"]
    imageset_config["collection_id"] = coll["id"]
    imagesets.update_from_dict(log, session, imageset_config)
    delta_time = datetime.now() - time_start
    log.debug("Collection uploaded in {}".format(delta_time))

    return coll
        
"""
        
        
