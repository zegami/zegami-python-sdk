# -*- coding: utf-8 -*-
"""
@author: Zegami Ltd
"""

import os
from pathlib import Path
import pandas as pd
import time
    
    
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


def _execute_create_collection(workspace_id, config):
    
    # Generate the collection creation URL
    url = '{}/{}/project/{}/collections/'\
        .format(self.HOME, self.API_0, workspace_id)
        
    


def create_collection(self, images, data, image_column, name, description='', workspace_id=None, fail_on_missing_files=True):
    '''
    Creates a single-source Zegami collection under the specified workspace 
    (or your default workspace if not specified). Resulting config is in multi
    image source format (one source).
    
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
    
    # Image file-type checker
    def _is_img(fp):
        return os.path.exists(fp)\
            and fp.lower().rsplit('.', 1)[-1]\
            in ['jpg', 'jpeg', 'png', 'bmp', 'dcm']
    
    # Cast 'images' to a list
    if type(images) != list:
        images = [images]
    
    # Check every entry in the provided images, build the img_fps list
    img_fps = []
    for entry in images:
        
        # Check it exists
        if not os.path.exists(entry):
            print('Warning - \'images\' filepath \'{}\' was not found!'\
                  .format(entry))
                
            if fail_on_missing_files:
                raise Exception('Missing image entry: \'{}\''.format(entry))
                
        # If it's a directory, recursively scan it for images
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
        
    assert type(description) == str,\
        'Expected \'description\' argument to be a str, not a {}'.format(type(description))
        
    assert type(image_column) == str,\
        'Expected \'image_column\' argument to be a str, not a {}'.format(type(image_column))
        
    
    # Get the start time of this operation
    t0 = time.time()
    
    # Ensure a workspace ID (possibly default)
    workspace_id = workspace_id or self.user_info['tenant_id']
        
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