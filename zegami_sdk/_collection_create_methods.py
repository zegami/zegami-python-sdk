
import os
from pathlib import Path
import pandas as pd
import time


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
    
    
def make_source(name, dataset_column, paths, imageset_type='file'):
    '''
    Create a source for use in creating v2 collections (multi/single-source).
    
    name : Name of the source
    
    dataset_column : The name of the column to find the filenames of uploaded
        images. For simplicity, if you separate your sources into their own
        subdirectories, you can give them all the same filename, and hence
        reference the same filename column (could just be called 'Filename').
        As long as the subdirectory is unique for each source, this will work.
    
    As an example of creating a triplet of 'R', 'G' and 'B' sources, create an
    'images/' directory in your project, and within this create a folder for
    each intended source. Then make_source() for each:
        
    r_source = make_source(
        name = 'R',
        dataset_column = 'Filename',
        paths = ['my_project/images/R'], # The subdirectory of all my R images
        imageset_type = 'file'
    )
    
    g_source = make_source(
        name = 'G',
        dataset_column = 'Filename',
        paths = ['my_project/images/G'], # The subdirectory of all my G images
        imageset_type = 'file'
    )
    
    b_source = make_source(
        name = 'B',
        dataset_column = 'Filename',
        paths = ['my_project/images/B'], # The subdirectory of all my B images
        imageset_type = 'file'
    )
    '''
    
    s = _validate_source({ name, dataset_column, imageset_type, paths })
    
    return s

    
def _validate_source(s):
    '''
    Takes a dictionary intended to be a source and returns a checked, clean
    version.
    '''
    
    expected_imageset_types = ['file', 'url', 'azure_storage_container']
        
    assert type(s) == dict, 'Expected source {} to be a dict, not a {}'\
        .format(s, type(s))
        
    # Check the name
    assert 'name' in s.keys(), 'Expected source {} to contain the key '\
        '\'source_name\''.format(s)
        
    assert s['name'], 'Expected \'source_name\' of {} to not have a '\
        'None-type value'.format(s)
        
    # Check the dataset column
    assert 'dataset_column' in s.keys(), 'Missing \'dataset_column\' '\
        'in source {}'.format(s)
        
    assert type(s['dataset_column']) == str, 'Expected \'dataset_column\' '\
        'in source {} to be a str'.format(s)
        
    assert 'imageset_type' in s.keys(), 'Missing \'imageset_type\' '\
        'in source {}'.format(s)
        
    assert s['imageset_type'] in expected_imageset_types,\
        '\'imageset_type \'{}\' should be one of: {}'\
        .format(s['imageset_type'], expected_imageset_types)
        
    assert 'paths' in s.keys(), 'Missing \'paths\' in source {}'.format(s)
    
    # If a single path is given, cast to a list
    if type(s['paths']) == str:
        
        s['paths'] = [s['paths']]
        
        print('source \paths\' argument: Casted single path to a list: {}'\
              .format(s['paths']))
    
    assert type(s['paths']), 'Expected \'paths\' to be a list, not a {} in '\
        'source {}'.format(type(s['paths']), s)
        
    # Check for valid path locations
    if s['imageset_type'] == 'file':
        for p in s['paths']:
            assert os.path.exists(p), 'Source path \'{}\' not found'\
                .format(p)
        
    return {
        'name' : s['name'],
        'dataset_column' : s['dataset_column'],
        'imageset_type' : s['imageset_type'],
        'paths' : s['paths']
    }
        
        
def _create_v1_collection_config(self, name, description='', enable_clustering=True):
    '''
    Creates a configuration for creating a v1 collection. These do no
    support multiple image sources.
    '''
    
    # Create the config
    cfg = {
        'name' : str(name),
        'description' : str(description),
        'enable_clustering' : enable_clustering,
    }
        
        
def _create_v2_collection_config(self, name, sources, description='', enable_clustering=True):
    '''
    Creates a configuration for creating a v2 collection. These support
    multi-image-sources.
    
    To create a source, use make_source(). This creates simple dictionaries
    describing different aspects of the same data rows. For example, you may
    want to have a 3 images sources per row of data, with each source
    representing the R, G and B channels of your image data.
    '''
    
    assert name, 'Expected name not to be None-like: \'{}\''.format(name)
    
    assert type(sources) == list, 'Expected \'sources\' to be a list, not '\
        '{}. If you want one source, use sources=[your_source]'\
        .format(type(sources))
        
    assert len(sources) > 0, 'Expected at least one source'
    
    assert enable_clustering in [True, False],\
        'Expected \'enable_clustering\ to be True or False, not {}'\
        .format(enable_clustering)
    
    # Create the config
    cfg = {
        'name' : str(name),
        'description' : str(description),
        'enable_clustering' : enable_clustering,
        'version' : 2,
        'image_sources' : []
    }
    
    # Create the sources to tack on
    used_names = []
    for s in sources:
        
        assert s['name'] not in used_names, 'Source name \'{}\' was already '\
            'used. Make all sources have unique names'\
            .format(s['name'])
            
        # Validate and clean each provided source
        cfg['image_sources'].append(_validate_source(s))
            
        # Add the parsed source to the list of already-used names
        used_names.append(s['name'])
    
    
def _execute_create_collection(self, config, workspace_id=None):
    
    workspace_id = workspace_id or self.active_workspace_id
    
    # Generate the collection creation URL
    url = '{}/{}/project/{}/collections/'\
        .format(self.HOME, self.API_0, workspace_id)
        
    # Rough checks on config
    assert type(config) == dict, 'Expected a dictionary for a config'
    assert 'name' in config.keys(), 'No \'name\' in config: {}'.format(config)
    assert 'collection_version' in config.keys(), 'No \'collection_version\' '\
        'in config: {}'.format(config)
        
    # Get some basic collection information
    coll_version = config['collection_version']
    coll_description = config['description'] if 'description' in config.keys()\
        else ''