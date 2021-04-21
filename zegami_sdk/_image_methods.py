# -*- coding: utf-8 -*-
"""
Created on Fri Mar 26 15:30:20 2021

@author: dougl
"""

import pandas as pd
import requests
from PIL import Image
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


def get_image_urls(self, collection, rows, source=None):
    '''
    Gets the URLs of every row of metadata provided. Can be a subset of
    all metadata obtained with 'get_rows_by_tag()', etc.
    Rows can be a pd.DataFrame, a list of ints, or an int.
    
    If the collection is a multi-source image collection, optionally provide
    the source index to retrieve. By default, the first source is retrieved.
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
    imageset = self._get_imageset(collection, source)
        
    # Convert from imageset space to rowspace
    lookup = self._get_image_meta_lookup(collection, source)
        
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
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        
        futures = [ex.submit(download_single, i, u) for i, u in enumerate(urls)]
    
        images = {}
        for f in as_completed(futures):
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


def list_image_sources(ZC, collection, return_dicts=False, hide_warning=False):
    '''
    Lists the image sources available in the provided collection. Specifying
    return_dicts=True returns the results as a list of source dictionaries
    rather than printing the results to the terminal.
    
    If it is an old-style collection without sources, returns [] if returning
    dicts. Also warns the user of they are using the old format. Use
    hide_warning=True to suppress this.
    '''
    
    version = ZC._extract_version(collection)
    
    if version < 2:
        
        if not hide_warning:
            print('\nNo image sources available, this is an old-style collection '\
                  'that does not operate on \'sources\'')
                
        return []
    
    assert 'image_sources' in collection.keys(),\
        'Expected to find \'image_sources\' in {}'.format(collection)
    
    srcs = collection['image_sources']
    
    if return_dicts:
        return srcs
    
    print('\nAvailable image sources:')
    for i, s in enumerate(srcs):
        print('{}\t[{}]:\t{}'.format(i, s['imageset_id'], s['name']))
    print('')
        