# -*- coding: utf-8 -*-
"""
Created on Fri Feb 26 15:05:31 2021

@author: dougl
"""

import pandas as pd
from io import BytesIO

    
def get_rows(self, collection):
    '''
    Gets the rows in a collection as a Dataframe.
    '''
    
    # Get the workspace ID
    workspace_id = self._extract_workspace_id(collection)
    
    url = '{}/{}/project/{}/datasets/{}/file'.format(
        self.HOME, self.API_0, workspace_id, self._extract_dataset_id(collection))
    
    response = self._auth_get(url, return_response=True)
    
    assert response.status_code == 200,\
        'get_rows: _auth_get({}) did not return a 200 response ({})'\
        .format(url, response.status_code)
        
    tsv_bytes = BytesIO(response.content)
    
    try:
        df = pd.read_csv(tsv_bytes, sep='\t')
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