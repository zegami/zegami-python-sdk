# -*- coding: utf-8 -*-
"""
Created on Fri Mar 26 15:39:51 2021

@author: dougl
"""

import requests


def _auth_get(self, url, return_response=False, **kwargs):
    '''
    Syncronous GET request. Used as standard over async currently.
    
    If return_response == True, the response object is returned rather than
    its .json() output.
    
    Any additional kwargs are forwarded onto the requests.get().
    '''
    
    r = requests.get(url, headers=self.headers, **kwargs)
    
    self._check_status(r, is_async_request=False)
    
    return r if return_response else r.json()


def _auth_post(self, url, body, return_response=False, **kwargs):
    '''
    Syncronous POST request. Used as standard over async currently.
    
    If return_response == True, the response object is returned rather than
    its .json() output.
    
    Any additional kwargs are forwarded onto the requests.post().
    '''
            
    r = requests.post(url, body, headers=self.headers, **kwargs)
    
    self._check_status(r, is_async_request=False)
        
    return r if return_response else r.json()
    
    
def _get_token(ZC, username, password):
    '''
    Gets the client's token using a username and password.
    '''
    
    url = '{}/oauth/token/'.format(ZC.HOME)
    data = { 'username' : username, 'password' : password, 'noexpire' : True }
    
    r = requests.post(url, json=data)
    
    if r.status_code != 200:
        raise Exception(f'Couldn\'t set token, bad response ({r.status_code})'
                        '\nWas your username/password correct?')
        
    j = r.json()
    
    return j['token']
    

def _check_status(response, is_async_request=False):
    '''
    Checks the response for a valid status code. If allow is set to True,
    doesn't throw an exception.
    '''
    
    code = response.status if is_async_request else response.status_code
    
    assert code == 200, 'Bad request response ({}): {}\n\nbody:\n{}'.format(code, response.reason, response.text)