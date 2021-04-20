# -*- coding: utf-8 -*-
"""
Created on Fri Mar 26 15:39:51 2021

@author: dougl
"""

import requests


def _auth_get(self, url, allow_bad=False, return_response=False):
    '''
    Syncronous GET request. Used as standard over async currently.
    
    If allow_bad == True, only a warning will be printed on bad responses.
    
    If return_response == True, the response object is returned rather
    than its .json() output.
    '''
    
    r = requests.get(url, headers=self.headers)
    
    if not self._check_status(r, allow_bad or return_response, is_async_request=False):
        if not return_response:
            return None
    
    return r if return_response else r.json()


def _auth_post(self, url, body, return_response=False, **kwargs):
    '''
    Syncronous POST. Used as standard over async currently.
    
    Can provide a body, and any 'requests.post()' kwargs will be forwarded.
    '''
            
    r = requests.post(url, body, headers=self.headers, **kwargs)
    
    if not self._check_status(r, is_async_request=False):
        if not return_response:
            return None
        
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
    

def _check_status(response, allow_bad=False, is_async_request=False):
    '''
    Checks the response for a valid status code. If allow is set to True,
    doesn't throw an exception.
    '''
    
    code = response.status if is_async_request else response.status_code
    
    if code != 200:
        if not allow_bad:
            raise Exception('Bad request response ({}): {}\n\nbody:\n{}'.format(code, response.reason, response.text))
        else:
            # print('Warning - bad response ({}) allowed through: {}'.format(code, response.reason))
            return False
    
    return True