# -*- coding: utf-8 -*-
"""
Zegami Ltd.
Apache 2.0
"""


from .workspace import Workspace
from .util import (
    _auth_get,
    _auth_post,
    _ensure_token,
    _get_token,
    _check_status
)


class ZegamiClient():
    
    HOME  = 'https://zegami.com'
    API_0 = 'api/v0'
    API_1 = 'api/v1'
    
    _auth_get = _auth_get
    _auth_post = _auth_post
    _ensure_token = _ensure_token
    _get_token = classmethod(_get_token)
    _check_status = staticmethod(_check_status)
    
    def __init__(self, username=None, password=None, token=None, allow_save_token=True):
        
        # Make sure we have a token
        self._ensure_token(username, password, token, allow_save_token)
        
        # Get user info, workspaces
        self._refresh_client()
        
        # Welcome message
        try:
            print('\nInitialized successfully, welcome {}.'\
                  .format(self.name.split(' ')[0]))
        except:
            pass
        
        
    @property
    def headers(): pass
    @headers.getter
    def headers(self):
        return { 'Authorization' : 'Bearer {}'.format(self.token) }
    
    
    @property
    def user_info(): pass
    @user_info.getter
    def user_info(self):
        if not self._user_info:
            self._refresh_client()
        assert self._user_info, 'user_info not set, even after a client refresh'
        return self._user_info
    
    
    @property
    def name(): pass
    @name.getter
    def name(self):
        assert self._user_info, 'Trying to get name from a non-existent user_info'
        assert 'name' in self._user_info.keys(),\
            'Couldn\'t find \'name\' in user_info: {}'.format(self._user_info)
        return self._user_info['name']
    
    
    @property
    def workspaces(): pass
    @workspaces.getter
    def workspaces(self):
        if not self._workspaces:
            self._refresh_client()
        assert self._workspaces, 'workspaces not set, even after a client refresh'
        return self._workspaces
    
    
    def get_workspace_by_name(self, name):
        ws = self.workspaces
        for w in ws:
            if w.name.lower() == name.lower():
                return w
        print('Couldn\'t find a workspace with the name \'{}\''.format(name))
    
    
    def get_workspace_by_id(self, id):
        ws = self.workspaces
        for w in ws:
            if w.id == id:
                return w
        print('Couldn\'t find a workspace with the ID \'{}\''.format(id))
    
    
    def show_workspaces(self):
        ws = self.workspaces
        assert ws, 'Invalid workspaces obtained'
        print('\nWorkspaces ({}):'.format(len(ws)))
        for w in ws:
            print('{} : {}'.format(w.id, w.name))
    
    
    def _refresh_client(self):
        ''' Refreshes user_info and workspaces. '''
        url = '{}/oauth/userinfo/'.format(self.HOME)
        self._user_info = self._auth_get(url)
        
        self._workspaces = [Workspace(self, w) for w in self._user_info['projects']]
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        