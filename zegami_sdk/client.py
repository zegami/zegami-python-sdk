# -*- coding: utf-8 -*-
"""
Zegami Ltd.
Apache 2.0
"""


from .workspace import Workspace
from .util import (
    _auth_get,
    _auth_post,
    _auth_put,
    _auth_delete,
    _ensure_token,
    _get_token,
    _check_status
)


class ZegamiClient():
    """This client acts as the root for browsing your Zegami data.

    It facilitates making authenticated requests using your token, initially
    generated with login credentials. After logging in once, subsequent
    credentials should typically not be required, as the token is saved
    locally (zegami.token in your root OS folder).

    Use zc.show_workspaces() to see your available workspaces. You can access
    your workspaces using either zc.get_workspace_by...() or by directly
    using workspace = zc.workspaces[0] ([1], [2], ...). These then act as
    controllers to browse collections from. Collections in turn act as
    controllers to browse data from.
    """

    TOKEN_NAME = 'zegami.token'
    HOME = 'https://zegami.com'
    API_0 = 'api/v0'
    API_1 = 'api/v1'

    _auth_get = _auth_get
    _auth_post = _auth_post
    _auth_put = _auth_put
    _auth_delete = _auth_delete
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
            print('\nInitialized successfully, welcome {}.'.format(self.name.split(' ')[0]))
        except:
            pass

    @property
    def headers():
        pass

    @headers.getter
    def headers(self):
        return {'Authorization': 'Bearer {}'.format(self.token)}

    @property
    def user_info():
        pass

    @user_info.getter
    def user_info(self):
        if not self._user_info:
            self._refresh_client()
        assert self._user_info, 'user_info not set, even after a client refresh'
        return self._user_info

    @property
    def name():
        pass

    @name.getter
    def name(self):
        assert self._user_info, 'Trying to get name from a non-existent user_info'
        assert 'name' in self._user_info.keys(),\
            'Couldn\'t find \'name\' in user_info: {}'.format(self._user_info)
        return self._user_info['name']
    
    @property
    def email():
        pass
    
    @email.getter
    def email(self):
        assert self._user_info, 'Trying to get email from a non-existent user_info'
        assert 'email' in self._user_info.keys(),\
            'Couldn\'t find \'email\' in user_info: {}'.format(self._user_info)
        return self._user_info['email']

    @property
    def workspaces():
        pass

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
        raise ValueError('Couldn\'t find a workspace with the name \'{}\''.format(name))

    def get_workspace_by_id(self, id):
        ws = self.workspaces
        for w in ws:
            if w.id == id:
                return w
        raise ValueError('Couldn\'t find a workspace with the ID \'{}\''.format(id))

    def show_workspaces(self):
        ws = self.workspaces
        assert ws, 'Invalid workspaces obtained'
        print('\nWorkspaces ({}):'.format(len(ws)))
        for w in ws:
            print('{} : {}'.format(w.id, w.name))

    def _refresh_client(self):
        """Refreshes user_info and workspaces."""
        url = '{}/oauth/userinfo/'.format(self.HOME)
        self._user_info = self._auth_get(url)
        self._workspaces = [Workspace(self, w) for w in self._user_info['projects']]


class _ZegamiStagingClient(ZegamiClient):

    TOKEN_NAME = 'staging.zegami.token'
    HOME = 'https://staging.zegami.com'

    def __init__(self, username=None, password=None, token=None, allow_save_token=True):
        super().__init__(username, password, token, allow_save_token)
