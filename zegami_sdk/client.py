# -*- coding: utf-8 -*-
# Copyright 2021 Zegami Ltd

"""base client functionality."""

from .util import (
    _auth_delete,
    _auth_get,
    _auth_post,
    _auth_put,
    _check_status,
    _create_blobstore_session,
    _create_zegami_session,
    _ensure_token,
    _get_token,
    _get_token_name,
    _obtain_signed_blob_storage_urls,
    _upload_to_signed_blob_storage_url
)
from .workspace import Workspace

DEFAULT_HOME = 'https://zegami.com'


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

    HOME = 'https://zegami.com'
    API_0 = 'api/v0'
    API_1 = 'api/v1'

    _auth_get = _auth_get
    _auth_post = _auth_post
    _auth_put = _auth_put
    _auth_delete = _auth_delete
    _create_zegami_session = _create_zegami_session
    _create_blobstore_session = _create_blobstore_session
    _ensure_token = _ensure_token
    _get_token_name = _get_token_name
    _get_token = _get_token
    _check_status = staticmethod(_check_status)
    _obtain_signed_blob_storage_urls = _obtain_signed_blob_storage_urls
    _upload_to_signed_blob_storage_url = _upload_to_signed_blob_storage_url
    _zegami_session = None
    _blobstore_session = None

    def __init__(self, username=None, password=None, token=None, allow_save_token=True, home=DEFAULT_HOME):
        # Make sure we have a token
        self.HOME = home
        self._ensure_token(username, password, token, allow_save_token)

        # Initialise a requests session
        self._create_zegami_session()
        self._create_blobstore_session()

        # Get user info, workspaces
        self._refresh_client()

        # Welcome message
        try:
            print('Client initialized successfully, welcome {}.\n'.format(self.name.split(' ')[0]))
        except Exception:
            pass

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

    def __init__(self, username=None, password=None, token=None, allow_save_token=True,
                 home='https://staging.zegami.com'):
        super().__init__(username, password, token, allow_save_token, home=home)
