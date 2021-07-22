# -*- coding: utf-8 -*-
"""
Created on Fri Apr 23 16:37:17 2021.

@author: dougl
"""

import os
from pathlib import Path
import uuid

import requests


def _ensure_token(self, username, password, token, allow_save_token):
    """Tries the various logical steps to ensure a login token is set.

    Will use username/password if given, but will fallback on potentially
    saved token files.
    """
    # Potential location of locally saved token
    local_token_path = os.path.join(Path.home(), self.TOKEN_NAME)

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
        if os.path.exists(local_token_path):
            with open(local_token_path, 'r') as f:
                self.token = f.read()
            print('Used token from \'{}\'.'.format(local_token_path))
        else:
            raise ValueError('No username & password or token was given, '
                             'and no locally saved token was found.')


def _get_token(ZC, username, password):
    """Gets the client's token using a username and password."""

    url = '{}/oauth/token/'.format(ZC.HOME)
    data = {'username': username, 'password': password, 'noexpire': True}

    r = requests.post(url, json=data)

    if r.status_code != 200:
        raise Exception(f'Couldn\'t set token, bad response ({r.status_code})'
                        '\nWas your username/password correct?')

    j = r.json()

    return j['token']


def _check_status(response, is_async_request=False):
    """Checks the response for a valid status code.

    If allow is set to True, doesn't throw an exception.
    """
    code = response.status if is_async_request else response.status_code

    assert response.ok, 'Bad request response ({}): {}\n\nbody:\n{}'.format(code, response.reason, response.text)


def _auth_get(self, url, return_response=False, **kwargs):
    """Syncronous GET request. Used as standard over async currently.

    If return_response == True, the response object is returned rather than
    its .json() output.

    Any additional kwargs are forwarded onto the requests.get().
    """
    r = requests.get(url, headers=self.headers, **kwargs)
    self._check_status(r, is_async_request=False)
    return r if return_response else r.json()


def _auth_delete(self, url, **kwargs):
    """Syncronous DELETE request. Used as standard over async currently.

    Any additional kwargs are forwarded onto the requests.delete().
    """
    resp = requests.delete(url, headers=self.headers, **kwargs)
    self._check_status(resp, is_async_request=False)
    return resp


def _auth_post(self, url, body, return_response=False, **kwargs):
    """Syncronous POST request. Used as standard over async currently.
    If return_response == True, the response object is returned rather than
    its .json() output.
    Any additional kwargs are forwarded onto the requests.post().
    """
    r = requests.post(url, body, headers=self.headers, **kwargs)
    self._check_status(r, is_async_request=False)
    return r if return_response else r.json()


def _auth_put(self, url, body, return_response=False, **kwargs):
    """Syncronous PUT request. Used as standard over async currently.
    If return_response == True, the response object is returned rather than
    its .json() output.
    Any additional kwargs are forwarded onto the requests.put().
    """
    r = requests.put(url, body, headers=self.headers, **kwargs)
    self._check_status(r, is_async_request=False)
    return r if return_response else r.json()


def _obtain_signed_blob_storage_url(self, workspace_id):
    """Obtain a signed blob storage url.
    Returns:
        [str]: blob storage url
        [str]: blob storage id
    """
    blob_url = f'{self.HOME}/{self.API_1}/project/{workspace_id}/signed_blob_url'
    blob_id = str(uuid.uuid4())
    response = self._auth_post(blob_url, body=None, json={"ids": [blob_id]}, return_response=True)
    data = response.json()
    url = data[blob_id]
    return url, blob_id


def _upload_to_signed_blob_storage_url(data, url, mime_type, headers=None, **kwargs):
    """Upload data to an already obtained blob storage url."""
    if url.startswith("/"):
        url = f'https://storage.googleapis.com{url}'
    headers = {'Content-Type': mime_type}
    # this header is required for the azure blob storage
    # https://docs.microsoft.com/en-us/rest/api/storageservices/put-blob
    if 'windows.net' in url:
        headers['x-ms-blob-type'] = 'BlockBlob'
    response = requests.put(url, data=data, headers=headers, **kwargs)
    assert response.status_code == 201
