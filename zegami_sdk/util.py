# -*- coding: utf-8 -*-
# Copyright 2021 Zegami Ltd

"""util methods."""


import os
from pathlib import Path
from urllib.parse import urlparse
import uuid

import requests
import urllib3

ALLOW_INSECURE_SSL = os.environ.get('ALLOW_INSECURE_SSL', False)


def __get_retry_adapter():
    retry_methods = urllib3.util.retry.Retry.DEFAULT_METHOD_WHITELIST.union(
        ('POST', 'PUT'))
    retry = urllib3.util.retry.Retry(
        total=10,
        backoff_factor=0.5,
        status_forcelist=[(502, 503, 504, 408)],
        method_whitelist=retry_methods
    )
    adapter = requests.adapters.HTTPAdapter(max_retries=retry)
    return adapter


def _create_zegami_session(self):
    """Create a session object to centrally handle auth and retry policy."""
    s = requests.Session()
    if ALLOW_INSECURE_SSL:
        s.verify = False
    s.headers.update({
        'Authorization': 'Bearer {}'.format(self.token),
        'Content-Type': 'application/json',
    })

    # Set up retry policy. Retry post requests as well as the usual methods.
    adapter = __get_retry_adapter()
    s.mount('http://', adapter)
    s.mount('https://', adapter)

    self._zegami_session = s


def _create_blobstore_session(self):
    """Session object to centrally handle retry policy."""
    s = requests.Session()
    if ALLOW_INSECURE_SSL:
        s.verify = False
    adapter = __get_retry_adapter()
    s.mount('http://', adapter)
    s.mount('https://', adapter)

    self._blobstore_session = s


def _get_token_name(self):
    url = urlparse(self.HOME)
    netloc = url.netloc
    prefix = netloc.replace('.', '_')
    return f'{prefix}.zegami.token'


def _ensure_token(self, username, password, token, allow_save_token):
    """Tries the various logical steps to ensure a login token is set.

    Will use username/password if given, but will fallback on potentially
    saved token files.
    """
    # Potential location of locally saved token
    local_token_path = os.path.join(Path.home(), self._get_token_name())

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


def _get_token(self, username, password):
    """Gets the client's token using a username and password."""
    url = '{}/oauth/token/'.format(self.HOME)
    data = {'username': username, 'password': password, 'noexpire': True}

    r = requests.post(url, json=data, verify=not ALLOW_INSECURE_SSL)

    if r.status_code != 200:
        raise Exception(f'Couldn\'t set token, bad response ({r.status_code}) Was your username/password correct?')

    j = r.json()

    return j['token']


def _check_status(response, is_async_request=False):
    """Checks the response for a valid status code.

    If allow is set to True, doesn't throw an exception.
    """
    if not response.ok:
        code = response.status if is_async_request else response.status_code
        response_message = 'Bad request response ({}): {}\n\nbody:\n{}'.format(
            code, response.reason, response.text
        )
        raise AssertionError(response_message)


def _auth_get(self, url, return_response=False, **kwargs):
    """Synchronous GET request. Used as standard over async currently.

    If return_response == True, the response object is returned rather than
    its .json() output.

    Any additional kwargs are forwarded onto the requests.get().
    """
    r = self._zegami_session.get(url, verify=not ALLOW_INSECURE_SSL, **kwargs)
    self._check_status(r, is_async_request=False)
    return r if return_response else r.json()


def _auth_delete(self, url, **kwargs):
    """Synchronous DELETE request. Used as standard over async currently.

    Any additional kwargs are forwarded onto the requests.delete().
    """
    resp = self._zegami_session.delete(
        url, verify=not ALLOW_INSECURE_SSL, **kwargs
    )
    self._check_status(resp, is_async_request=False)
    return resp


def _auth_post(self, url, body, return_response=False, **kwargs):
    """Synchronous POST request. Used as standard over async currently.

    If return_response == True, the response object is returned rather than
    its .json() output.
    Any additional kwargs are forwarded onto the requests.post().
    """
    r = self._zegami_session.post(
        url, body, verify=not ALLOW_INSECURE_SSL, **kwargs
    )
    self._check_status(r, is_async_request=False)
    return r if return_response else r.json()


def _auth_put(self, url, body, return_response=False, **kwargs):
    """Synchronous PUT request. Used as standard over async currently.

    If return_response == True, the response object is returned rather than
    its .json() output.
    Any additional kwargs are forwarded onto the requests.put().
    """
    r = self._zegami_session.put(
        url, body, verify=not ALLOW_INSECURE_SSL, **kwargs
    )
    self._check_status(r, is_async_request=False)
    return r if return_response else r.json()


def _obtain_signed_blob_storage_urls(self, workspace_id, id_count=1, blob_path=None):
    """Obtain a signed blob storage url.

    Returns:
        [dict]: blob storage urls
        [dict]: blob storage ids
    """
    blob_url = f'{self.HOME}/{self.API_1}/project/{workspace_id}/signed_blob_url'

    if blob_path:
        id_set = {"ids": [f'{blob_path}/{str(uuid.uuid4())}' for i in range(id_count)]}
    else:
        id_set = {"ids": [str(uuid.uuid4()) for i in range(id_count)]}

    response = self._auth_post(blob_url, body=None, json=id_set, return_response=True)
    data = response.json()
    urls = data
    return urls, id_set


def _upload_to_signed_blob_storage_url(self, data, url, mime_type, **kwargs):
    """Upload data to an already obtained blob storage url."""
    if url.startswith("/"):
        url = f'https://storage.googleapis.com{url}'
    headers = {'Content-Type': mime_type}
    # this header is required for the azure blob storage
    # https://docs.microsoft.com/en-us/rest/api/storageservices/put-blob
    if 'windows.net' in url:
        headers['x-ms-blob-type'] = 'BlockBlob'
    response = self._blobstore_session.put(
        url, data=data, headers=headers, verify=not ALLOW_INSECURE_SSL, **kwargs
    )
    assert response.ok
