# coding: utf-8
#
# Copyright 2017 Zegami Ltd

"""SDK Integration Authentication tests."""

import sys
import os
from unittest.mock import patch
import unittest

from zegami_sdk import util
import requests_mock

from zegami_sdk.client import ZegamiClient
from pathlib import Path
import io

from .helper import guess_data_mimetype


class TestHelper(unittest.TestCase):

    def test_no_libmagic_guess_data_mimetype(self):
        """Force import error and check fallback mimetype set."""
        test_output = io.StringIO()
        sys.stdout = test_output
        sys.modules.pop('magic')  # Unload the magic module
        with patch('sys.path', []):  # Reload it with empty path
            self.assertEqual(
                guess_data_mimetype('mock_data'),
                'application/octet-stream'
            )  # Check fallback mimetype is used
        sys.stdout = sys.__stdout__
        test_output_stdout = test_output.getvalue()
        assert 'WARNING:' in test_output_stdout
        assert 'libmagic' in test_output_stdout

    def test_libmagic_guess_data_mimetype(self):
        """Force import error and check fallback mimetype set"""
        self.assertEqual(
            guess_data_mimetype('mock_data'),
            'text/plain'
        )


class TestZegamiClientMock(ZegamiClient):

    TOKEN_NAME = 'mock.zegami.token'
    HOME = 'https://mockzegami.com'


class TestSdkUtil(unittest.TestCase):

    @requests_mock.Mocker()
    def setUp(self, m):
        m.post('https://mockzegami.com/oauth/token/', json={'token': 'asdkjsajgfdjfsda'})
        m.get('https://mockzegami.com/oauth/userinfo/', json={'projects': []})
        super().setUp()
        self.username = 'testUser'
        self.password = 'passWord'
        self.local_token_path = os.path.join(Path.home(), 'mock.zegami.token')
        self.url = TestZegamiClientMock(username=self.username, password=self.password)

    def tearDown(self):
        os.remove(self.local_token_path)

    @requests_mock.Mocker()
    def test_get_token(self, m):
        m.post('https://mockzegami.com/oauth/token/', json={'token': 'asdkjsajgfdjfsda'})
        token = util._get_token(self.url, username=self.username, password=self.password)
        self.assertTrue(token)
        self.assertNotEqual(os.path.getsize(self.local_token_path), 0)

    def test_ensure_token_user_login(self):
        with requests_mock.Mocker() as m:
            m.post('https://mockzegami.com/oauth/token/', json={'token': 'asdkjsajgfdjfsda'})
            util._ensure_token(
                self.url, username=self.username, password=self.password,
                token=None, allow_save_token=True
            )
            self.assertNotEqual(os.path.getsize(self.local_token_path), 0)
