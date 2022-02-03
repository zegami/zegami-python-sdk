# -*- coding: utf-8 -*-
# Copyright 2021 Zegami Ltd

"""Create collection example."""

from zegami_sdk.client import ZegamiClient
from zegami_sdk.source import UploadableSource
from zegami_sdk.source import UrlSource

WORKSPACE_ID = ''
HOME = 'https://zegami.com'

zc = ZegamiClient(home=HOME)

workspace = zc.get_workspace_by_id(WORKSPACE_ID)

data_file = r"path/to/data"
images = r"path/to/images"
url_template = 'https://example.com/images/{}?accesscode=abc3e20423423497'
image_fetch_headers = 'image/jpg'
column_name = 'id'

upload1 = UploadableSource('source_name', images, 'name')
upload2 = UrlSource('url_source', url_template, image_fetch_headers, column_name)
uploads = [upload1, upload2]
workspace.create_collection('test_sdk_url', uploads, data_file)
