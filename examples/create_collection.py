# -*- coding: utf-8 -*-
# Copyright 2021 Zegami Ltd

"""Create collection example."""

from zegami_sdk.client import ZegamiClient
from zegami_sdk.source import UploadableSource

WORKSPACE_ID = ''
HOME = 'https://zegami.com'

zc = ZegamiClient(home=HOME)

workspace = zc.get_workspace_by_id(WORKSPACE_ID)

data_file = r"path/to/data"
images = r"path/to/images"

upload = UploadableSource('source_name', images, 'name')
workspace.create_collection('coll_name', upload, data_file)
