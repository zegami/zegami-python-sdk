# -*- coding: utf-8 -*-
# Copyright 2022 Zegami Ltd

""" Replace datafile of collection."""

from zegami_sdk.client import ZegamiClient

WORKSPACE_ID = ''
COLLECTION_ID = ''

zc = ZegamiClient()
workspace = zc.get_workspace_by_id(WORKSPACE_ID)
collection = workspace.get_collection_by_id(COLLECTION_ID)

datafile = r"path/to/file"

collection.replace_data(datafile)
