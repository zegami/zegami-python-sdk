# -*- coding: utf-8 -*-
# Copyright 2022 Zegami Ltd

""" Replace datafile of collection."""
from zegami_sdk.client import ZegamiClient

# Input Variables

WORKSPACE_ID = ''
COLLECTION_ID = ''
datafile = r"path/to/file"
is_production = True  # Set as False if the collection is on staging

# Replace data

if is_production:
    HOME = "https://zegami.com"
else:
    HOME = "https://staging.zegami.com"

zc = ZegamiClient(home=HOME)
workspace = zc.get_workspace_by_id(WORKSPACE_ID)
collection = workspace.get_collection_by_id(COLLECTION_ID)

collection.replace_data(datafile)
