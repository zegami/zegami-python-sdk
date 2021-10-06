# -*- coding: utf-8 -*-
# Copyright 2021 Zegami Ltd

"""Create collection example."""

from zegami_sdk.client import ZegamiClient

WORKSPACE_ID = ''
COLLECTION_ID = ''

zc = ZegamiClient()

workspace = zc.get_workspace_by_id(WORKSPACE_ID)
coll = workspace._create_empty_collection('sdk_empty_collection')

print(coll)
