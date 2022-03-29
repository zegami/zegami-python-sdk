# -*- coding: utf-8 -*-
# Copyright 2022 Zegami Ltd

"""Enable Absolute View in collection."""

from zegami_sdk.client import ZegamiClient

WORKSPACE_ID = ''
COLLECTION_ID = ''

zc = ZegamiClient()
w = zc.get_workspace_by_id(WORKSPACE_ID)
c = w.get_collection_by_name(COLLECTION_ID)

c.set_userdata({'enable_absolute_view': True})
