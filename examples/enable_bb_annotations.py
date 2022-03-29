# -*- coding: utf-8 -*-
# Copyright 2022 Zegami Ltd

""" Enable bounding box annotations in collection.
    Set annotation classes in the UI."""

from zegami_sdk.client import ZegamiClient

WORKSPACE_ID = ''
COLLECTION_ID = ''

zc = ZegamiClient()
workspace = zc.get_workspace_by_id(WORKSPACE_ID)
collection = workspace.get_collection_by_id(COLLECTION_ID)

collection.set_userdata({'enable_bounding_box_annotations': True})
