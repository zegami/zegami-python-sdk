# -*- coding: utf-8 -*-

"""Download images from a collection with a specific tag."""

from zegami_sdk.client import ZegamiClient

WORKSPACE_ID = ''
COLLECTION_ID = ''
TAG = ''
USERNAME = ''
PASSWORD = ''

zc = ZegamiClient(username=USERNAME, password=PASSWORD)

workspace = zc.get_workspace_by_id(WORKSPACE_ID)
collection = workspace.get_collection_by_id(COLLECTION_ID)
rows = collection.get_rows_by_tags([TAG])
image_urls = collection.get_image_urls(rows)
collection.save_image_batch(image_urls, './images', extension='dcm', max_workers=20)
