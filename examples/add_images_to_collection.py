# -*- coding: utf-8 -*-
# Copyright 2021 Zegami Ltd

"""Example for adding images to an existing collection."""

from zegami_sdk.client import ZegamiClient
from zegami_sdk.source import UploadableSource

WORKSPACE_ID = ''
COLLECTION_ID = ''

zc = ZegamiClient()

workspace = zc.get_workspace_by_id(WORKSPACE_ID)
collection = workspace.get_collection_by_id(COLLECTION_ID)

# New data file, should be in same format as original file
supplementary_data_file = r"/path/to/data/file"

# extra images for first source
images_1 = r"/path/to/images"

upload_1 = UploadableSource('original', images_1, column_filename='ImageName')

collection.add_images(upload_1, supplementary_data_file)
