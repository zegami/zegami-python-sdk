# -*- coding: utf-8 -*-
# Copyright 2022 Zegami Ltd

"""Duplicating a given collection, train and run explainability map and clustering on duplicated collection."""

import time

from zegami_sdk.client import ZegamiClient

WORKSPACE_ID = ''
OLD_COLLECTION_ID = ''
TRAINING_NAME = ''
zc = ZegamiClient("", "")

# Initialise Zegami inferface
print('Initialising Zegami interface\n')

workspace = zc.get_workspace_by_id(WORKSPACE_ID)
old_coll = workspace.get_collection_by_id(OLD_COLLECTION_ID)

# Duplicating the collection
print('Duplicating collection {}\n'.format(old_coll.name))

new_coll_name = 'Duplicating {} with model {}'.format(old_coll.name, TRAINING_NAME)
resp = old_coll.duplicate(new_coll_name)
new_coll_id = resp['new_collection_id']
# wait until new collection duplication is finished
while True:
    new_coll = workspace.get_collection_by_id(new_coll_id)
    if new_coll.status['status'] == 'completed':
        break
    time.sleep(60)
source = new_coll.sources[0]

# TODO: Training on the duplicated collection
MODEL_PATH = ''
MODEL_NAME = ''
WIDTH = 164
HEIGHT = 164
model_blob_path = '{}/{}'.format(OLD_COLLECTION_ID, MODEL_NAME)

# Upload custom model to the workspace.
print('Upload model to the workspace \n')

with open(MODEL_PATH, "rb") as data:
    workspace.create_storage_item(data, item_name=model_blob_path)

# Add explainability map
print('Adding explainbility map to {}\n'.format(new_coll.name))

augment_imageset_id = source._data.get('augment_imageset_id')
data = {
    'SOURCE_NAME': MODEL_NAME,
    'PARENT_IMAGESET_ID': augment_imageset_id,
    'SOURCE': {
        "file_name": model_blob_path,
        "width": WIDTH,
        "height": HEIGHT,
    }
}
new_coll.add_explainability(data)

# Add clustering
print('Adding clustering to {}\n'.format(new_coll.name))

clustering_data = {
    'FEATURE_EXTRACTION_SOURCE': {
        "MODEL_NAME": model_blob_path,
        "greyscale": True,
        "width": WIDTH,
        "height": HEIGHT,
    },
    'CLUSTERING_SOURCE': {
        'out_column_title_prefix': '{}_'.format(MODEL_NAME),
    }
}
new_coll.add_custom_clustering(clustering_data)

print('Ended processing: {}\n'.format(MODEL_NAME))
