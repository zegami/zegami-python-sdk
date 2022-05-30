# -*- coding: utf-8 -*-
# Copyright 2022 Zegami Ltd

"""Upload a custom model to run explainability map and clustering on duplicated collection."""

import time

from zegami_sdk import nodes
from zegami_sdk.client import ZegamiClient

WORKSPACE_ID = ''
OLD_COLLECTION_ID = ''
MODEL_PATH = ''
MODEL_NAME = ''
WIDTH = 164
HEIGHT = 164
zc = ZegamiClient("", "")

# Initialise Zegami inferface
print('Initialising Zegami interface\n')

workspace = zc.get_workspace_by_id(WORKSPACE_ID)
old_coll = workspace.get_collection_by_id(OLD_COLLECTION_ID)
model_blob_path = '{}/{}'.format(OLD_COLLECTION_ID, MODEL_NAME)

# Upload custom model to the workspace.
print('Upload model to the workspace \n')

with open(MODEL_PATH, "rb") as data:
    workspace.create_storage_item(data, item_name=model_blob_path)

# Duplicating the collection
print('Duplicating collection {}\n'.format(old_coll.name))

new_coll_name = 'Duplicating {} with model {}'.format(old_coll.name, MODEL_NAME)
resp = old_coll.duplicate(new_coll_name)
new_coll_id = resp['new_collection_id']
# wait until new collection duplication is finished
while True:
    new_coll = workspace.get_collection_by_id(new_coll_id)
    if new_coll.status['status'] == 'completed':
        break
    time.sleep(60)
source = new_coll.sources[0]

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
new_coll.add_explainability_map_source(data)

# Add clustering
print('Adding clustering to {}\n'.format(new_coll.name))

scaled_imageset_id = source._data.get('scaled_imageset_id')
join_dataset_id = source._data.get('imageset_dataset_join_id')

collection_group_first_source = [
    'source_' + source.name,
    'collection_' + new_coll_id
]

resp = nodes.add_node(
    zc,
    workspace,
    'custom_feature_extraction',
    {
        "MODEL_NAME": model_blob_path,
        "greyscale": True,
        "width": WIDTH,
        "height": HEIGHT,
    },
    'imageset',
    imageset_parents=scaled_imageset_id,
    name="custom feature extraction node",
    node_group=collection_group_first_source,
    processing_category='image_clustering'
)
custom_feature_extraction_node = resp.get('imageset')

resp = nodes.add_node(
    zc,
    workspace,
    'cluster',
    {
        'out_column_title_prefix': '{}_'.format(MODEL_NAME),
    },
    dataset_parents=custom_feature_extraction_node.get('id'),
    name="{} custom feature extraction similarity".format(MODEL_NAME),
    node_group=collection_group_first_source,
    processing_category='image_clustering'
)
cluster_node = resp.get('dataset')

resp = nodes.add_node(
    zc,
    workspace,
    'mapping',
    {},
    dataset_parents=[cluster_node.get('id'), join_dataset_id],
    name="{} custom feature extraction mapping".format(MODEL_NAME),
    node_group=collection_group_first_source,
    processing_category='image_clustering'
)
mapping_node = resp.get('dataset')

output_dataset_id = new_coll._data.get('output_dataset_id')
resp = nodes.add_parent(
    zc,
    workspace,
    output_dataset_id,
    mapping_node.get('id')
)

print('Ended processing: {}\n'.format(MODEL_NAME))
