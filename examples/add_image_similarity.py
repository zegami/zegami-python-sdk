# -*- coding: utf-8 -*-
# Copyright 2022 Zegami Ltd

"""Add another image similarity branch."""

from zegami_sdk import nodes
from zegami_sdk.client import ZegamiClient

WORKSPACE_ID = ''
COLLECTION_ID = ''
NAME = 'Image Similarity 2'
col_name = NAME.replace(' ', '_').lower()

zc = ZegamiClient()


workspace = zc.get_workspace_by_id(WORKSPACE_ID)
collection = workspace.get_collection_by_id(COLLECTION_ID)
collection_group = 'collection_' + COLLECTION_ID
print(collection)

# Add new clustering node
if collection.version == 1:
    dataset_id = collection._data.get('dataset_id')
    scaled_imageset_id = collection._data.get('scaled_imageset_id')
    join_dataset_id = collection._data.get('imageset_dataset_join_id')
else:
    # v2
    source = collection.sources[0]
    dataset_id = collection._data.get('dataset_id')
    scaled_imageset_id = source._data.get('scaled_imageset_id')
    join_dataset_id = source._data.get('imageset_dataset_join_id')

print('adding feature extraction node')
# create feature extraction node
resp = nodes.add_node(
    zc,
    workspace,
    'image_feature_extraction',
    {

    },
    imageset_parents=[scaled_imageset_id],
    type='imageset',
    name="Feature extraction " + NAME,
    node_group=collection_group,
    processing_category='image_clustering'
)
features_node = resp.get('imageset')
print('\nadded feature extraction node', features_node)

# create clustering node
resp = nodes.add_node(
    zc,
    workspace,
    'cluster',
    {
        "columns_order": [
            1002,
            1003
        ],
        "out_column_titles": [
            NAME + " x",
            NAME + " y"
        ],
        "out_columns": [
            col_name + "x",
            col_name + "y"
        ]
    },
    dataset_parents=features_node.get('id'),
    name="clustering" + NAME,
    node_group=collection_group,
    processing_category='image_clustering'
)
cluster_node = resp.get('dataset')
print('\nadded cluster node', cluster_node)

# create mapping node
resp = nodes.add_node(
    zc,
    workspace,
    'mapping',
    {},
    dataset_parents=[cluster_node.get('id'), join_dataset_id],
    name=NAME + " mapping",
    node_group=collection_group,
    processing_category='image_clustering'
)
mapping_node = resp.get('dataset')
print('\nadded mapping node', mapping_node)

# Include output in collection output
output_dataset_id = collection._data.get('output_dataset_id')
resp = nodes.add_parent(
    zc,
    workspace,
    output_dataset_id,
    mapping_node.get('id')
)
