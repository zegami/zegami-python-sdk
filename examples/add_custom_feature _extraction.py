# -*- coding: utf-8 -*-
# Copyright 2022 Zegami Ltd

"""Add custom feature extraction example."""

from zegami_sdk import nodes
from zegami_sdk.client import ZegamiClient

WORKSPACE_ID = ''
COLLECTION_ID = ''

zc = ZegamiClient("", "")

workspace = zc.get_workspace_by_id(WORKSPACE_ID)
collection = workspace.get_collection_by_id(COLLECTION_ID)
collection_group = 'collection_' + COLLECTION_ID
print(collection)

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

resp = nodes.add_node(
    zc,
    workspace,
    'custom_feature_extraction',
    {
        "model_name": "feature_extractor.h5",
        "greyscale": True,
        "width": 164,
        "height": 164,
    },
    'imageset',
    imageset_parents=scaled_imageset_id,
    name="custom feature extraction node",
    node_group=collection_group,
    processing_category='image_clustering'
)
custom_feature_extraction_node = resp.get('imageset')

# Add new clustering node
resp = nodes.add_node(
    zc,
    workspace,
    'cluster',
    {
        'exclude': [''],  # list of column names as found in tsv files (not Ids)
        'out_columns': ['custom_model_x', 'custom_model_y'],
        'out_column_titles': ['Custom_Model_X', 'Custom_Model_Y'],
        'columns_order': [1100, 1101]  # important to set this to a unique value
    },
    dataset_parents=custom_feature_extraction_node.get('id'),
    name="custom feature extraction similarity",
    node_group=collection_group,
    processing_category='image_clustering'
)
cluster_node = resp.get('dataset')

# create mapping node
resp = nodes.add_node(
    zc,
    workspace,
    'mapping',
    {},
    dataset_parents=[cluster_node.get('id'), join_dataset_id],
    name="custom feature extraction mapping",
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
