# -*- coding: utf-8 -*-
# Copyright 2022 Zegami Ltd

"""Add explainability map example."""

from zegami_sdk import nodes
from zegami_sdk.client import ZegamiClient

WORKSPACE_ID = ''
COLLECTION_ID = ''
NEW_SOURCE_NAME = ''

zc = ZegamiClient("", "")

workspace = zc.get_workspace_by_id(WORKSPACE_ID)
collection = workspace.get_collection_by_id(COLLECTION_ID)
print(collection)

# only works for v2 collection
source = collection.sources[0]
augment_imageset_id = source._data.get('augment_imageset_id')

collection_group = [
    'source_' + NEW_SOURCE_NAME,
    'collection_' + COLLECTION_ID
]

# create explainability map node
resp = nodes.add_node(
    zc,
    workspace,
    'explainability_map',
    {
        "model_name": "feature_extractor.h5",
    },
    'imageset',
    imageset_parents=augment_imageset_id,
    name="explainability map node",
    node_group=collection_group,
    processing_category='upload'
)
explainability_map_node = resp.get('imageset')

# add a new source with root imageset as the explainability map node
collection.add_source(NEW_SOURCE_NAME, explainability_map_node.get('id'))
