# -*- coding: utf-8 -*-
# Copyright 2022 Zegami Ltd

"""Add explainability map example."""

from zegami_sdk import nodes
from zegami_sdk.client import ZegamiClient

WORKSPACE_ID = ''
COLLECTION_ID = ''
NEW_SOURCE_NAME = ''
MODEL_PATH = ''
MODEL_NAME = ''

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

# Optional: Upload custom model to the workspace.
# Only need to upload the same model once and all collections in workspace can access it.
with open(MODEL_PATH, "rb") as data:
    workspace.create_storage_item(data, item_name=MODEL_NAME)

# create explainability map node
resp = nodes.add_node(
    zc,
    workspace,
    'explainability_map',
    {   
        "file_name": MODEL_NAME, # Optional: Name of the model in workspace storage. This will overwrite the tensorflow model param.
        "model_name": 'Xception',  # Optional: Name of the tensorflow model. Default is ResNet50.
        "width": 224, # Optional: Width value to pre process the image before inputting into the model.
        "height": 224, # Optional: Height value to pre process the image before inputting into the model.
        "last_conv_layer_name": '', # Optional: Name of the last convolutional layer.
        "class_index": '', # Optional: Index of class to evaluate activation maps with respect to. If left blank, the top predicted class is used for each image.
        "class_index": 0.5, # Optional: Blend weight for combining heatmap to image. Default is 0.4.
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
