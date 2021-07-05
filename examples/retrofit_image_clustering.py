"""
Script to add image similarity clustering to a collection which predates the switch to using an output dataset.
Requires a collection to have a fully populated scaled_imageset, which not all older collections do.
Adds a new output dataset, and this field would need to be added to the collection document for this to work.
"""

from zegami_sdk.client import ZegamiClient
from zegami_sdk import nodes

WORKSPACE_ID = ''
COLLECTION_ID = ''

zc = ZegamiClient()


workspace = zc.get_workspace_by_id(WORKSPACE_ID)
collection = workspace.get_collection_by_id(COLLECTION_ID)
print(collection)

# Add new clustering node
dataset_id = collection._data.get('dataset_id')
scaled_imageset_id = collection._data.get('scaled_imageset_id')
join_dataset_id = collection._data.get('imageset_dataset_join_id')


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
    name="Feature extraction imageset"
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
            "Image Similarity x",
            "Image Similarity y"
        ],
        "out_columns": [
            "image_similarity_x",
            "image_similarity_y"
        ]
    },
    dataset_parents=features_node.get('id'),
    name="Image similarity"
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
    name="Image similarity mapping"
)
mapping_node = resp.get('dataset')
print('\nadded mapping node', mapping_node)

# create output node
resp = nodes.add_node(
    zc,
    workspace,
    'merge',
    {},
    dataset_parents=[dataset_id, mapping_node.get('id')],
    name="Output node"
)
merge_node = resp.get('dataset')
print('\nadded merge node', merge_node)

# Update collection to point at output node?

