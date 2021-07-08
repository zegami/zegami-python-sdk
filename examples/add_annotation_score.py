from zegami_sdk import nodes
from zegami_sdk.client import ZegamiClient

WORKSPACE_ID = ''
COLLECTION_ID = ''

zc = ZegamiClient("", "")

workspace = zc.get_workspace_by_id(WORKSPACE_ID)
collection = workspace.get_collection_by_id(COLLECTION_ID)
print(collection)

imageset_id = collection._data.get('imageset_id')

resp = nodes.add_node(
    zc,
    workspace,
    'mask_annotation_score',
    # truth authors and evaluated authors should either 
    # be not specified or a list of strings
    {
        # "evaluated_authors": ["authors name"],
    },
    'imageset',
    imageset_parents=[imageset_id],
    name="annotation score ims"
)
annotation_score_node = resp.get('imageset')

# create mapping dataset to map
join_dataset_id = collection._data.get('imageset_dataset_join_id')
resp = nodes.add_node(
    zc,
    workspace,
    'mapping',
    {},
    'dataset',
    dataset_parents=[join_dataset_id],
    imageset_parents=[annotation_score_node.get('id')],
    name="annotation score mapping ds"
)
mapping_node = resp.get('dataset')

# insert into output parents
output_dataset_id = collection._data.get('output_dataset_id')
nodes.add_parent(
    zc,
    workspace,
    output_dataset_id,
    mapping_node.get('id')
)
