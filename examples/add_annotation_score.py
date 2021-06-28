from zegami_sdk import nodes
from zegami_sdk.client import ZegamiClient

WORKSPACE_ID = 'idUpFsFf'
COLLECTION_ID = '60d49817ddb415563a6526e1'

zc = ZegamiClient("mingya.zhou@zegami.com", "GHdUh5deDHXez2G")

workspace = zc.get_workspace_by_id(WORKSPACE_ID)
collection = workspace.get_collection_by_id(COLLECTION_ID)
print(collection)

imageset_id = collection._data.get('imageset_id')

resp = nodes.add_node(
    zc,
    workspace,
    'mask_annotation_score',
    {
        "evaluated_authors": ["Alloy Scratch Detector_0100"],
    },
    'imageset',
    imageset_parents=[imageset_id],
    name="annotation score ims"
)
annotation_score_node = resp.get('imageset')

# create image_info dataset to render annotation score as a tsv file
resp = nodes.add_node(
    zc,
    workspace,
    'image_info',
    {},
    'dataset',
    imageset_parents=annotation_score_node.get('id'),
    name="annotation score info ds"
)
info_node = resp.get('dataset')

# create mapping dataset to map
join_dataset_id = collection._data.get('imageset_dataset_join_id')
resp = nodes.add_node(
    zc,
    workspace,
    'mapping',
    {},
    'dataset',
    dataset_parents=[
        info_node.get('id'),  # first parent is the data node
        join_dataset_id,  # second parent is the mapping node
    ],
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
