from zegami_sdk.client import ZegamiClient
from zegami_sdk import nodes

WORKSPACE_ID = ''
COLLECTION_ID = ''


zc = ZegamiClient()


workspace = zc.get_workspace_by_id(WORKSPACE_ID)
# import pdb; pdb.set_trace()
collection = workspace.get_collection_by_id(COLLECTION_ID)
print(collection)

augment_imageset_id = collection._data.get('augment_imageset_id')

# create classification imagesets to generate classification data
resp = nodes.add_node(
    zc,
    workspace,
    'image_classification',
    {},
    'imageset',
    imageset_parents=[augment_imageset_id],
    name="classification ims"
)
classification_node = resp.get('imageset')

# create image_info dataset to render classifications as a tsv file
resp = nodes.add_node(
    zc,
    workspace,
    'image_info',
    {},
    'dataset',
    imageset_parents=classification_node.get('id'),
    name="classification info ds"
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
    name="classification mapping ds"
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
