from zegami_sdk import nodes
from zegami_sdk.client import ZegamiClient

WORKSPACE_ID = ''
COLLECTION_ID = ''

zc = ZegamiClient()

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
