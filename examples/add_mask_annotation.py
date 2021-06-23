from zegami_sdk import nodes
from zegami_sdk.client import ZegamiClient

WORKSPACE_ID = ''
COLLECTION_ID = ''

zc = ZegamiClient()

workspace = zc.get_workspace_by_id(WORKSPACE_ID)
collection = workspace.get_collection_by_id(COLLECTION_ID)
print(collection)

imageset_id = collection._data.get('imageset_id')

# create classification imagesets to generate classification data
resp = nodes.add_node(
    zc,
    workspace,
    'mask_annotation',
    {
        "weights_blob": "mask_rcnn_alloy-scratch-detector_0100.h5",
        "model_author_name": "Alloy Scratch Detector_0100",
        # TODO add param for num classes
    },
    'imageset',
    imageset_parents=[imageset_id],
    name="masks ims"
)
classification_node = resp.get('imageset')
