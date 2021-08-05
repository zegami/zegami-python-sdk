from zegami_sdk import nodes
from zegami_sdk.client import ZegamiClient

WORKSPACE_ID = 'tw5Tie2e'
COLLECTION_ID = ''

zc = ZegamiClient()

workspace = zc.get_workspace_by_id(WORKSPACE_ID)
coll = workspace._create_empty_collection('sdk_empty_collection')

print(coll)