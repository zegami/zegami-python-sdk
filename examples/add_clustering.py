from zegami_sdk import nodes
from zegami_sdk.client import ZegamiClient

WORKSPACE_ID = ''
COLLECTION_ID = ''


zc = ZegamiClient()


workspace = zc.get_workspace_by_id(WORKSPACE_ID)
collection = workspace.get_collection_by_id(COLLECTION_ID)
print(collection)

# Add new clustering node
dataset_id = collection._data.get('dataset_id')
resp = nodes.add_node(
    zc,
    workspace,
    'cluster',
    {
        'exclude': [''],  # list of column names as found in tsv files (not Ids)
        'out_columns': ['mean_datasimx', 'mean_datasimy'],
        'out_column_titles': ['Mean_DataSimX', 'Mean_DataSimY'],
        'columns_order': [1100, 1101]  # important to set this to a unique value
    },
    dataset_parents=dataset_id,
    name="Mean data similarity"
)
cluster_node = resp.get('dataset')

# Include output in collection output
output_dataset_id = collection._data.get('output_dataset_id')
resp = nodes.add_parent(
    zc,
    workspace,
    output_dataset_id,
    cluster_node.get('id')
)
