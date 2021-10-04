from zegami_sdk.client import ZegamiClient
from zegami_sdk import nodes
from zegami_sdk.source import Source

WORKSPACE_ID = 'idUpFsFf'
COLLECTION_ID = '60db1559b5f44e582dadf9c5'

zc = ZegamiClient(home="https://staging.zegami.com")

workspace = zc.get_workspace_by_id(WORKSPACE_ID)
parent_coll = workspace.get_collection_by_id(COLLECTION_ID)

new_coll_data = workspace._create_empty_collection('clone test', None, version=1)

print(parent_coll._data)

def substitute_node(source_coll_data, target_coll_data, node_key, node_type):
    source_node_id = source_coll_data[node_key]
    target_node_id = target_coll_data[node_key]
    source_gid = f'collection_{target_coll_data["id"]}'
    target_gid = f'collection_{source_coll_data["id"]}'
    nodes.add_node_group(zc, workspace, source_node_id, source_gid, node_type)
    nodes.remove_node_group(zc, workspace, target_node_id, target_gid, node_type)
    target_coll_data[node_key] = source_node_id

# Update the child collection to use the parent imageset etc
substitute_node(parent_coll._data, new_coll_data, 'imageset_id', 'imageset')
substitute_node(parent_coll._data, new_coll_data, 'augment_imageset_id', 'imageset')

substitute_node(parent_coll._data, new_coll_data, 'dz_imageset_id', 'imageset')
# TODO update node groups for individual dz atlas/pyramid imagesets?

substitute_node(parent_coll._data, new_coll_data, 'scaled_imageset_id', 'imageset')

new_coll_data['name'] = 'Shallow clone of ' + parent_coll._data['name']

print('Updating coll document')
new_coll = workspace.get_collection_by_id(new_coll_data['id'])
new_coll._update(new_coll_data)


# reparent dz_json dataset to parent_coll augment ims
print('updating dz_json node')
new_dz_json = new_coll_data['dz_json_dataset_id']
parent_augment = new_coll_data['augment_imageset_id']
nodes.set_parent(zc, workspace, new_dz_json, parent_augment, "dataset", "imageset")

# reparent join dataset to parent_coll upload ims
print('updating join node')
new_join = new_coll_data['imageset_dataset_join_id']
parent_upload = new_coll_data['imageset_id']
nodes.set_parent(zc, workspace, new_join, parent_upload, "dataset", "imageset")

# reparent imageinfo imageset to parent_coll augment ims
print('updating join node')
new_join = new_coll_data['imageset_dataset_join_id']
parent_upload = new_coll_data['imageset_id']
nodes.set_parent(zc, workspace, new_join, parent_upload, "dataset", "imageset")

# merge dataset should use imageinfo from original
target_merge_dataset_id = new_coll_data['output_dataset_id']
parent_merge_dataset_id = parent_coll._data['output_dataset_id']
parent_merge_node, parent_url = nodes._fetch_node(zc, workspace, parent_merge_dataset_id)
target_merge_node, target_url = nodes._fetch_node(zc, workspace, target_merge_dataset_id)
parent_imageclustering = parent_merge_node['source']['dataset_id'][1]
parent_imageinfo = parent_merge_node['source']['dataset_id'][3]

target_merge_node['source']['dataset_id'][1] = parent_imageclustering
target_merge_node['source']['dataset_id'][3] = parent_imageinfo
zc._auth_put(target_url, None, json=target_merge_node)

# duplicate parent_coll's data in child_coll
print('replacing data')
new_coll.replace_data(parent_coll.rows)
