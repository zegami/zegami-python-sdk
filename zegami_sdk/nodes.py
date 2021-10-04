# Copyright 2021 Zegami Ltd

"""Nodes functionality."""


def add_node(
    client,
    workspace,
    action,
    params={},
    type="dataset",
    dataset_parents=None,
    imageset_parents=None,
    name="New node",
    node_group=None,
    processing_category=None
):
    """Create a new processing node."""
    assert type in ["dataset", "imageset"]

    source = {
        action: params,
    }
    if dataset_parents:
        source['dataset_id'] = dataset_parents
    if imageset_parents:
        source['imageset_id'] = imageset_parents
    payload = {
        'name': name,
        'source': source,
    }

    if node_group:
        payload['node_groups'] = node_group
    if processing_category:
        payload['processing_category'] = processing_category

    url = '{}/{}/project/{}/{}'.format(
        client.HOME, client.API_0, workspace.id, type + 's'
    )
    resp = client._auth_post(url, None, json=payload)
    new_node_id = resp.get(type).get('id')
    print("Created node: {}".format(new_node_id))

    return resp


def _fetch_node(client, workspace, node_id, type="dataset"):
    assert type in ["dataset", "imageset"]

    # fetch target node
    url = '{}/{}/project/{}/{}/{}'.format(
        client.HOME, client.API_0, workspace.id, type + 's', node_id
    )
    resp = client._auth_get(url)
    node = resp.get(type)

    # strip irrelevant fields
    readonly_fields = ['data_link', 'id', 'parent_versioning_values', 'schema', 'total_rows']
    for field in readonly_fields:
        if field in node:
            node.pop(field)

    return node, url

def add_parent(client, workspace, node_id, parent_node_id, type="dataset"):
    """
    Add parent_node_id to the list of parents of node_id.

    This should eventually be done via a dedicated API endpoint to avoid the need to fetch and modify the existing node
    """
    node, url = _fetch_node(client, workspace, node_id, type)

    # add new parent to source
    parent_ids = node.get('source').get(type + '_id')

    parent_ids.append(parent_node_id)

    # update node over API
    client._auth_put(url, None, json=node, return_response=True)


def _get_imageset_images(client, workspace, node_id):
    """
    Get the list of image info entries for the given node
    """
    # fetch target node
    url = '{}/{}/project/{}/{}/{}/images'.format(
        client.HOME, client.API_1, workspace.id, "nodes", node_id
    )
    resp = client._auth_get(url)
    return resp['images']

def remove_parent(client, workspace, node_id, parent_node_id, type="dataset"):
    node, url = _fetch_node(client, workspace, node_id, type)
    parent_ids = node['source'][type + '_id']
    node['source'][type + 'id'] = [parent for parent in parent_ids if parent != parent_node_id]

    client._auth_put(url, None, json=node)


def set_parent(client, workspace, node_id, parent_node_id, type="dataset", parent_type="dataset"):
    """
    Update the given node to use the new parent for the given type.

    Only applicable to node types which have a single parent of the given type.
    """
    node, url = _fetch_node(client, workspace, node_id, type)

    node.get('source')[parent_type + '_id'] = parent_node_id

    # update node over API
    client._auth_put(url, None, json=node)


def _get_null_imageset_entries(client, workspace, node_id):
    """
    Get the indices of all image info entries which are null
    """
    images_info = _get_imageset_images(client, workspace, node_id)
    indices = [i for i, info in enumerate(images_info) if info is None]
    return indices


def _create_tasks_for_null_entries(client, workspace, node_id):
    """
    Trigger creation of tasks for any entries in the imageset which are null.
    This can happen as a result of failed database writes.
    """
    url = '{}/{}/project/{}/{}/{}/create_tasks_for_null'.format(
        client.HOME, client.API_1, workspace.id, "nodes", node_id
    )
    client._auth_post(url, None)


def remove_node_group(client, workspace, node_id, group_id, type="dataset"):
    node, url = _fetch_node(client, workspace, node_id, type)
    current_groups = node.get('node_groups', [])
    node['node_groups'] = [group for group in current_groups if group != group_id]
    
    client._auth_put(url, None, json=node)


def add_node_group(client, workspace, node_id, group_id, type="dataset"):
    node, url = _fetch_node(client, workspace, node_id, type)
    if group_id not in node['node_groups']:
        node['node_groups'].append(group_id)
        client._auth_put(url, None, json=node)
