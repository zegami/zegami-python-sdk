# Copyright 2021 Zegami Ltd

"""Nodes functionality."""


def add_node(client, workspace, action, params={}, type="dataset",
             dataset_parents=None, imageset_parents=None, name="New node"):
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

    print(payload)

    url = '{}/{}/project/{}/{}'.format(
        client.HOME, client.API_0, workspace.id, type + 's'
    )
    resp = client._auth_post(url, None, json=payload)
    new_node_id = resp.get(type).get('id')
    print("Created node: {}".format(new_node_id))

    return resp


def add_parent(client, workspace, node_id, parent_node_id, type="dataset"):
    """
    Add parent_node_id to the list of parents of node_id.

    This should eventually be done via a dedicated API endpoint to avoid the need to fetch and modify the existing node
    """
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

    # add new parent to source
    parent_ids = node.get('source').get(type + '_id')

    parent_ids.append(parent_node_id)

    # update node over API
    client._auth_put(url, None, json=node)
