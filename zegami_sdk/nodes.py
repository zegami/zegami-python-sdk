# Copyright 2021 Zegami Ltd

"""Nodes functionality."""


class NodeManager():

    ALLOWED_NODE_TYPES = ["dataset", "imageset"]

    def __init__(self, workspace):
        """Handles node creation/modification for its owning workspace."""

        self._workspace = workspace

    @property
    def workspace():
        pass

    @workspace.getter
    def workspace(self):
        return self._workspace

    @property
    def client():
        pass

    @client.getter
    def client(self):
        return self.workspace.client

    @classmethod
    def _check_node_type(cls, node_type):
        if node_type not in cls.ALLOWED_NODE_TYPES:
            raise TypeError('node_type should be in {}, not {}'.format(
                cls.ALLOWED_NODE_TYPES, node_type))

    def _add_node(
            self, action, params={}, node_type="dataset", dataset_parents=None,
            imageset_parents=None, name="New node", node_group=None,
            processing_category=None):
        """Create a new processing node."""

        self._check_node_type(node_type)

        source = {action: params}

        if dataset_parents:
            source['dataset_id'] = dataset_parents
        if imageset_parents:
            source['imageset_id'] = imageset_parents

        payload = {
            'name': name,
            'source': source,
        }

        if node_group:
            payload['node_groups'] = [node_group]
        if processing_category:
            payload['processing_category'] = processing_category

        url = '{}/{}/project/{}/{}'.format(
            self.client.HOME, self.client.API_0, self.workspace.id,
            '{}s'.format(node_type))

        resp = self.client._auth_post(url, None, json=payload)
        new_node_id = resp.get(node_type).get('id')
        print("Created node: {}".format(new_node_id))

        return resp

    def _add_parent(self, node_id, parent_node_id, node_type="dataset"):
        """Add parent_node_id to the list of parents of node_id.

        This should eventually be done via a dedicated API endpoint to avoid
        the need to fetch and modify the existing node.
        """

        self._check_node_type(node_type)

        # Fetch target node
        url = '{}/{}/project/{}/{}/{}'.format(
            self.client.HOME, self.client.API_0, self.workspace.id,
            '{}s'.format(node_type), node_id)

        resp = self.client._auth_get(url)
        node = resp.get(node_type)  # dict

        # Strip irrelevant fields
        readonly_fields = ['data_link', 'id', 'parent_versioning_values',
                           'schema', 'total_rows']
        node = {k: v for k, v in node.items() if k not in readonly_fields}

        # Add new parent to source
        parent_ids = node.get('source').get('{}_id'.format(node_type))
        parent_ids.append(parent_node_id)

        # Update node over API
        self.client._auth_put(url, None, json=node, return_response=True)

    def _get_imageset_images(self, node_id):
        """Get the list of image info entries for the given node."""

        # fetch target node
        url = '{}/{}/project/{}/{}/{}/images'.format(
            self.client.HOME, self.client.API_1, self.workspace.id,
            "nodes", node_id)

        resp = self.client._auth_get(url)

        return resp['images']

    def _get_null_imageset_entries(self, node_id):
        """Get the indices of all image info entries which are null."""

        images_info = self._get_imageset_images(node_id)
        indices = [i for i, info in enumerate(images_info) if info is None]

        return indices

    def _create_tasks_for_null_entries(self, node_id):
        """Trigger creation of tasks for any entries in the imageset which are
        null. This can happen as a result of failed database writes.
        """

        url = '{}/{}/project/{}/{}/{}/create_tasks_for_null'.format(
            self.client.HOME, self.client.API_1, self.workspace.id,
            "nodes", node_id)

        self.client._auth_post(url, None)
