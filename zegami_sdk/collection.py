# -*- coding: utf-8 -*-
# Copyright 2021 Zegami Ltd

"""Collection functionality."""

from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
import json
import os
from time import time
import pandas as pd
from PIL import Image, UnidentifiedImageError

from .source import Source, UploadableSource
from .nodes import add_node, add_parent


class Collection():

    def __repr__(self) -> str:
        return "<CollectionV{} id={} name={}>".format(
            self.version, self.id, self.name)

    def __len__(self) -> int:
        return int(self._retrieve('total_data_items'))

    def __init__(self, client, workspace, collection_dict, allow_caching=True):
        """
        Represents a Zegami collection, providing controls for data/annotation
        read/writing.

        User instantiation is not required or recommended, collection instances
        can be found in Workspace() objects, and new collections can be created
        using workspaces.
        """

        self._client = client
        self._data = collection_dict
        self._workspace = workspace
        self._generate_sources()

        # Caching
        self.allow_caching = allow_caching
        self.clear_cache()

    def clear_cache(self):
        self._cached_rows = None
        self._cached_annotations_data = None
        self._cached_image_meta_source_lookups = {}

    @property
    def client():
        pass

    @client.getter
    def client(self):
        if not self._client:
            raise ValueError('Collection: Client is not valid')
        return self._client

    @property
    def name():
        pass

    @name.getter
    def name(self) -> str:
        return self._retrieve('name')

    @property
    def id():
        pass

    @id.getter
    def id(self):
        return self._retrieve('id')

    @property
    def _dataset_id():
        pass

    @_dataset_id.getter
    def _dataset_id(self) -> str:
        return self._retrieve('dataset_id')

    @property
    def _upload_dataset_id():
        pass

    @_upload_dataset_id.getter
    def _upload_dataset_id(self) -> str:
        return self._retrieve('upload_dataset_id')

    @property
    def version():
        pass

    @version.getter
    def version(self):
        return self._data['version'] if 'version' in self._data.keys() else 1

    @property
    def workspace():
        pass

    @workspace.getter
    def workspace(self):
        return self._workspace

    @property
    def workspace_id():
        pass

    @workspace_id.getter
    def workspace_id(self):
        if self.workspace is None:
            raise ValueError('Collection has no assigned workspace')
        return self.workspace.id

    @property
    def url():
        pass

    @url.getter
    def url(self):
        return '{}/collections/{}-{}'.format(
            self.client.HOME, self.workspace_id, self.id)

    def _generate_sources(self):
        """On-construction sets source instances using of collection data."""

        if self.version < 2:
            source_data = self._data.copy()
            source_data['name'] = 'None'
            self._sources = [Source(self, source_data)]

        else:
            self._sources = [
                Source(self, s) for s in self._data['image_sources']]

    @property
    def sources():
        pass

    @sources.getter
    def sources(self) -> list:
        """
        All Source() instances belonging to this collection. V1 collections do
        not use sources, however a single pseudo source with the correct
        imageset information is returned.
        """

        return self._sources

    def show_sources(self):
        """Lists the IDs and names of all sources in the collection."""

        print('\nImage sources ({}):'.format(len(self.sources)))
        for s in self.sources:
            print(s)

    @property
    def rows():
        pass

    @rows.getter
    def rows(self) -> pd.DataFrame:
        """All data rows of the collection as a dataframe."""

        if self.allow_caching and self._cached_rows is not None:
            return self._cached_rows

        # Obtain the metadata bytes from Zegami
        url = '{}/{}/project/{}/datasets/{}/file'.format(
            self.client.HOME, self.client.API_0,
            self.workspace_id, self._dataset_id)

        r = self.client._auth_get(url, return_response=True)
        tsv_bytes = BytesIO(r.content)

        # Convert into a pd.DataFrame
        try:
            df = pd.read_csv(tsv_bytes, sep='\t')
        except Exception:
            try:
                df = pd.read_excel(tsv_bytes)
            except Exception:
                print('Warning - failed to open metadata as a dataframe, '
                      'returned the tsv bytes instead.')
                return tsv_bytes

        if self.allow_caching:
            self._cached_rows = df

        return df

    @property
    def tags():
        pass

    @tags.getter
    def tags(self):
        return self._get_tag_indices()

    @property
    def status():
        pass

    @status.getter
    def status(self):
        """The current status of this collection."""

        url = '{}/{}/project/{}/collections/{}'.format(
            self.client.HOME, self.client.API_0, self.workspace_id, self.id)
        resp = self.client._auth_get(url)

        return resp['collection']['status']

    @property
    def status_bool():
        pass

    @status_bool.getter
    def status_bool(self) -> bool:
        """The status of this collection as a fully processed or not."""

        return self.status['progress'] == 1

    @property
    def node_statuses():
        pass

    @node_statuses.getter
    def node_statuses(self):
        """All nodes and statuses that belong to the collection."""
        url = '{}/{}/project/{}/collections/{}/node_statuses'.format(
            self.client.HOME, self.client.API_0, self.workspace_id, self.id)
        resp = self.client._auth_get(url)
        return resp

    def _move_to_folder(self, folder_name):
        """
        Move current collection into a folder. When folder_name is None, the collection will
        not belong to any folder.
        This feature is still WIP.
        """
        url = '{}/{}/project/{}/collections/{}'.format(
            self.client.HOME, self.client.API_0, self.workspace_id, self.id)
        collection_body = self.client._auth_get(url)['collection']

        if folder_name is None:
            if 'folder' in collection_body:
                del collection_body['folder']
            if 'folder' in self._data:
                del self._data['folder']
        else:
            collection_body['folder'] = folder_name
            self._data['folder'] = folder_name

        if 'projectId' in collection_body:
            del collection_body['projectId']
        if 'published' in collection_body:
            for record in collection_body['published']:
                del record['status']

        self.client._auth_put(
            url, body=None,
            return_response=True, json=collection_body)

    def duplicate(self, duplicate_name=None):
        """
        Creates a completely separate copy of the collection within the workspace
        Processed blobs are reused but there is no ongoing link to the original
        """
        url = '{}/{}/project/{}/collections/duplicate'.format(
            self.client.HOME, self.client.API_0, self.workspace_id)
        payload = {
            "old_collection_id": self.id,
        }
        if duplicate_name:
            payload["new_collection_name"] = duplicate_name

        resp = self.client._auth_post(url, json.dumps(payload))
        print('Duplicated collection. New collection id: ', resp['new_collection_id'])
        return resp

    def row_index_to_imageset_index(self, row_idx, source=0) -> int:
        """
        Turn a row-space index into an imageset-space index. Typically used
        in more advanced operations.
        """

        row_idx = int(row_idx)
        if row_idx < 0:
            raise ValueError(
                'Use an index above 0, not {}'.format(row_idx))

        lookup = self._get_image_meta_lookup(source=source)

        try:
            return lookup[row_idx]
        except IndexError:
            raise IndexError(
                "Invalid row index {} for this source."
                .format(row_idx))

    def imageset_index_to_row_index(self, imageset_index, source=0) -> int:
        """
        Turn an imageset-space index into a row-space index. Typically used
        in more advanced operations.
        """

        imageset_index = int(imageset_index)
        if imageset_index < 0:
            raise ValueError(
                'Use an index above 0, not {}'.format(imageset_index))

        lookup = self._get_image_meta_lookup(source=source)

        try:
            return lookup.index(imageset_index)
        except ValueError:
            raise IndexError(
                "Invalid imageset index {} for this source"
                .format(imageset_index))

    def add_snapshot(self, name, desc, snapshot):
        url = '{}/{}/project/{}/snapshots/{}/snapshots'.format(
            self.client.HOME, self.client.API_0, self.workspace_id, self.id)
        payload = {
            'name': name,
            'description': desc,
            'snapshot': snapshot,
            'version': 3,
        }
        r = self.client._auth_post(url, json.dumps(payload), return_response=True)
        return r

    def add_feature_pipeline(self, pipeline_name, steps, source=0, generate_snapshot=False):
        """
        Add mRMR, cluster, mapping nodes and update the merge node for the source.

        pipeline_name: self defined name used to derive column ids/titles
        source: index or name of the collection source to use
        generate_snapshot: whether to generate a snapshot for the new clustering results
        steps: list of nodes which would feed one into the other in sequence
            - example:
                [
                    {
                        'action': 'mRMR',
                        'params': {
                            'target_column': 'weight',
                            'K': 20,
                            'option': 'regression'  # another option is classification
                        },
                    },
                    {
                        'action': 'cluster',
                        'params': {}
                        }
                    }
                ]

        The results from get_feature_pipelines() can be used to passed in here to recreate a pipeline.
        """
        # get the source
        source = self._parse_source(source)
        node_group = [
            'source_{}'.format(source.name),
            'collection_{}'.format(self.id),
            'feature_pipeline_{}'.format(pipeline_name)]

        mRMR_params = steps[0]['params']

        # find the feature extraction node
        source_feature_extraction_node = self.get_feature_extraction_imageset_id(source)

        join_dataset_id = source._imageset_dataset_join_id
        imageset_parents = [source_feature_extraction_node]
        dataset_parents = [self._dataset_id, join_dataset_id]

        mRMR_node = add_node(
            self.client,
            self.workspace,
            action=steps[0]['action'],
            params=mRMR_params,
            type="imageset",
            dataset_parents=dataset_parents,
            imageset_parents=imageset_parents,
            processing_category='image_clustering',
            node_group=node_group,
            name="{} imageset for {} of {}".format(pipeline_name, source.name, self.name),
        )

        cluster_params = steps[1]['params']
        cluster_params["out_column_title_prefix"] = "{} Image Similarity ({})".format(pipeline_name, source.name)
        pipeline_name_stripped = (pipeline_name.lower().
                                  replace(' ', '').replace('_', '').replace('-', '').replace('.', ''))
        cluster_params["out_column_name_prefix"] = "image_similarity_{}_{}".format(source.name, pipeline_name_stripped)
        cluster_node = add_node(
            self.client,
            self.workspace,
            action=steps[1]['action'],
            params=cluster_params,
            type="dataset",
            dataset_parents=mRMR_node.get('imageset').get('id'),
            imageset_parents=None,
            processing_category='image_clustering',
            node_group=node_group,
            name="{} Image clustering dataset for {} of {}".format(pipeline_name, source.name, self.name),
        )

        # add node to map the output to row space
        mapping_node = add_node(
            self.client,
            self.workspace,
            'mapping',
            {},
            dataset_parents=[cluster_node.get('dataset').get('id'), join_dataset_id],
            name=pipeline_name + " mapping",
            node_group=node_group,
            processing_category='image_clustering'
        )

        # add to the collection's output node
        output_dataset_id = self._data.get('output_dataset_id')
        add_parent(
            self.client,
            self.workspace,
            output_dataset_id,
            mapping_node.get('dataset').get('id')
        )

        # generate snapshot
        if generate_snapshot:
            snapshot_name = '{} Image Similarity View ({})'.format(pipeline_name, source.name)
            snapshot_desc = 'Target column is {}, K value is {}'.format(mRMR_params['target_column'], mRMR_params['K'])
            source_name_stripped = (source.name.lower().
                                    replace(' ', '').replace('_', '').replace('-', '').replace('.', ''))
            snapshot_payload = {
                'view': 'scatter',
                'sc_h': 'imageSimilarity{}{}0'.format(source_name_stripped, pipeline_name_stripped),
                'sc_v': 'imageSimilarity{}{}1'.format(source_name_stripped, pipeline_name_stripped),
                'source': source.name,
                'pan': 'TYPES_PANEL'
            }
            self.add_snapshot(snapshot_name, snapshot_desc, snapshot_payload)

    def get_feature_pipelines(self):  # noqa: C901
        """
        Get all feature pipelines in a collection.

        Example shape:
            feature_pipelines = [
                {
                    name='mRMR20',
                    source=0,
                    steps=[     # list of nodes which would feed one into the other in sequence
                        {
                            'action': 'mRMR',
                            'params': {
                                'target_column': 'weight',
                                'K': 20,
                            },
                        },
                        {
                            'action': 'cluster',
                            'params': {
                                "out_column_start_order": 1010,
                                'algorithm_args': {
                                    'algorithm': 'umap',
                                    'n_components': 2,
                                    "n_neighbors": 15,
                                    "min_dist": 0.5,
                                    "spread": 2,
                                    "random_state": 123,
                                }
                            }
                        }
                    ]
                },
            ]
        """
        all_nodes = self.node_statuses
        source_names = [s.name for s in self.sources]
        feature_pipelines = []

        # nodes sorted by source and feature pipeline name
        feature_pipelines_nodes = {}
        for source_name in source_names:
            feature_pipelines_nodes[source_name] = {}

        for node in all_nodes:
            node_groups = node.get('node_groups')
            # check if node_groups contain 'feature_pipeline_'
            if node_groups and len(node_groups) == 3:
                # get the source name after 'source_'
                node_source_name = [
                    group for group in node_groups if group.startswith('source_')
                ][0][7:]
                # get the source name after 'feature_pipeline_'
                feature_pipeline_name = [
                    group for group in node_groups if group.startswith('feature_pipeline_')
                ][0][17:]
                if feature_pipeline_name in feature_pipelines_nodes[node_source_name]:
                    feature_pipelines_nodes[node_source_name][feature_pipeline_name].append(node)
                else:
                    feature_pipelines_nodes[node_source_name][feature_pipeline_name] = [node]

        for source_name in source_names:
            source_pipelines = feature_pipelines_nodes[source_name]
            for pipeline_name, nodes in source_pipelines.items():
                for node in nodes:
                    if 'cluster' in node['source']:
                        cluster_params = node['source']['cluster']
                        # some params should be generated by add_feature_pipeline
                        unwanted_params = [
                            'out_column_name_prefix',
                            'out_column_title_prefix',
                            'out_column_start_order',
                        ]
                        for param in unwanted_params:
                            if param in cluster_params.keys():
                                cluster_params.pop(param)
                    if 'mRMR' in node['source']:
                        mRMR_params = node['source']['mRMR']
                feature_pipelines.append({
                    'pipeline_name': pipeline_name,
                    'source_name': source_name,
                    'steps': [
                        {
                            'action': 'mRMR',
                            'params': mRMR_params,
                        },
                        {
                            'action': 'cluster',
                            'params': cluster_params,
                        }
                    ]
                })

        return feature_pipelines

    def add_explainability(self, data, parent_source=0):
        """
        Add an explainability map node and create a new source with the node.
        """
        collection_group_source = [
            'source_' + data['NEW_SOURCE_NAME'],
            'collection_' + self.id
        ]
        parent_source = self._parse_source(parent_source)
        augment_imageset_id = parent_source._data.get('augment_imageset_id')
        resp = add_node(
            self.client,
            self.workspace,
            'explainability_map',
            data['EXPLAINABILITY_SOURCE'],
            'imageset',
            imageset_parents=augment_imageset_id,
            name="{} explainability map node".format(data['NEW_SOURCE_NAME']),
            node_group=collection_group_source,
            processing_category='upload'
        )
        explainability_map_node = resp.get('imageset')
        self.add_source(data['NEW_SOURCE_NAME'], explainability_map_node.get('id'))

    def add_custom_clustering(self, data, source=0):
        """
        Add feature extraction and clustering given an custom model.
        """
        source = self._parse_source(source)
        collection_group_source = [
            'source_' + source.name,
            'collection_' + self.id
        ]
        scaled_imageset_id = source._data.get('scaled_imageset_id')
        join_dataset_id = source._imageset_dataset_join_id

        resp = add_node(
            self.client,
            self.workspace,
            'custom_feature_extraction',
            data['FEATURE_EXTRACTION_SOURCE'],
            'imageset',
            imageset_parents=scaled_imageset_id,
            name="custom feature extraction node",
            node_group=collection_group_source,
            processing_category='image_clustering'
        )
        custom_feature_extraction_node = resp.get('imageset')

        resp = add_node(
            self.client,
            self.workspace,
            'cluster',
            data['CLUSTERING_SOURCE'],
            dataset_parents=custom_feature_extraction_node.get('id'),
            name="custom feature extraction similarity",
            node_group=collection_group_source,
            processing_category='image_clustering'
        )
        cluster_node = resp.get('dataset')

        resp = add_node(
            self.client,
            self.workspace,
            'mapping',
            {},
            dataset_parents=[cluster_node.get('id'), join_dataset_id],
            name="custom feature extraction mapping",
            node_group=collection_group_source,
            processing_category='image_clustering'
        )
        mapping_node = resp.get('dataset')

        output_dataset_id = self._data.get('output_dataset_id')
        resp = add_parent(
            self.client,
            self.workspace,
            output_dataset_id,
            mapping_node.get('id')
        )

    def get_rows_by_filter(self, filters):
        """
        Gets rows of metadata in a collection by a flexible filter.

        The filter should be a dictionary describing what to permit through
        any specified columns.

        Example:
            row_filter = { 'breed': ['Cairn', 'Dingo'] }

        For each filter, OR logic is used ('Cairn' or 'Dingo' would pass)

        For multiple filters, AND logic is used (adding an 'age' filter would
        require the 'breed' AND 'age' filters to both pass).
        """

        if type(filters) != dict:
            raise TypeError('Filters should be a dict.')

        rows = self.rows.copy()

        for fk, fv in filters.items():
            if not type(fv) == list:
                fv = [fv]
            rows = rows[rows[fk].isin(fv)]

        return rows

    def get_rows_by_tags(self, tag_names):
        """
        Gets rows of metadata in a collection by a list of tag_names.

        Example:
            tag_names = ['name1', 'name2']

        This would return rows which has tags in the tag_names.
        """

        if type(tag_names) != list:
            raise TypeError('Expected tag_names to be a list, not a {}'
                            .format(type(tag_names)))

        row_indices = set()
        for tag in tag_names:
            if tag in self.tags.keys():
                row_indices.update(self.tags[tag])
        rows = self.rows.iloc[list(row_indices)]

        return rows

    def get_image_urls(self, rows=None, source=0, generate_signed_urls=False,
                       signed_expiry_days=None, override_imageset_id=None):
        """
        Converts rows into their corresponding image URLs.

        If generate_signed_urls is false the URLs require a token to download
        These urls can be passed to download_image()/download_image_batch().

        If generate_signed_urls is true the urls can be used to fetch the
        images directly from blob storage, using a temporary access signature
        with an optionally specified lifetime.

        By default the uploaded images are fetched, but it's possible to fetch
        e.g. the thumbnails only, by providing an alternative imageset id.
        """

        # Turn the provided 'rows' into a list of ints.
        # If 'rows' are not defined, get all rows of collection.
        if rows is not None:
            if type(rows) == pd.DataFrame:
                indices = list(rows.index)
            elif type(rows) == list:
                indices = [int(r) for r in rows]
            elif type(rows) == int:
                indices = [rows]
            else:
                raise ValueError('Invalid rows argument, \'{}\' not supported'
                                 .format(type(rows)))
        else:
            indices = [i for i in range(len(self))]

        # Convert the row-space indices into imageset-space indices
        lookup = self._get_image_meta_lookup(source)
        imageset_indices = [lookup[i] for i in indices]

        # Convert these into URLs
        if override_imageset_id is not None:
            imageset_id = override_imageset_id
        else:
            imageset_id = self._get_imageset_id(source)

        c = self.client
        if not generate_signed_urls:
            return ['{}/{}/project/{}/imagesets/{}/images/{}/data'.format(
                c.HOME, c.API_0, self.workspace_id, imageset_id,
                i) for i in imageset_indices]
        else:

            query = ''

            if signed_expiry_days is not None:
                query = '?expiry_days={}'.format(signed_expiry_days)

            get_signed_urls = [
                '{}/{}/project/{}/imagesets/{}/images/{}/signed_route{}'
                .format(c.HOME, c.API_0, self.workspace_id, imageset_id,
                        i, query) for i in imageset_indices]

            signed_route_urls = []
            for url in get_signed_urls:
                # Unjoined rows will have None. Possibly better to filter these
                # out earlier, but this works
                if 'None' in url:
                    signed_route_urls.append('')
                else:
                    response = c._auth_get(url)
                    signed_route_urls.append(response['url'])

            return signed_route_urls

    def get_feature_extraction_imageset_id(self, source=0) -> str:
        """Returns the feature extraction imageset id in the given source index."""
        source = self._parse_source(source)
        source_name = source.name
        all_nodes = self.node_statuses
        for node in all_nodes:
            if ('image_feature_extraction' in node['source'].keys() and
                    node['node_groups'][0] == 'source_{}'.format(source_name)):
                return node["id"]
        return None

    def download_annotation(self, annotation_id):
        """
        Converts an annotation_id into downloaded annotation data.

        This will vary in content depending on the annotation type and
        format.
        """

        url = '{}/{}/project/{}/annotations/{}'.format(
            self.client.HOME, self.client.API_1, self.workspace.id,
            annotation_id)

        return self.client._auth_get(url)

    def replace_data(self, data, fail_if_not_ready=True):
        """
        Replaces the data in the collection.

        The provided input should be a pandas dataframe or a local
        csv/json/tsv/txt/xlsx/xls file. If a xlsx/xls file is used only data
        from the default sheet will be fetched.

        By default, this operation will fail immediately if the collection is
        not fully processed to avoid issues.
        """

        # If this collection is not fully processed, do not allow data upload
        if fail_if_not_ready and not self.status_bool:
            raise ValueError(
                'Collection has not fully processed. Wait for the collection '
                'to finish processing, or force this method with '
                'fail_if_not_ready=False (not recommended)\n\n{}'
                .format(self.status))

        # Prepare data as bytes
        if type(data) == pd.DataFrame:
            tsv = data.to_csv(sep='\t', index=False)
            upload_data = bytes(tsv, 'utf-8')
            name = 'provided_as_dataframe.tsv'
        else:
            name = os.path.split(data)[-1]
            if name.split('.')[-1] in ['csv', 'json', 'tsv',
                                       'txt', 'xls', 'xlsx']:
                with open(data, 'rb') as f:
                    upload_data = f.read()
            else:
                raise ValueError(
                    'File extension must one of these: csv, json, tsv, txt, '
                    'xls, xlsx')

        # Create blob storage and upload to it
        urls, id_set = self.client._obtain_signed_blob_storage_urls(
            self.workspace_id,
            blob_path='datasets/{}'.format(self._upload_dataset_id)
        )
        blob_id = id_set['ids'][0]
        url = urls[blob_id]

        # Upload data to it
        self.client._upload_to_signed_blob_storage_url(
            upload_data, url, 'application/octet-stream')

        # Update the upload dataset details
        upload_dataset_url = '{}/{}/project/{}/datasets/{}'.format(
            self.client.HOME, self.client.API_0, self.workspace_id,
            self._upload_dataset_id)

        current_dataset = self.client._auth_get(upload_dataset_url)["dataset"]
        current_dataset["source"]["upload"]["name"] = name
        current_dataset["source"]["blob_id"] = blob_id

        # Returning response is true as otherwise it will try to return json
        # but this response is empty
        self.client._auth_put(
            upload_dataset_url, body=None,
            return_response=True, json=current_dataset)

        self._cached_rows = None

    def save_image(self, url, target_folder_path='./', filename='image',
                   extension='png'):
        """
        Downloads an image and saves to disk.
        For input, see Collection.get_image_urls().
        """

        if not os.path.exists(target_folder_path):
            os.makedirs(target_folder_path)

        r = self.client._auth_get(url, return_response=True, stream=True)
        with open(target_folder_path + '/' + filename + '.' + extension, 'wb')\
                as f:
            f.write(r.content)

    def save_image_batch(self, urls, target_folder_path='./', extension='png',
                         max_workers=50, show_time_taken=True):
        """
        Downloads a batch of images and saves to disk.

        Filenames are the row index followed by the specified extension.
        For input, see Collection.get_image_urls().
        """

        def save_single(index, url):
            self.save_image(url, target_folder_path, filename=str(index),
                            extension=extension)
            return index

        t0 = time()
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = [ex.submit(save_single, i, u)
                       for i, u in enumerate(urls)]
            ex.shutdown(wait=True)

        # Error catch all completed futures
        for f in futures:
            if f.exception() is not None:
                raise Exception(
                    'Exception in multi-threaded image saving: {}'
                    .format(f.exception()))

        if show_time_taken:
            print('\nDownloaded {} images in {:.2f} seconds.'
                  .format(len(futures), time() - t0))

    def download_image(self, url):
        """
        Downloads an image into memory as a PIL.Image.

        For input, see Collection.get_image_urls().
        """

        r = self.client._auth_get(url, return_response=True, stream=True)
        r.raw.decode = True

        try:
            return Image.open(r.raw)
        except UnidentifiedImageError:
            return Image.open(BytesIO(r.content))

    def download_image_batch(self, urls, max_workers=50, show_time_taken=True):
        """
        Downloads multiple images into memory (each as a PIL.Image)
        concurrently.

        Please be aware that these images are being downloaded into memory,
        if you download a huge collection of images you may eat up your
        RAM!
        """

        t0 = time()
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = [ex.submit(self.download_image, u) for u in urls]
            ex.shutdown(wait=True)

        # Error catch all completed futures
        for f in futures:
            if f.exception() is not None:
                raise Exception(
                    'Exception in multi-threaded image downloading: {}'
                    .format(f.exception()))

        if show_time_taken:
            print('\nDownloaded {} images in {:.2f} seconds.'
                  .format(len(futures), time() - t0))

        return [f.result() for f in futures]

    def delete_images_with_tag(self, tag='delete'):
        """Delete all the images in the collection with the tag 'delete'.s."""

        row_indices = set()
        if tag in self.tags.keys():
            row_indices.update(self.tags[tag])
            lookup = self._get_image_meta_lookup()
            imageset_indices = [lookup[int(i)] for i in row_indices]
            c = self.client
            urls = ['{}/{}/project/{}/imagesets/{}/images/{}'.format(
                c.HOME, c.API_0, self.workspace_id, self._get_imageset_id(),
                i) for i in imageset_indices]
            for url in urls:
                c._auth_delete(url)
            print(f'\nDeleted {len(urls)} images')

    def _get_tag_indices(self):
        """Returns collection tags indices."""

        c = self.client
        url = '{}/{}/project/{}/collections/{}/tags'.format(
            c.HOME, c.API_1, self.workspace_id, self.id)
        response = c._auth_get(url)
        return self._parse_tags(response['tagRecords'])

    def _parse_tags(self, tag_records):
        """
        Parses tag indices into a list of tags, each with an list of
        indices.
        """

        tags = {}
        for record in tag_records:
            if record['tag'] not in tags.keys():
                tags[record['tag']] = []
            tags[record['tag']].append(record['key'])
        return tags

    def get_annotations(self, anno_type=None, source=0) -> dict:
        """
        Gets the annotations of a collection.
        Defaults to searching for annotations of all types.
        In V1 collections, the source argument is ignored.
        """

        if self.version < 2:
            url = '{}/{}/project/{}/annotations/collection/{}'.format(
                self.client.HOME, self.client.API_1, self.workspace_id,
                self.id)
        else:
            source = self._parse_source(source)
            url = '{}/{}/project/{}/annotations/collection/{}/source/{}'\
                .format(self.client.HOME, self.client.API_1, self.workspace_id,
                        self.id, source.id)

        if anno_type is not None:
            url += '?type=' + anno_type

        annos = self.client._auth_get(url)

        if self.version < 2:
            annos = annos['sources'][0]

        return annos['annotations']

    def get_annotations_for_image(
            self, row_index, source=0, anno_type=None) -> list:
        """
        Returns annotations for a single item in the collection.
        Defaults to searching for annotations of all types.
        """

        source = self._parse_source(source)

        if type(row_index) != int or row_index < 0:
            raise ValueError(
                'Expected row_index to be a positive int, not {}'
                .format(row_index))

        imageset_index = self.row_index_to_imageset_index(
            row_index, source=source)

        url = '{}/{}/project/{}/annotations/imageset/{}/images/{}'\
            .format(self.client.HOME, self.client.API_1, self.workspace_id,
                    self._get_imageset_id(), imageset_index)

        if anno_type is not None:
            url += '?type=' + anno_type

        return self.client._auth_get(url)

    def upload_annotation(self, uploadable, row_index=None, image_index=None,
                          source=None, author=None, debug=False):
        """
        Uploads an annotation to Zegami.

        Requires uploadable annotation data (see
        AnnotationClass.create_uploadable()), the row index of the image the
        annotation belongs to, and the source (if using a multi-image-source
        collection). If no source is provided, it will be uploaded to the
        first source.

        Optionally provide an author, which for an inference result should
        probably some identifier for the model. If nothing is provided, the
        ZegamiClient's .name property will be used.
        """

        source = None if self.version == 1 else self._parse_source(source)
        imageset_id = self._get_imageset_id(source)
        image_meta_lookup = self._get_image_meta_lookup(source)
        author = author or self.client.email

        if image_index is None:
            assert row_index is not None,\
                'Must provide either row_index or image_index'
            image_index = image_meta_lookup[row_index]
        else:
            assert row_index is None,\
                'Must provide only one or row_index, image_index'

        assert type(uploadable) == dict,\
            'Expected uploadable data to be a dict, not a {}'\
            .format(type(uploadable))

        assert 'type' in uploadable.keys(),\
            'Expected \'type\' key in uploadable: {}'.format(uploadable)

        assert 'annotation' in uploadable.keys(),\
            'Expected \'annotation\' key in uploadable: {}'.format(uploadable)

        assert type(imageset_id) == str,\
            'Expected imageset_id to be a str, not {}'\
            .format(type(imageset_id))

        # Get the class-specific data to upload
        payload = {
            'author': author,
            'imageset_id': imageset_id,
            'image_index': int(image_index),
            'annotation': uploadable['annotation'],
            'type': uploadable['type'],
            'format': uploadable['format'],
            'class_id': str(int(uploadable['class_id'])),
        }

        # Check that there are no missing fields in the payload
        for k, v in payload.items():
            assert v is not None, 'Empty annotation uploadable data value '
            'for \'{}\''.format(k)

        # Potentially print for debugging purposes
        if debug:
            print('\nupload_annotation payload:\n')
            for k, v in payload.items():
                if k == 'annotation':
                    print('- annotation:')
                    for k2, v2 in payload['annotation'].items():
                        print('\t- {} : {}'.format(k2, v2))
                else:
                    print('- {} : {}'.format(k, v))
            print('\nJSON:\n{}'.format(json.dumps(payload)))

        # POST
        c = self.client
        url = '{}/{}/project/{}/annotations'.format(
            c.HOME, c.API_1, self.workspace_id)
        r = c._auth_post(url, json.dumps(payload), return_response=True)

        return r

    def delete_annotation(self, annotation_id):
        """
        Delete an annotation by its ID. These are obtainable using the
        get_annotations...() methods.
        """

        c = self.client
        url = '{}/{}/project/{}/annotations/{}'\
            .format(c.HOME, c.API_1, self.workspace_id, annotation_id)
        payload = {
            'author': c.email
        }
        r = c._auth_delete(url, data=json.dumps(payload))

        return r

    def delete_all_annotations(self, only_for_source=None):
        """
        Deletes all annotations saved to the collection. By default this
        operation deletes all annotations from all sources. Provide a
        specific source (instance or index) to limit this to a particular
        source.
        """

        # Get the sources to delete annotations from
        scoped_sources = self.sources if only_for_source is None\
            else [self._parse_source(only_for_source)]

        c = 0
        for source in scoped_sources:

            annos = self.get_annotations(source=source)
            if len(annos) == 0:
                continue

            print('Deleting {} annotations from source {}'
                  .format(len(annos), source.name))

            for j, anno in enumerate(annos):
                self.delete_annotation(anno['id'])
                print('\r{}/{}'.format(j + 1, len(annos)), end='',
                      flush=True)
                c += 1
            print('')

        print('\nDeleted {} annotations from collection "{}"'.format(
            c, self.name))

    @property
    def userdata():
        pass

    @userdata.getter
    def userdata(self):

        c = self.client
        url = '{}/{}/project/{}/collections/{}'.format(
            c.HOME, c.API_0, self.workspace_id, self.id)
        data = c._auth_get(url)['collection']
        userdata = data['userdata'] if 'userdata' in data.keys() else None

        return userdata

    def set_userdata(self, data):
        """ Additively sets userdata. To remove data set its value to None. """

        c = self.client
        url = '{}/{}/project/{}/collections/{}/userdata'.format(
            c.HOME, c.API_0, self.workspace_id, self.id)
        userdata = c._auth_post(url, json.dumps(data))

        return userdata

    @property
    def classes():
        pass

    @classes.getter
    def classes(self) -> list:
        """
        Property for the class configuration of the collection.

        Used in an annotation workflow to tell Zegami how to treat defined
        classes.

        To set new classes, provide a list of class dictionaries of shape:

        collection.classes = [
            {
                'color' : '#32a852',    # A hex color for the class
                'name'  : 'Dog',        # A human-readable identifier
                'id'    : 0             # The unique integer class ID
            },
            {
                'color' : '#43f821',    # A hex color for the class
                'name'  : 'Cat',        # A human-readable identifier
                'id'    : 1             # The unique integer class ID
            }
        ]
        """

        u = self.userdata

        return list(u['classes'].values()) if u is not None\
            and 'classes' in u.keys() else []

    def add_images(self, uploadable_sources, data=None):  # noqa: C901
        """
        Add more images to a collection, given a set of uploadable_sources and
        optional data rows. See workspace.create_collection for details of
        these arguments. Note that the images won't appear in the collection
        unless rows are provided referencing them.
        """

        uploadable_sources = UploadableSource._parse_list(uploadable_sources)

        # If using multi-source, must provide data
        if data is None and len(uploadable_sources) > 1:
            raise ValueError(
                'If uploading more than one image source, data '
                'is required to correctly join different images from each'
            )

        # Parse data
        if type(data) is str:
            if not os.path.exists(data):
                raise FileNotFoundError('Data file "{}" doesn\'t exist'
                                        .format(data))

            # Check the file extension
            if data.split('.')[-1] == 'tsv':
                data = pd.read_csv(data, delimiter='\t')
            elif data.split('.')[-1] in ['xls', 'xlsx']:
                data = pd.read_excel(data)
            else:
                data = pd.read_csv(data)

        # Check that all source filenames exist in the provided data
        if data is not None:
            print('- Checking data matches uploadable sources')
            for s in uploadable_sources:
                s._check_in_data(data)

            # append rows to data
            new_rows = self.rows.append(data)
            self.replace_data(new_rows)

        # validate and register uploadable sources against existing sources
        for i, us in enumerate(uploadable_sources):
            us._register_source(i, self.sources[i])

        # upload
        for us in uploadable_sources:
            us._upload()

    @classes.setter
    def classes(self, classes):  # noqa: C901

        # Check for a valid classes list
        if type(classes) != list:
            raise TypeError(
                'Expected \'classes\' to be a list, not {}'
                .format(type(classes)))

        payload = {
            'classes': {}
        }

        for d in classes:
            # Check for a sensible class dict
            if type(d) != dict:
                raise TypeError(
                    'Expected \'classes\' entry to be a dict, not {}'
                    .format(type(d)))
            if len(d.keys()) != 3:
                raise ValueError(
                    'Expected classes dict to have 3 keys, not {} ({})'
                    .format(len(d.keys()), d))
            for k in ['color', 'name', 'id']:
                if k not in d.keys():
                    raise ValueError('Unexpected class key: {}. Keys must be '
                                     'color | name | id.'.format(k))

            # Format as the expected payload
            payload['classes'][d['id']] = {
                'color': str(d['color']),
                'name': str(d['name']),
                'id': str(int(d['id']))
            }

        # POST
        c = self.client
        url = '{}/{}/project/{}/collections/{}/userdata'.format(
            c.HOME, c.API_0, self.workspace_id, self.id)
        c._auth_post(url, json.dumps(payload))

        print('New classes set:')
        for d in self.classes:
            print(d)

    def _retrieve(self, key):
        """Retrieve a key from self._data, with a friendly error on failure."""

        if key not in self._data:
            raise KeyError(
                'Collection did not find requested key "{}" in its _data'
                .format(key))
        return self._data[key]

    def _get_imageset_id(self, source=0) -> str:
        """
        Returns imageset_id of the collection. If using V2+, can optionally
        provide a source for that source's imageset_id instead.
        """

        source = self._parse_source(source)

        return source.imageset_id

    def _join_id_to_lookup(self, join_id) -> list:
        """
        Given a join_id, provides the associated image-meta lookup for
        converting between image and row spaces.
        """

        # Type-check provided join_id
        if type(join_id) != str:
            raise TypeError(
                'Expected join_id to be str, not: {} ({})'
                .format(join_id, type(join_id)))

        # Obtain the dataset based on the join_id (dict)
        url = '{}/{}/project/{}/datasets/{}'.format(
            self.client.HOME, self.client.API_0, self.workspace_id, join_id)
        dataset = self.client._auth_get(url)['dataset']

        if 'imageset_indices' in dataset.keys():
            return dataset['imageset_indices']
        else:
            # Image only collection. Lookup should be n => n.
            # This is a bit of a hack, but works
            return {k: k for k in range(100000)}

    def _get_image_meta_lookup(self, source=0) -> list:
        """
        Returns the image-meta lookup for converting between image and row
        space. There is a lookup for each Source in V2 collections, so caching
        keeps track of the relevent Source() lookups by join_id.
        """

        source = self._parse_source(source)
        join_id = source._imageset_dataset_join_id

        # If already obtained and cached, return that
        if self.allow_caching and join_id in\
                self._cached_image_meta_source_lookups:
            return self._cached_image_meta_source_lookups[join_id]

        # No cached lookup, obtain it and potentially cache it
        lookup = self._join_id_to_lookup(join_id)
        if self.allow_caching:
            self._cached_image_meta_source_lookups[join_id] = lookup

        return lookup

    @staticmethod
    def _source_warning() -> None:
        print(
            'Warning - Called with a source when this is not a '
            'multi-image-source collection. Treating as if no source '
            'was required.')

    def get_annotations_as_dataframe(
            self, anno_type=None, source=0) -> pd.DataFrame:
        """
        Collects all annotations of a type (or all if anno_type=None) and
        returns the information as a dataframe.
        """

        source = self._parse_source(source)

        annos = self.get_annotations(
            anno_type=anno_type, source=source)

        classes = self.classes

        def to_dict(a):
            d = {}
            if classes and 'class_id' in a.keys():
                c = next(filter(lambda c: int(c['id']) == int(a['class_id']), classes))
                d['Class'] = c['name']
                d['Class ID'] = int(c['id'])
            d['Type'] = a['type']
            d['Author'] = a['author']
            d['Row Index'] = self.imageset_index_to_row_index(a['image_index'])
            d['Imageset Index'] = a['image_index']
            d['ID'] = a['id']
            if 'metadata' in a.keys():
                for k, v in a['metadata'].items():
                    d[k] = v
            return d

        df = pd.DataFrame([to_dict(a) for a in annos])

        return df

    def _parse_source(self, source) -> Source:  # noqa: C901
        """
        Accepts an int or a Source instance or source name and always returns a checked
        Source instance. If a V1 collection, always returns the one and only
        first source.
        """

        if self.version == 1:
            return self.sources[0]

        # If an index is given, check the index is sensible and return a Source
        if type(source) == int:
            if source < 0:
                raise ValueError(
                    'Expected source to be a positive int, not {}'
                    .format(source))
            if source >= len(self.sources):
                raise ValueError(
                    'Source not valid for number of available sources (index '
                    '{} for list length {})'
                    .format(source, len(self.sources)))
            return self.sources[source]

        # If a string is given, check the source name that matches
        if type(source) == str:
            for s in self.sources:
                if s.name == source:
                    return s
            raise ValueError('Cannot find a source with name {}'.format(source))

        # If a Source is given, check it belongs to this collection and return
        if not isinstance(source, Source):
            raise TypeError(
                'Provided source was neither an int nor a Source instance: {}'
                .format(source))

        try:
            s = next(filter(lambda s: s.imageset_id == source.imageset_id, self.sources))
        except StopIteration:
            raise ValueError(
                'Provided source with ID "{}" does not match any sources '
                'associated with this collection'.format(source.name))

        return s

    def add_source(self, source_name, root_imageset_id):
        """
        Accepts source name and root imageset id to add a source to the collection.
        """
        url = '{}/{}/project/{}/collections/{}/sources'.format(
            self.client.HOME, self.client.API_0, self.workspace_id, self.id)
        payload = {
            'name': source_name,
            'imageset_id': root_imageset_id
        }
        self.client._auth_post(url, json.dumps(payload))
