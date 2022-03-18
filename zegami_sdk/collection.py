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


class Collection():

    @staticmethod
    def _construct_collection(
            client, workspace, collection_dict, allow_caching=True):
        """
        Use this to instantiate Collection instances properly.

        Requires an instantiated ZegamiClient, a Workspace instance, and the
        data describing the collection.
        """

        if type(collection_dict) != dict:
            raise TypeError(
                'Expected collection_dict to be a dict, not {}'
                .format(collection_dict))

        v = collection_dict['version'] if 'version' in collection_dict.keys()\
            else 1

        if v == 1:
            return Collection(
                client, workspace, collection_dict, allow_caching)

        elif v == 2:
            return CollectionV2(
                client, workspace, collection_dict, allow_caching)

        else:
            raise ValueError('Unsupported collection version: {}'.format(v))

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
        self._check_data()
        self._check_version()

        # Caching
        self.allow_caching = allow_caching
        self.clear_cache()

    def clear_cache(self):
        self._cached_rows = None
        self._cached_image_meta_lookup = None
        self._cached_annotations_data = None

    @property
    def client():
        pass

    @client.getter
    def client(self):
        assert self._client, 'Client is not valid'
        return self._client

    @property
    def name():
        pass

    @name.getter
    def name(self) -> str:
        self._check_data()
        assert 'name' in self._data.keys(),\
            'Collection\'s data didn\'t have a \'name\' key'
        return self._data['name']

    @property
    def id():
        pass

    @id.getter
    def id(self):
        self._check_data()
        assert 'id' in self._data.keys(),\
            'Collection\'s data didn\'t have an \'id\' key'
        return self._data['id']

    @property
    def _dataset_id():
        pass

    @_dataset_id.getter
    def _dataset_id(self) -> str:
        self._check_data()
        assert 'dataset_id' in self._data.keys(),\
            'Collection\'s data didn\'t have a \'dataset_id\' key'
        return self._data['dataset_id']

    @property
    def _upload_dataset_id():
        pass

    @_upload_dataset_id.getter
    def _upload_dataset_id(self) -> str:
        self._check_data()
        assert 'upload_dataset_id' in self._data.keys(),\
            'Collection\'s data didn\'t have a \'upload_dataset_id\' key'
        return self._data['upload_dataset_id']

    @property
    def version():
        pass

    @version.getter
    def version(self):
        self._check_data()
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
        assert self.workspace is not None,\
            'Tried to get a workspace ID when no workspace was set'
        return self.workspace.id

    @property
    def url():
        pass

    @url.getter
    def url(self):
        return '{}/collections/{}-{}'.format(
            self.client.HOME, self.workspace_id, self.id)

    @property
    def sources():
        pass

    @sources.getter
    def sources(self):
        """
        Returns all Source() instances belonging to this collection. V1
        collections do not use sources, however a single pseudo source with
        the correct imageset information is returned.
        """

        if self.version < 2:
            return [Source(self, self._data)]

        if 'image_sources' not in self._data.keys():
            raise ValueError(
                "Expected to find 'image_sources' in collection but didn't: {}"
                .format(self._data))

        return [Source(self, s) for s in self._data['image_sources']]

    def show_sources(self):
        """Lists the IDs and names of all sources in the collection."""

        ss = self.sources
        print('\nImage sources ({}):'.format(len(ss)))
        for s in ss:
            print('{} : {}'.format(s.imageset_id, s.name))

    @property
    def rows():
        pass

    @rows.getter
    def rows(self):
        """Returns all data rows of the collection as a Pandas DataFrame."""
        if self.allow_caching and self._cached_rows is not None:
            return self._cached_rows

        c = self.client

        # Obtain the metadata bytes from Zegami
        url = '{}/{}/project/{}/datasets/{}/file'.format(
            c.HOME, c.API_0, self.workspace_id, self._dataset_id)
        r = c._auth_get(url, return_response=True)
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

    def add_feature_pipeline(self, name, steps, source=0):
        # get the source
        source = self._parse_source(source)

        # find the feature extraction node
        source_feature_extraction_node = None
        
        # Add each node in the list to the chain
        tip_node_id = source_feature_extraction_node

        for step in steps:
            node = nodes.add_node(
                self.client,
                self.workspace_id,
                action=step.action,
                params=step.params,
                type=None,  # TODO
                dataset_parents=None,
                imageset_parents=None,
                processing_category='image_clustering',
                node_group=self.node_group,
                name="",  # TODO
            )
            tip_node_id = node['id']

        # add node to map the output to row space
        join_dataset_id = source._data.get('imageset_dataset_join_id')
        mapping_node = nodes.add_node(
            self.client,
            self.workspace_id,
            'mapping',
            {},
            dataset_parents=[tip_node_id, join_dataset_id],
            name=name + " mapping",
            node_group=self.node_group,
            processing_category='image_clustering'
        )

        # add to the collection's output node
        output_dataset_id = self._data.get('output_dataset_id')
        nodes.add_parent(
            self.client,
            self.workspace_id,
            output_dataset_id,
            mapping_node.get('id')
        )

        # TODO generate snapshot


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
        assert type(tag_names) == list,\
            'Expected tag_names to be a list, not a {}'.format(type(tag_names))

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
                raise ValueError('Invalid rows argument, \'{}\' not supported'.format(type(rows)))
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

    def download_annotation(self, annotation_id):
        """
        Converts an annotation_id into downloaded annotation data.

        This will vary in content depending on the annotation type and
        format.
        """

        zc = self.client

        url = '{}/{}/project/{}/annotations/{}'.format(
            zc.HOME, zc.API_1, self.workspace.id, annotation_id)

        return zc._auth_get(url)

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

    def get_annotations(self, anno_type=None) -> dict:
        """
        Gets the annotations of a collection.
        Defaults to searching for annotations of all types.
        """

        c = self.client
        url = '{}/{}/project/{}/annotations/collection/{}'.format(
            c.HOME, c.API_1, self.workspace_id, self.id)

        if anno_type is not None:
            url += '?type=' + anno_type

        return c._auth_get(url)

    def get_annotations_for_image(
            self, row_index, source=None, anno_type=None) -> list:
        """
        Returns annotations for a single item in the collection.
        Defaults to searching for annotations of all types.
        """

        if source is not None:
            self._source_warning()

        assert type(row_index) == int and row_index >= 0,\
            'Expected row_index to be a positive int, not {}'.format(row_index)

        c = self.client
        lookup = self._get_image_meta_lookup()
        imageset_index = lookup[row_index]

        url = '{}/{}/project/{}/annotations/imageset/{}/images/{}'\
            .format(c.HOME, c.API_1, self.workspace_id,
                    self._get_imageset_id(), imageset_index)

        if anno_type is not None:
            url += '?type=' + anno_type

        return c._auth_get(url)

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

    def delete_all_annotations(self, source=0):
        """Deletes all annotations saved to the collection."""

        # A list of sources of annotations
        anno_sources = self.get_annotations()['sources']

        c = 0
        for i, source in enumerate(anno_sources):

            # A list of annotation objects
            annotations = source['annotations']
            if len(annotations) == 0:
                continue

            print('Deleting {} annotations from source {}'
                  .format(len(annotations), i))

            for j, annotation in enumerate(annotations):
                self.delete_annotation(annotation['id'])
                print('\r{}/{}'.format(j + 1, len(annotations)), end='',
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
        for s in uploadable_sources:
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

    def _check_data(self) -> None:

        if not self._data:
            raise ValueError('Collection had no self._data set')

        if type(self._data) != dict:
            raise TypeError(
                'Collection didn\'t have a dict for its data ({})'
                .format(type(self._data)))

    def _check_version(self) -> None:

        v = self.version
        class_v = 2 if isinstance(self, CollectionV2) else 1

        if v != class_v:
            raise ValueError(
                'Collection data indicates the class used to construct this '
                'collection is the wrong version. v{} class vs v{} data'
                .format(class_v, v))

    def _get_imageset_id(self, source=0) -> str:

        self._check_data()
        if 'imageset_id' not in self._data:
            raise KeyError(
                'Collection\'s data didn\'t have an \'imageset_id\' key')

        return self._data['imageset_id']

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
        """Used to convert between image and row space."""

        # If this has already been cached, return that
        if self.allow_caching and self._cached_image_meta_lookup:
            self._check_data()
            return self._cached_image_meta_lookup

        key = 'imageset_dataset_join_id'
        if key not in self._data.keys():
            raise KeyError(
                'Collection: Key "{}" not found in self._data'.format(key))

        join_id = self._data[key]
        lookup = self._join_id_to_lookup(join_id)

        # If caching, store it for later
        if self.allow_caching:
            self._cached_image_meta_lookup = lookup

        return lookup

    @staticmethod
    def _source_warning() -> None:
        print(
            'Warning - Called with a source when this is not a '
            'multi-image-source collection. Treating as if no source '
            'was required.')

    def __len__(self) -> int:
        self._check_data()
        key = 'total_data_items'
        if key not in self._data.keys():
            raise KeyError(
                'Collection\'s self._data was missing the key "{}"'
                .format(key))
        return self._data[key]

    def __repr__(self) -> str:
        return "<Collection id={} name={}>".format(self.id, self.name)

    def id_to_class(self, ID):
        return self.classes[ID - 1]['name']

    def get_annotations_metadata(self, anno_type=None) -> pd.DataFrame:
        """Compute pandas.DataFrame of annotations metadata from a collection.
        Defaults to searching for annotations of all types.
        """
        annos = self.get_annotations(anno_type=anno_type)['annotations']
        metadata = pd.DataFrame()
        for i, anno in enumerate(annos):
            row = {**{'Image': anno['image_index'],
                      'ID': anno['id'],
                      'Class': self.id_to_class(int(anno['class_id']))},
                   **anno['metadata']}
            metadata = metadata.append(row, ignore_index=True)
        return metadata


class CollectionV2(Collection):

    def clear_cache(self):
        super().clear_cache()
        self._cached_image_meta_source_lookups = {}

    @property
    def sources():
        pass

    @sources.getter
    def sources(self) -> list:
        """Returns all Source() instances belonging to this collection."""

        if 'image_sources' not in self._data.keys():
            raise KeyError(
                'Expected to find \'image_sources\' in collection but '
                'didn\'t: {}'.format(self._data))

        return [Source(self, s) for s in self._data['image_sources']]

    def show_sources(self):
        """Lists the IDs and names of all sources in the collection."""

        ss = self.sources
        print('\nImage sources ({}):'.format(len(ss)))
        for s in ss:
            print('{} : {}'.format(s.imageset_id, s.name))

    def get_annotations(self, source=0, anno_type=None) -> dict:
        """
        Gets annotations for a particular source of a collection.
        Defaults to searching for annotations of all types.
        """

        source = self._parse_source(source)

        url = '{}/{}/project/{}/annotations/collection/{}/source/{}'\
            .format(self.client.HOME, self.client.API_1, self.workspace_id,
                    self.id, source.id)
        if anno_type is not None:
            url += '?type=' + anno_type

        return self.client._auth_get(url)

    def get_annotations_for_image(
            self, row_index, source=0, anno_type=None) -> list:
        """
        Returns annotations for a single item in the collection.
        Defaults to searching for annotations of all types.
        """

        # Parse the source for a valid Source() instance
        source = self._parse_source(source)

        # Determine the imageset index
        imageset_index = self.imageset_index_to_row_index(row_index, source)

        # Download and return annotations of the requested type
        url = '{}/{}/project/{}/annotations/imageset/{}/images/{}'\
            .format(self.client.HOME, self.client.API_1, self.workspace_id,
                    self._get_imageset_id(), imageset_index)
        if anno_type is not None:
            url += '?type=' + anno_type

        return self.client._auth_get(url)

    def _get_imageset_id(self, source=0) -> str:
        """
        Source can be an int or a Source instance associated with this
        collection.
        """

        self._check_data()
        self._check_version()
        source = self._parse_source(source)

        return source.imageset_id

    def _get_image_meta_lookup(self, source=0) -> list:
        """
        Returns the image-meta lookup for converting between image and row
        space. There is a lookup for each Source in V2 collections, so caching
        keeps track of the relevent Source() lookups by join_id.
        """

        self._check_data()
        self._check_version()

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

    def _parse_source(self, source):
        """
        Accepts an int or a Source instance and always returns a checked
        Source instance.
        """

        ss = self.sources

        # If an index is given, check the index is sensible and return a Source
        if type(source) == int:
            if source < 0:
                raise ValueError(
                    'Expected source to be a positive int, not {}'
                    .format(source))
            if source >= len(ss):
                raise ValueError(
                    'Source not valid for number of available sources (index '
                    '{} for list length {})'
                    .format(source, len(ss)))
            return ss[source]

        # If a Source is given, check it belongs to this collection and return
        if not isinstance(source, Source):
            raise TypeError(
                'Provided source was neither an int nor a Source instance: {}'
                .format(source))

        for s in ss:
            if s.id == source.id:
                return source

        raise Exception('Provided source was a Source instance, but didn\'t '
                        'belong to this collection ({})'.format(self.name))

    def __repr__(self) -> str:
        return "<Collection V2 id={} name={}>".format(self.id, self.name)
