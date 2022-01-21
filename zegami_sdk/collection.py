# -*- coding: utf-8 -*-
# Copyright 2021 Zegami Ltd

"""collection functionality."""

from concurrent.futures import as_completed, ThreadPoolExecutor
from io import BytesIO
import json
import os
from time import time

import pandas as pd
from PIL import Image

from .source import Source, UploadableSource


class Collection():

    @staticmethod
    def _construct_collection(client, workspace, collection_dict, allow_caching=True):
        """Use this to instantiate Collection instances.

        Requires an instantiated ZegamiClient, a Workspace instance, and the data
        describing the collection.
        """
        assert type(collection_dict) == dict,\
            'Expected collection_dict to be a dict, not {}'.format(collection_dict)
        v = collection_dict['version'] if 'version' in collection_dict.keys() else 1
        if v == 1:
            return Collection(client, workspace, collection_dict, allow_caching)
        elif v == 2:
            return CollectionV2(client, workspace, collection_dict, allow_caching)

    def __init__(self, client, workspace, collection_dict, allow_caching=True):
        """!! STOP !! Don't instantiate using Collection(x, y, z).

        Use Collection._construct_collection(x, y, z) instead.
        It will determine which subclass is appropriate.
        """
        self._client = client
        self._data = collection_dict
        self._workspace = workspace
        self._check_data()
        self._check_version()

        # Caching
        self.allow_caching = allow_caching
        self._cached_rows = None
        self._cached_image_meta_lookup = None

    def clear_cache(self):
        self._cached_rows = None
        self._cached_image_meta_lookup = None

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
        if self.version < 2:
            print('{} is an old-style collection and does not support multiple image sources'.format(self.name))
            #  return a constructed pseudo source object in order to provide a consistant interface
            return [Source(self, self._data)]

    def show_sources(self):
        print('{} is an old-style collection and does not support multiple image sources'.format(self.name))

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
                print('Warning - failed to open metadata as a dataframe, returned '
                      'the tsv bytes instead.')
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
        details_url = f'{self.client.HOME}/{self.client.API_0}/project/{self.workspace_id}/collections/{self.id}'
        resp = self.client._auth_get(details_url)
        return resp['collection']['status']

    def get_rows_by_filter(self, filters):
        """Gets rows of metadata in a collection by a flexible filter.

        The filter should be a dictionary describing what to permit through
        any specified columns.

        Example:
            row_filter = { 'breed': ['Cairn', 'Dingo'] }

        This would only return rows whose 'breed' column matches 'Cairn'
        or 'Dingo'.
        """
        assert type(filters) == dict, 'Filters should be a dict.'
        rows = self.rows

        for fk, fv in filters.items():
            if not type(fv) == list:
                fv = [fv]
            rows = rows[rows[fk].isin(fv)]
        return rows

    def get_rows_by_tags(self, tag_names):
        """Gets rows of metadata in a collection by a list of tag_names.

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

    def get_image_urls(self, rows, source=0, generate_signed_urls=False,
                       signed_expiry_days=None, override_imageset_id=None):
        """Converts rows into their corresponding image URLs.

        If generate_signed_urls is false the URLs require a token to download
        These urls can be passed to download_image()/download_image_batch().

        If generate_signed_urls is true the urls can be used to fetch the images directly
        from blob storage, using a temporary access signature with an optionally specified lifetime.

        By default the uploaded images are fetched, but it's possible to fech e.g. the thumbnails
        only, by providing an alternative imageset id.
        """
        # Turn the provided 'rows' into a list of ints
        if type(rows) == pd.DataFrame:
            indices = list(rows.index)

        elif type(rows) == list:
            indices = [int(r) for r in rows]
        elif type(rows) == int:
            indices = [rows]
        else:
            raise ValueError('Invalid rows argument, \'{}\' not supported'.format(type(rows)))

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
            get_signed_urls = ['{}/{}/project/{}/imagesets/{}/images/{}/signed_route{}'.format(
                c.HOME, c.API_0, self.workspace_id, imageset_id,
                i, query
            ) for i in imageset_indices]
            signed_route_urls = []
            for url in get_signed_urls:
                # Unjoined rows will have None. Possibly better to filter these out earlier, but this works
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

    def replace_data(self, data):
        """
        Replaces the data in the collection.

        The provided input should be a pandas dataframe or a local
        csv/json/tsv/txt/xlsx/xls file. If a xlsx/xls file is used only data
        from the default sheet will be fetched. The rows take time to be
        updated, hence checking the status of the collection with coll.status,
        might be helpful if you need to ensure that you're using the updated
        data rows.
        """
        if type(data) == pd.DataFrame:
            tsv = data.to_csv(sep='\t', index=False)
            upload_data = bytes(tsv, 'utf-8')
            name = 'provided_as_dataframe.tsv'
        else:
            name = os.path.split(data)[-1]
            if name.split('.')[-1] in ['csv', 'json', 'tsv', 'txt', 'xls', 'xlsx']:
                with open(data, 'rb') as f:
                    upload_data = f.read()
            else:
                raise ValueError("File extension must one of these: csv, json, tsv, txt, xls, xlsx")

        zeg_client = self.client
        upload_dataset_url = (
            f"{zeg_client.HOME}/{zeg_client.API_0}/project/"
            f"{self.workspace_id}/datasets/{self._upload_dataset_id}"
        )
        mime_type = 'application/octet-stream'

        # create blob storage and upload to it
        urls, id_set = zeg_client._obtain_signed_blob_storage_urls(
            self.workspace_id,
            blob_path=f'datasets/{self._upload_dataset_id}'
        )
        blob_id = id_set['ids'][0]
        url = urls[blob_id]
        zeg_client._upload_to_signed_blob_storage_url(upload_data, url, mime_type)

        # update the upload dataset details
        current_dataset = zeg_client._auth_get(upload_dataset_url)["dataset"]
        current_dataset["source"]["upload"]["name"] = name
        current_dataset["source"]["blob_id"] = blob_id

        # returning response is true as otherwise it will try to return json but this response is empty
        zeg_client._auth_put(upload_dataset_url, body=None, return_response=True, json=current_dataset)

        self._cached_rows = None

    def save_image(self, url, target_folder_path='./', filename='image', extension='png'):
        """
        Downloads an image and saves to disk

        For input, see Collection.get_image_urls().
        """
        if not os.path.exists(target_folder_path):
            os.makedirs(target_folder_path)

        r = self.client._auth_get(url, return_response=True, stream=True)
        with open(target_folder_path + '/' + filename + '.' + extension, 'wb') as f:
            f.write(r.content)

    def save_image_batch(self, urls, target_folder_path='./', extension='png', max_workers=50, show_time_taken=True):
        """
        Downloads a batch of images and saves to disk.

        Filenames are the imageset index followed by the specified extension.
        For input, see Collection.get_image_urls().
        """
        def save_single(index, url):
            self.save_image(url, target_folder_path, filename=str(index), extension=extension)
            return index

        t = time()
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = [ex.submit(save_single, i, u) for i, u in enumerate(urls)]

            images = {}
            for f in as_completed(futures):
                i = f.result()
                images[i] = 1

                if show_time_taken:
                    print('\nDownloaded {} images in {:.2f} seconds.'.format(len(images), time() - t))

    def download_image(self, url):
        """
        Downloads an image into memory as a PIL.Image.

        For input, see Collection.get_image_urls().
        """
        r = self.client._auth_get(url, return_response=True, stream=True)
        r.raw.decode = True
        return Image.open(r.raw)

    def download_image_batch(self, urls, max_workers=50, show_time_taken=True):
        """
        Downloads multiple images into memory (each as a PIL.Image) concurrently.

        Please be aware that these images are being downloaded into memory,
        if you download a huge collection of images you may eat up your
        RAM!
        """
        def download_single(index, url):
            return (index, self.download_image(url))

        t = time()
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = [ex.submit(download_single, i, u) for i, u in enumerate(urls)]

            images = {}
            for f in as_completed(futures):
                i, img = f.result()
                images[i] = img

                if show_time_taken:
                    print('\nDownloaded {} images in {:.2f} seconds.'.format(
                        len(images), time() - t))

        # Results are a randomly ordered dictionary of results, so reorder them
        ordered = []
        for i in range(len(images)):
            ordered.append(images[i])
        return ordered

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
        """Parses tag indices into a list of tags, each with an list of indices."""
        tags = {}
        for record in tag_records:
            if record['tag'] not in tags.keys():
                tags[record['tag']] = []
            tags[record['tag']].append(record['key'])
        return tags

    def get_annotations(self, anno_type='mask') -> dict:
        """
        Returns one type of annotations attached to the collection.

        Default as mask annotations.
        """
        c = self.client
        url = '{}/{}/project/{}/annotations/collection/{}?type={}'.format(
            c.HOME, c.API_1, self.workspace_id, self.id, anno_type)

        return c._auth_get(url)

    def get_annotations_for_image(self, row_index, source=None, anno_type='mask') -> list:
        """Returns one type of annotations for a single item in the collection. Default as mask annotations."""
        if source is not None:
            self._source_warning()

        assert type(row_index) == int and row_index >= 0,\
            'Expected row_index to be a positive int, not {}'.format(row_index)

        c = self.client
        lookup = self._get_image_meta_lookup()
        imageset_index = lookup[row_index]

        url = '{}/{}/project/{}/annotations/imageset/{}/images/{}?type={}'\
            .format(c.HOME, c.API_1, self.workspace_id, self._get_imageset_id(), imageset_index, anno_type)

        return c._auth_get(url)

    def upload_annotation(self, uploadable, row_index=None, image_index=None, source=None, author=None, debug=False):
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
        """Delete an annotation by its ID. These are obtainable using the get_annotations...() methods."""
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
        url = '{}/{}/project/{}/collections/{}'.format(c.HOME, c.API_0, self.workspace_id, self.id)
        data = c._auth_get(url)['collection']
        userdata = data['userdata'] if 'userdata' in data.keys() else None
        return userdata

    def set_userdata(self, data):
        """ Additively sets userdata. To remove data set its value to None. """
        c = self.client
        url = '{}/{}/project/{}/collections/{}/userdata'.format(c.HOME, c.API_0, self.workspace_id, self.id)
        userdata = c._auth_post(url, json.dumps(data))
        return userdata

    @property
    def classes():
        pass

    @classes.getter
    def classes(self) -> list:
        """
        Property for the class configuration of the collection.

        Used in an annotation workflow to tell Zegami how to treat defined classes.

        To set new classes, provide a list of class dictionaries of shape:

        collection.classes = [
            {
                'color' : '#32a852',    # A hex color for the class
                'name'  : 'Dog',        # A human-readable identifier for the class
                'id'    : 0             # The unique integer class ID
            },
            {
                'color' : '#43f821',    # A hex color for the class
                'name'  : 'Cat',        # A human-readable identifier for the class
                'id'    : 1             # The unique integer class ID
            }
        ]
        """
        u = self.userdata
        return list(u['classes'].values()) if u is not None and 'classes' in u.keys() else []

    def add_images(self, uploadable_sources, data=None):  # noqa: C901
        """
        Add more images to a collection, given a set of uploadable_sources and optional data rows.
        See workspace.create_collection for details of these arguments.
        Note that the images won't appear in the collection unless rows are provided referencing them.
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
                raise FileNotFoundError('Data file "{}" doesn\'t exist'.format(data))
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
            raise TypeError('Expected \'classes\' to be a list, not {}'.format(type(classes)))

        payload = {
            'classes': {}
        }

        for d in classes:
            # Check for a sensible class dict
            if type(d) != dict:
                raise TypeError('Expected \'classes\' entry to be a dict, not {}'.format(type(d)))
            if len(d.keys()) != 3:
                raise ValueError('Expected classes dict to have 3 keys, not {} ({})'.format(len(d.keys()), d))
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
        url = '{}/{}/project/{}/collections/{}/userdata'.format(c.HOME, c.API_0, self.workspace_id, self.id)
        c._auth_post(url, json.dumps(payload))

        print('New classes set:')
        for d in self.classes:
            print(d)

    def _check_data(self) -> None:
        assert self._data, 'Collection had no self._data set'
        assert type(self._data) == dict,\
            'Collection didn\'t have a dict for its data ({})'.format(type(self._data))

    def _check_version(self) -> None:
        v = self.version
        class_v = 2 if isinstance(self, CollectionV2) else 1

        assert v == class_v, 'Collection data indicates the class used to '\
            'construct this collection is the wrong version. v{} class vs v{} data'\
            .format(class_v, v)

    def _get_imageset_id(self, source=0) -> str:
        self._check_data()
        assert 'imageset_id' in self._data,\
            'Collection\'s data didn\'t have an \'imageset_id\' key'
        return self._data['imageset_id']

    def _join_id_to_lookup(self, join_id) -> list:
        c = self.client
        assert type(join_id) == str, 'Expected join_id to be string: {}'.format(join_id)
        url = '{}/{}/project/{}/datasets/{}'.format(c.HOME, c.API_0, self.workspace_id, join_id)
        dataset = c._auth_get(url)['dataset']

        if 'imageset_indices' in dataset.keys():
            return dataset['imageset_indices']
        else:
            # image only collection. Lookup should be n => n.
            # This is a bit of a hack, but works
            return {k: k for k in range(100000)}

        return dataset['imageset_indices']

    def _get_image_meta_lookup(self, source=0) -> list:
        if self.allow_caching and self._cached_image_meta_lookup:
            self._check_data()

        key = 'imageset_dataset_join_id'
        assert key in self._data.keys(),\
            'Collection\'s data didn\'t contain \'{}\''.format(key)

        join_id = self._data[key]

        return self._join_id_to_lookup(join_id)

    @staticmethod
    def _source_warning() -> None:
        print(
            'Warning - Called with a source when this is not a '
            'multi-image-source collection. Treating as if no source '
            'was required.')

    def __len__(self) -> int:
        self._check_data()
        assert 'total_data_items' in self._data.keys(),\
            'Collection\'s data didn\'t have a \'total_data_items\' key'
        return self._data['total_data_items']

    def __repr__(self) -> str:
        return "<Collection id={} name={}>".format(self.id, self.name)


class CollectionV2(Collection):

    @property
    def sources():
        pass

    @sources.getter
    def sources(self) -> list:
        assert 'image_sources' in self._data.keys(),\
            'Expected to find \'image_sources\' in collection but didn\'t: {}'\
            .format(self._data)
        return [Source(self, s) for s in self._data['image_sources']]

    def show_sources(self):
        """Lists the IDs and names of all sources in the collection."""
        ss = self.sources
        print('\nImage sources ({}):'.format(len(ss)))
        for s in ss:
            print('{} : {}'.format(s.imageset_id, s.name))

    def get_annotations(self, source=0, anno_type='mask') -> list:
        """
        Gets one type of annotations for a particular source of a collection.

        Default as mask annotations.
        """
        c = self.client
        source = self._parse_source(source)

        url = '{}/{}/project/{}/annotations/collection/{}/source/{}?type={}'.format(
            c.HOME, c.API_1, self.workspace_id, self.id, source.id, anno_type)
        return c._auth_get(url)

    def get_annotations_for_image(self, row_index, source=0, anno_type='mask') -> list:
        """
        Returns one type of annotations for a single item in the collection.

        Default as mask annotations.
        """
        c = self.client
        source = self._parse_source(source)

        lookup = self._get_image_meta_lookup(source)
        imageset_index = lookup[row_index]
        url = '{}/{}/project/{}/annotations/imageset/{}/images/{}?type={}'.format(
            c.HOME, c.API_1, self.workspace_id, self._get_imageset_id(), imageset_index, anno_type)

        return c._auth_get(url)

    def _get_imageset_id(self, source=0) -> str:
        """Source can be an int or a Source instance from this collection."""
        self._check_data()
        self._check_version()
        return self._parse_source(source).imageset_id

    def _get_image_meta_lookup(self, source=0) -> list:
        self._check_data()
        join_id = self._parse_source(source)._imageset_dataset_join_id
        return self._join_id_to_lookup(join_id)

    def _parse_source(self, source):
        """Accepts an int or a Source instance and always returns a checked Source instance."""
        ss = self.sources

        # If an index is given, check the index is sensible and return a Source
        if type(source) == int:
            assert source >= 0,\
                'Expected source to be a positive int'
            assert source < len(ss),\
                'Source not valid for number of available sources (index {} for list length {})'\
                .format(source, len(ss))
            return ss[source]

        # If a Source is given, check it belongs to this collection and return
        assert isinstance(source, Source), 'Provided source was neither an '
        'int nor a Source instance: {}'.format(source)

        for s in ss:
            if s.id == source.id:
                return source

        raise Exception('Provided source was a Source instance, but didn\'t '
                        'belong to this collection ({})'.format(self.name))

    def __repr__(self) -> str:
        return "<Collection V2 id={} name={}>".format(self.id, self.name)
