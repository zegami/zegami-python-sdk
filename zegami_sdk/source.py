# -*- coding: utf-8 -*-
# Copyright 2021 Zegami Ltd

"""collection source functionality."""

from concurrent.futures import as_completed, ThreadPoolExecutor
from glob import glob
import json
import os
from tqdm import tqdm


class Source():
    """
    A data structure representing information about a subset of a collection.
    V1 collections have one source, V2+ can contain multiple each with their
    own imagesets.
    """

    def __repr__(self):
        return '<Source "{}" from Collection "{}", id: "{}"'.format(
            self.name, self.collection.name, self.id)

    def __init__(self, collection, source_dict):
        self._collection = collection
        self._data = source_dict

    @property
    def collection():
        pass

    @collection.getter
    def collection(self):
        return self._collection

    @property
    def name():
        pass

    @name.getter
    def name(self):
        return self._retrieve('name')

    @property
    def id():
        pass

    @id.getter
    def id(self):
        """The .source_id of this Source. Note: Invalid for V1 collections."""

        if self.collection.version < 2:
            return None
        return self._retrieve('source_id')

    @property
    def imageset_id():
        pass

    @imageset_id.getter
    def imageset_id(self):
        return self._retrieve('imageset_id')

    @property
    def index():
        pass

    @index.getter
    def index(self) -> int:
        """
        The index/position of this source in its collection's .sources list.
        """

        return self.collection.sources.index(self)

    @property
    def _imageset_dataset_join_id():
        pass

    @_imageset_dataset_join_id.getter
    def _imageset_dataset_join_id(self):
        return self._retrieve('imageset_dataset_join_id')

    def _retrieve(self, key):
        if key not in self._data:
            raise KeyError('Key "{}" not found in Source _data'.format(key))
        return self._data[key]

    @property
    def image_details():
        pass

    @image_details.getter
    def image_details(self):

        collection = self.collection
        c = collection.client

        ims_url = '{}/{}/project/{}/nodes/{}/images'.format(
            c.HOME, c.API_1, collection.workspace_id, self.imageset_id)
        ims = c._auth_get(ims_url)

        return ims


class UploadableSource():

    IMAGE_MIMES = {
        ".bmp": "image/bmp",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png": "image/png",
        ".gif": "image/gif",
        ".tif": "image/tiff",
        ".tiff": "image/tiff",
        ".dcm": "application/dicom",
    }

    BLACKLIST = (
        ".yaml",
        ".yml",
        "thumbs.db",
        ".ds_store",
        ".dll",
        ".sys",
        ".txt",
        ".ini",
        ".tsv",
        ".csv",
        ".json"
    )

    def __init__(self, name, image_dir, column_filename='__auto_join__', recursive_search=True, filename_filter=[],
                 additional_mimes={}):
        """
        Used in conjunction with create_collection().

        An UploadableSource() points towards and manages the upload of local
        files, resulting in the generation of a true Source() in the
        collection.

        To limit to an allowed specific list of filenames, provide
        'filename_filter'. This filter will check against
        os.path.basename(filepath).

        Common mime types are inferred from the file extension,
        but a dict of additional mime type mappings can be provided eg to
        cater for files with no extension.
        """

        self.name = name
        self.image_dir = image_dir
        self.column_filename = column_filename

        # Set externally once a blank collection has been made
        self._source = None
        self._index = None

        self.image_mimes = {**UploadableSource.IMAGE_MIMES, **additional_mimes}

        # Check the directory exists
        if not os.path.exists(image_dir):
            raise FileNotFoundError('image_dir "{}" does not exist'.format(self.image_dir))
        if not os.path.isdir(image_dir):
            raise TypeError('image_dir "{}" is not a directory'.format(self.image_dir))

        # Potentially limit paths based on filename_filter.
        if filename_filter:
            if type(filename_filter) != list:
                raise TypeError('filename_filter should be a list')
            fps = [os.path.join(image_dir, fp) for fp in filename_filter if os.path.exists(os.path.join(image_dir, fp))]

        else:
            # Find all files matching the allowed mime-types
            fps = sum(
                [glob('{}/**/*{}'.format(image_dir, ext), recursive=recursive_search)
                 for ext in self.IMAGE_MIMES.keys()], [])

        self.filepaths = fps
        self.filenames = [os.path.basename(fp) for fp in self.filepaths]

        print('UploadableSource "{}" found {} images in "{}"'.format(self.name, len(self), image_dir))

    @property
    def source():
        pass

    @source.getter
    def source(self) -> Source:
        """An UploadableSource() is the gateway to uploading into a true Zegami collection Source().

        Once a collection is created and an empty Source() exists, this reference points to it ready to upload to.
        """
        if self._source is None:
            raise Exception(
                'UploadableSource\'s generated source has not been set yet. This should be done automatically '
                'after the blank collection has been generated.'
            )
        return self._source

    @property
    def index():
        pass

    @index.getter
    def index(self) -> int:
        """The source index this UploadableSource is for.

        Only set after a blank source has been generated ready to be uploaded to.
        """
        if self._index is None:
            raise Exception('UploadableSource\'s generated source index has '
                            'not been set yet. This should be done '
                            'automatically after the blank collection has '
                            'been generated')
        return self._index

    @property
    def imageset_id():
        pass

    @imageset_id.getter
    def imageset_id(self):
        return self.source.imageset_id

    def __len__(self):
        return len(self.filepaths)

    def _register_source(self, index, source):
        """Called to register a new (empty) Source() from a new collection to this, ready for uploading data into."""
        if type(index) is not int:
            raise TypeError('index should be an int, not {}'.format(type(index)))
        if repr(type(source)) != repr(Source):
            raise TypeError('source should be a Source(), not {}'.format(type(source)))

        self._index = index
        self._source = source

        if not self.source.name == 'None' and not self.source.name == self.name:
            raise Exception(
                'UploadableSource "{}" registered to Source "{}" when their names should match'
                .format(self.name, self.source.name)
            )

    def _assign_images_to_smaller_lists(self, file_paths, start=0):
        """Create smaller lists based on the number of images in the directory."""
        # Recurse and pick up only valid files (either with image extensions, or not on blacklist)
        total_work = len(file_paths)
        workloads = []
        workload = []
        workload_start = start

        if total_work > 2500:
            size = 100
        elif total_work < 100:
            size = 1
        else:
            size = 10

        i = 0
        while i < total_work:
            path = file_paths[i]
            workload.append(path)
            i += 1
            if len(workload) == size or i == total_work:
                workloads.append({'paths': workload, 'start': workload_start})
                workload = []
                workload_start = start + i

        return workloads, total_work, size

    def get_threaded_workloads(self, executor, workloads):
        threaded_workloads = []
        for workload in workloads:
            threaded_workloads.append(executor.submit(
                self._upload_image_group,
                workload['paths'],
                workload['start']
            ))
        return threaded_workloads

    def _upload(self):
        """Uploads all images by filepath to the collection.

        provided a Source() has been generated and designated to this instance.
        """
        collection = self.source.collection
        c = collection.client

        print('- Uploadable source {} "{}" beginning upload'.format(self.index, self.name))

        # Tell the server how many uploads are expected for this source
        url = '{}/{}/project/{}/imagesets/{}/extend'.format(c.HOME, c.API_0, collection.workspace_id, self.imageset_id)
        delta = len(self)
        # If there are no new uploads, ignore.
        if delta == 0:
            print('No new data to be uploaded.')
            return
        resp = c._auth_post(url, body=None, json={'delta': delta})
        new_size = resp['new_size']
        start = new_size - delta

        (workloads, total_work, group_size) = self._assign_images_to_smaller_lists(self.filepaths, start=start)

        # Multiprocess upload the images
        # divide the filepaths into smaller groups
        # with ThreadPoolExecutor() as ex:
        CONCURRENCY = 16
        with ThreadPoolExecutor(CONCURRENCY) as executor:
            threaded_workloads = self.get_threaded_workloads(executor, workloads)
            kwargs = {
                'total': len(threaded_workloads),
                'unit': 'image',
                'unit_scale': group_size,
                'leave': True
            }
            for f in tqdm(as_completed(threaded_workloads), **kwargs):
                if f.exception():
                    raise f.exception()

    def _upload_image_group(self, paths, start_index):
        """Upload a group of images.

        Item is a tuple comprising:
            - blob_id
            - blob_url
            - file path
        """
        coll = self.source.collection
        c = coll.client

        # Obtain blob storage information
        blob_storage_urls, id_set = c._obtain_signed_blob_storage_urls(
            coll.workspace_id, id_count=len(paths), blob_path="imagesets/{}".format(self.imageset_id))

        # Check that numbers of values are still matching
        if not len(paths) == len(blob_storage_urls):
            raise Exception(
                'Mismatch in blob urls count ({}) to filepath count ({})'
                .format(len(blob_storage_urls), len(self))
            )

        bulk_info = []
        for (i, path) in enumerate(paths):
            mime_type = self._get_mime_type(path)
            blob_id = id_set['ids'][i]
            blob_url = blob_storage_urls[blob_id]
            bulk_info.append({
                'blob_id': blob_id,
                'name': os.path.basename(path),
                'size': os.path.getsize(path),
                'mimetype': mime_type
            })
            self._upload_image(c, path, blob_url, mime_type)

        # Upload bulk image info
        url = (
            f'{c.HOME}/{c.API_0}/project/{coll.workspace_id}/imagesets/{self.imageset_id}'
            f'/images_bulk?start={start_index}'
        )
        c._auth_post(url, body=None, return_response=True, json={'images': bulk_info})

    def _upload_image(self, client, path, blob_url, mime_type):
        """Uploads a single image to the collection."""
        try:
            with open(path, 'rb') as f:
                client._upload_to_signed_blob_storage_url(f, blob_url, mime_type)
        except Exception as e:
            print('Error uploading "{}" to blob storage:\n{}'.format(path, e))

    def _check_in_data(self, data):
        cols = list(data.columns)
        if self.column_filename != '__auto_join__' and self.column_filename not in cols:
            raise Exception('Source "{}" had the filename_column "{}" '
                            'which is not a column of the provided data:\n{}'
                            .format(self.name, self.column_filename, cols))

    @classmethod
    def _parse_list(cls, uploadable_sources) -> list:
        """Returns a checked list of instances."""
        if isinstance(uploadable_sources, cls):
            uploadable_sources = [uploadable_sources]
        elif type(uploadable_sources) is not list:
            raise TypeError('uploadable_sources should be a list of UploadableSources')

        for u in uploadable_sources:
            if not isinstance(u, UploadableSource):
                raise TypeError('uploadable_sources should be a list of source.UploadableSource() instances')

        names = [u.name for u in uploadable_sources]
        for name in names:
            if names.count(name) > 1:
                raise ValueError('Two or more sources share the name "{}"'.format(name))

        return uploadable_sources

    def _get_mime_type(self, path) -> str:
        """Gets the mime_type of the path. Raises an error if not a valid image mime_type."""
        if '.' not in path:
            return self.image_mimes['']
        ext = os.path.splitext(path)[-1]
        if ext in self.image_mimes.keys():
            return self.image_mimes[ext]
        raise TypeError('"{}" is not a supported image mime_type ({})'.format(path, self.image_mimes))


class UrlSource(UploadableSource):

    def __init__(self, name, url_template, image_fetch_headers, column_filename=None):
        """Used in conjunction with create_collection().

        A UrlSource() fetches the images from the url template given, resulting in the
        generation of a true Source() in the collection.
        """
        self.name = name
        self.url_template = url_template
        self.image_fetch_headers = image_fetch_headers
        self.column_filename = column_filename

        # Set externally once a blank collection has been made
        self._source = None
        self._index = None

    def _upload(self):
        """Update upload imageset to use the provided url template to get the images.

        provided a Source() has been generated and designated to this instance.
        """
        collection = self.source.collection
        c = collection.client

        print('- Configuring source {} "{}" to fetch images from url'
              .format(self.index, self.name))

        upload_ims_url = '{}/{}/project/{}/imagesets/{}'.format(
            c.HOME, c.API_0, collection.workspace_id, self.imageset_id)
        upload_ims = c._auth_get(upload_ims_url)

        new_source = {
            "dataset_id": collection._dataset_id,
            'fetch': {
                'headers': self.image_fetch_headers,
                'url': {
                    'dataset_column': self.column_filename,
                    'url_template': self.url_template,
                }
            }
        }
        upload_ims['imageset']['source'] = new_source
        payload = json.dumps(upload_ims['imageset'])
        r = c._auth_put(upload_ims_url, payload, return_response=True)

        return r
