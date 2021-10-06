# -*- coding: utf-8 -*-
# Copyright 2021 Zegami Ltd

"""collection source functionality."""

from concurrent.futures import as_completed, ThreadPoolExecutor
from glob import glob
import os

from tqdm import tqdm


class Source():

    def __init__(self, collection, source_dict):
        self._collection = collection
        self._data = source_dict

    @property
    def name():
        pass

    @name.getter
    def name(self):
        assert self._data, 'Source had no self._data set'
        assert 'name' in self._data.keys(), 'Source\'s data didn\'t have a \'name\' key'
        return self._data['name']

    @property
    def collection():
        pass

    @collection.getter
    def collection(self):
        return self._collection

    @property
    def id():
        pass

    @id.getter
    def id(self):
        assert self._data, 'Source had no self._data set'
        assert 'source_id' in self._data, 'Source\'s data didn\'t have a \'source_id\' key'
        return self._data['source_id']

    @property
    def imageset_id():
        pass

    @imageset_id.getter
    def imageset_id(self):
        assert self._data, 'Source had no self._data set'
        assert 'imageset_id' in self._data, 'Source\'s data didn\'t have an \'imageset_id\' key'
        return self._data['imageset_id']

    @property
    def _imageset_dataset_join_id():
        pass

    @_imageset_dataset_join_id.getter
    def _imageset_dataset_join_id(self):
        k = 'imageset_dataset_join_id'
        assert self._data, 'Source had no self._data set'
        assert k in self._data, 'Source\'s data didn\'t have an \'{}\' key'.format(k)
        return self._data[k]


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

    def __init__(self, name, image_dir, column_filename='Filename', recursive_search=True):
        """Used in conjunction with create_collection().

        An UploadableSource() points towards and manages the upload of local files, resulting in the
        generation of a true Source() in the collection.
        """
        self.name = name
        self.image_dir = image_dir
        self.column_filename = column_filename

        # Set externally once a blank collection has been made
        self._source = None
        self._index = None

        # Check the directory exists
        if not os.path.exists(image_dir):
            raise FileNotFoundError('image_dir "{}" does not exist'.format(self.image_dir))
        if not os.path.isdir(image_dir):
            raise TypeError('image_dir "{}" is not a directory'.format(self.image_dir))

        # Find all files matching the allowed mime-types
        self.filepaths = sum(
            [glob('{}/**/*{}'.format(image_dir, ext), recursive=recursive_search)
                for ext in self.IMAGE_MIMES.keys()], [])

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

        if not self.source.name == self.name:
            raise Exception(
                'UploadableSource "{}" registered to Source "{}" when their names should match'
                .format(self.name, self.source.name)
            )

    def _upload(self):
        """Uploads all images by filepath to the collection.

        provided a Source() has been generated and designated to this instance.
        """
        collection = self.source.collection
        c = collection.client

        print('- Uploadable source {} "{}" beginning upload'.format(self.index, self.name))

        # Tell the server how many uploads are expected for this source
        url = '{}/{}/project/{}/imagesets/{}/extend'.format(c.HOME, c.API_0, collection.workspace_id, self.imageset_id)
        c._auth_post(url, body=None, json={'delta': len(self)})

        # Obtain blob storage information
        blob_storage_urls, id_set = c._obtain_signed_blob_storage_urls(
            collection.workspace_id, id_count=len(self), blob_path=f'imaegsets/{self.imageset_id}')

        # Check that numbers of values are still matching
        if not len(self) == len(blob_storage_urls):
            raise Exception(
                'Mismatch in blob urls count ({}) to filepath count ({})'.format(len(blob_storage_urls), len(self))
            )

        # Multiprocess upload the images
        bulk_info = []
        with ThreadPoolExecutor() as ex:

            # Submit the upload jobs
            futures = []
            for i, path in enumerate(self.filepaths):
                blob_id = id_set['ids'][i]
                blob_url = blob_storage_urls[blob_id]
                mime_type = self._get_mime_type(path)
                bulk_info.append({
                    'blob_id': blob_id,
                    'name': os.path.basename(path),
                    'size': os.path.getsize(path),
                    'mimetype': mime_type
                })

                futures.append(ex.submit(self._upload_image, c, path, blob_url, mime_type))

            # Check for exceptions and update progress bar
            failed = 0
            with tqdm(total=len(futures), unit='image') as pbar:
                for f in as_completed(futures):
                    try:
                        f.result()
                    except Exception as e:
                        print(e)
                        failed += 1
                    pbar.update(1)

            ex.shutdown(wait=True)

        # Upload bulk image info
        url = '{}/{}/project/{}/imagesets/{}/images_bulk?start=0'\
            .format(c.HOME, c.API_0, collection.workspace_id, self.imageset_id)
        c._auth_post(url, body=None, return_response=True, json={'images': bulk_info})

        print('- Finished uploading with {} failures'.format(failed))

    def _upload_image(self, client, path, blob_url, mime_type):
        """Uploads a single image to the collection."""
        try:
            with open(path, 'rb') as f:
                client._upload_to_signed_blob_storage_url(f, blob_url, mime_type)
        except Exception as e:
            print('Error uploading "{}" to blob storage:\n{}'.format(path, e))

    def _check_in_data(self, data):
        cols = list(data.columns)
        if self.column_filename not in cols:
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

    @classmethod
    def _get_mime_type(cls, path) -> str:
        """Gets the mime_type of the path. Raises an error if not a valid image mime_type."""
        ext = os.path.splitext(path)[-1]
        if ext in cls.IMAGE_MIMES.keys():
            return cls.IMAGE_MIMES[ext]
        raise TypeError('"{}" is not a supported image mime_type ({})'.format(path, cls.IMAGE_MIMES))
