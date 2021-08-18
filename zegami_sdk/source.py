# -*- coding: utf-8 -*-
"""
Zegami Ltd.

Apache 2.0
"""

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import time
from tqdm import tqdm


class Source():

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
    def id():
        pass

    @id.getter
    def id(self):
        assert self._data, 'Source had no self._data set'
        assert 'source_id' in self._data, 'Source\'s data didn\'t have a \'source_id\' key'
        return self._data['source_id']

    @property
    def _imageset_id():
        pass

    @_imageset_id.getter
    def _imageset_id(self):
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

    def _upload_images_list(self, paths, mime_type=None):
        """Upload a list of images to the collection, optionally a mime type can be provided."""
        images_info = []
        index = 0

        client = self._collection.client
        urls, id_set = client._obtain_signed_blob_storage_urls(self._collection.workspace_id, id_count=len(paths))

        for path in paths:
            file_name = os.path.basename(path)
            file_ext = os.path.splitext(path)[-1]

            # Check if the file has a correct extension
            if mime_type:
                mime_type = mime_type
            elif file_ext in self.IMAGE_MIMES.keys():
                mime_type = self.IMAGE_MIMES[file_ext]
            else:
                print("\n File is not a recognised type", file_name)
                continue

            with open(path, 'rb') as f:

                # find the blob storage url and upload to it
                blob_id = id_set["ids"][index]
                url = urls[blob_id]

                # Make sure the upload isn't interrupted for all images
                try:
                    client._upload_to_signed_blob_storage_url(f, url, mime_type)
                except Exception as ex:
                    # The upload will retry in the face of errors. This blob can't be uploaded ata ll
                    # Continue on to the next image without including any image info
                    print(f'\nAn error occurred while uploading image {path} to blob storage:', ex)
                    continue

                # update the new image details to the relevant endpoint
                info = {
                    "image": {
                        "blob_id": blob_id,
                        "name": file_name,
                        "size": os.path.getsize(path),
                        "mimetype": mime_type
                    }
                }

                # append to the list in order to update later
                images_info.append(info['image'])
                index = index + 1

        add_images_bulk_url = f'{client.HOME}/{client.API_0}/project/{self._collection.workspace_id}' \
            f'/imagesets/{self._imageset_id}/images_bulk'

        client._auth_post(add_images_bulk_url, body=None, return_response=True, json={'images': images_info})

    def _assign_images_to_smaller_lists(self, image_dir):
        """Create smaller lists based on the number of images in the directory."""
        files = os.listdir(image_dir)
        total_work = len(files)
        final_paths = []
        temp = []

        if total_work > 2500:
            size = 100
        elif total_work < 100:
            size = 1
        else:
            size = 10

        i = 0
        while i < total_work:
            path = os.path.join(image_dir, files[i])
            temp.append(path)
            i += 1
            if len(temp) == size or i == total_work:
                final_paths.append(temp)
                temp = []

        return final_paths, total_work, size

    def _upload_all_images(self, image_dir, mime_type=None, max_workers=50, show_time_taken=True):
        """Upload multiple images to a collection."""
        t = time()
        all_paths, total_work, size = self._assign_images_to_smaller_lists(image_dir)
        with tqdm(total=total_work) as pbar:
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                futures = [ex.submit(self._upload_images_list, paths, mime_type=mime_type) for paths in all_paths]

                for future in as_completed(futures):
                    result = future.result()  # noqa: F841
                    pbar.update(1 * int(size))

        if show_time_taken:
            print(f'\nUploaded {total_work} images in {time() - t:.2f} seconds.')
