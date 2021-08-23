# -*- coding: utf-8 -*-
"""
Zegami Ltd.

Apache 2.0
"""

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import time
from tqdm import tqdm


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


def _scan_directory_tree(path, allowed_ext, blacklist_ext, ignore_mime):
    files = []
    for entry in os.scandir(path):
        whitelisted = entry.name.lower().endswith(allowed_ext)
        if ignore_mime and not whitelisted:
            whitelisted = True
        # Some files should not be uploaded even if we are forcing mime type.
        if entry.name.lower().endswith(blacklist_ext):
            whitelisted = False
            print("Ignoring file due to disallowed extension: {}".format(entry.name))
        if entry.is_file() and whitelisted:
            files.append(entry.path)
        if entry.is_dir():
            files.extend(_scan_directory_tree(entry.path, allowed_ext, blacklist_ext, ignore_mime))
    return files


def format_bytes(size):
    power = 1024
    n = 0
    power_labels = {0: '', 1: 'K', 2: 'M', 3: 'G', 4: 'T'}
    while size > power:
        size /= power
        n += 1
    return "{}{}B".format(round(size, 2), power_labels[n])


def _resolve_paths(paths, should_recurse, ignore_mime):
    """Resolve all paths to a list of files."""
    allowed_ext = tuple(IMAGE_MIMES.keys())
    blacklist_ext = BLACKLIST

    resolved = []
    for path in paths:
        whitelisted = (path.lower().endswith(allowed_ext) or ignore_mime)
        if os.path.isdir(path):
            if should_recurse:
                resolved.extend(
                    _scan_directory_tree(path, allowed_ext, blacklist_ext, ignore_mime)
                )
            else:
                resolved.extend(
                    entry.path for entry in os.scandir(path)
                    if entry.is_file() and (
                        entry.name.lower().endswith(allowed_ext) or
                        (ignore_mime and not entry.name.lower().endswith(blacklist_ext))
                    )
                )
        elif os.path.isfile(path) and whitelisted:
            resolved.append(path)

    total_size = 0
    for path in resolved:
        size = os.path.getsize(path)
        total_size += size
    print("Total upload size: {}".format(format_bytes(total_size)))
    return resolved


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

    def _upload_images_list(self, workload, mime_type=None, start=0):
        """
        Upload a list of images to the collection, optionally a mime type can be provided.
        start indicates the starting imageset index
        """
        paths = workload['paths']
        images_info = []
        index = 0

        client = self._collection.client

        # extend imageset to let the server know how many uploads are expected
        extend_url = '{}/{}/project/{}/imagesets/{}/extend'.format(
            client.HOME, client.API_0, self._collection.workspace_id, self._imageset_id
        )
        client._auth_post(extend_url, body=None, json={'delta': len(paths)})

        urls, id_set = client._obtain_signed_blob_storage_urls(self._collection.workspace_id, id_count=len(paths))

        for path in paths:
            file_name = os.path.basename(path)
            file_ext = os.path.splitext(path)[-1]

            # Check if the file has a correct extension
            if mime_type:
                mime_type = mime_type
            elif file_ext in IMAGE_MIMES.keys():
                mime_type = IMAGE_MIMES[file_ext]
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
                    print('')
                except Exception as ex:
                    # The upload will retry in the face of errors. This blob can't be uploaded ata ll
                    # Continue on to the next image without uploading any image info
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

        add_images_bulk_url = add_images_bulk_url + '?start={}'.format(workload['start'])

        print('add_images_url', add_images_bulk_url)
        print('images info', images_info)

        client._auth_post(add_images_bulk_url, body=None, return_response=True, json={'images': images_info})

    def _assign_images_to_smaller_lists(self, image_dir, recurse_dirs, ignore_mime):
        """Create smaller lists based on the number of images in the directory."""
        # Recurse and pick up only valid files (either with image extensions, or not on blacklist)
        files = _resolve_paths([image_dir], recurse_dirs, ignore_mime=ignore_mime)

        total_work = len(files)
        workloads = []
        workload = []
        start = 0

        if total_work > 2500:
            size = 100
        elif total_work < 100:
            size = 1
        else:
            size = 10

        i = 0
        while i < total_work:
            path = os.path.join(image_dir, files[i])
            workload.append(path)
            i += 1
            if len(workload) == size or i == total_work:
                workloads.append({'paths': workload, 'start': start})
                workload = []
                start = i

        return workloads, total_work, size

    def _upload_all_images(self, image_dir, recurse_dirs=False, mime_type=None, max_workers=50, show_time_taken=True):
        """Upload multiple images to a collection."""
        t = time()
        workloads, total_work, size = self._assign_images_to_smaller_lists(
            image_dir,
            recurse_dirs=recurse_dirs,
            ignore_mime=mime_type is not None,
        )
        with tqdm(total=total_work) as pbar:
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                futures = [ex.submit(self._upload_images_list, workload, mime_type=mime_type) for workload in workloads]

                for future in as_completed(futures):
                    result = future.result()  # noqa: F841
                    pbar.update(1 * int(size))

        # Replace empties. Informs the server that all uploads are complete.
        c = self._collection.client
        replace_empties_url = f'{c.HOME}/{c.API_0}/project/{self._collection.workspace_id}/imagesets/{self._imageset_id}/replace_empties'
        c._auth_post(replace_empties_url, body=None, json={})

        if show_time_taken:
            print(f'\nUploaded {total_work} images in {time() - t:.2f} seconds.')
