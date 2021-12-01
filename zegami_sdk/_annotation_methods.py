# -*- coding: utf-8 -*-
# Copyright 2021 Zegami Ltd

"""private annotation methods."""

import base64
import os


import numpy as np
from PIL import Image


def get_annotations_for_collection(self, collection, source=None, type='mask'):
    """
    Gets all one type of annotations available for a given collection.

    Default as mask annotations.

    Optionally, provide a source index (integer) to retrieve only annotations
    related to that source.
    """
    wid = self._extract_workspace_id(collection)
    cid = self._extract_id(collection)

    url = '{}/{}/project/{}/annotations/collection/{}'.format(
        self.HOME, self.API_1, wid, cid)

    # If a source was provided, modify the URL
    if source is not None:

        assert type(source) == int and source >= 0,\
            'Expected provided source to be a positive integer, got {}'\
            .format(source)

        srcs = self.list_image_sources(collection, return_dicts=True, hide_warning=True)

        assert source < len(srcs),\
            'Provided source is too high for number of sources available '\
            '(index {} in list length {})'.format(source, len(srcs))
        url += '/source/{}'.format(source)

    url += '?type={}'.format(type)

    # Perform the GET
    annos = self._auth_get(url)

    return annos


def get_annotations_for_image(self, collection, row_index, source=None, type='mask'):
    """
    Gets one type of annotations for a single image in a collection.

    Default as mask annotations.
    Specify the image by giving its data row.
    """
    assert source is None or type(source) == int and source >= 0,\
        'Expected source to be None or a positive int, not {}'.format(source)

    srcs = self.list_image_sources(collection, return_dicts=True, hide_warning=True)
    uses_sources = len(srcs) > 0

    if uses_sources and source is None:
        source = 0

    wid = self._extract_workspace_id(collection)
    cid = self._extract_id(collection)

    # Convert the row index into the
    lookup = self._get_image_meta_lookup(collection, source=source)
    imageset_index = lookup[row_index]

    if uses_sources:
        url = '{}/{}/project/{}/annotations/collection/{}/source/{}/images/{}?type={}'\
            .format(self.HOME, self.API_1, wid, cid, srcs[source]['source_id'], imageset_index, type)

    else:
        iid = self._extract_imageset_id(collection)
        url = '{}/{}/project/{}/annotations/imageset/{}/images/{}?type={}'\
            .format(self.HOME, self.API_1, wid, iid, imageset_index, type)

    # Perform the GET
    annos = self._auth_get(url)

    return annos


def post_annotation(self, collection, row_index, annotation, source=None, return_req=False):
    """Posts an annotation to Zegami, storing it online.

    Requires the target collection and the row_index of the item being annotated. If the image
    is from a particular source, provide that too.

    For the 'annotation', provide the result of zc.create_<type>_annotation().
    """
    srcs = self.list_image_sources(collection, return_dicts=True, hide_warning=True)
    uses_sources = len(srcs) > 0

    if uses_sources:
        if source is None:
            source = 0

    wid = self._extract_workspace_id(collection)
    iid = self._extract_imageset_id(collection)

    lookup = self._get_image_meta_lookup(collection, source=source)
    imageset_index = lookup[row_index]

    annotation['imageset_id'] = iid
    annotation['image_index'] = imageset_index

    url = '{}/{}/project/{}/annotations/'.format(self.HOME, self.API_1, wid)

    r = self._auth_post(url, annotation, return_req)

    return r


def create_mask_annotation(mask):
    """Creates a mask annotation using a mask.

    Accepts either a boolean numpy array, or the path to a mask png image.

    Note: 'imageset_id' and 'image_index' keys MUST be added to this before
    sending.
    """
    if type(mask) == str:

        assert os.path.exists(mask),\
            'Got type(mask): str but the path \'{}\' did not exist'.format(mask)

        mask = np.array(Image.open(mask))

    elif type(mask) != np.array:
        raise TypeError('Expected mask to be a str (filepath) or a np array, not {}'
                        .format(type(mask)))

    if len(mask.shape) > 2:
        mask = mask[:, :, 0]

    if mask.dtype is not bool:
        mask = mask > 127

    h, w = mask.shape

    # Encode the single channel boolean mask into a '1' type image, as bytes
    mask_bytes = Image.fromarray(mask.astype('uint8') * 255).convert('1').tobytes()

    # Encode the mask bytes prior to serialisation
    mask_serialised = base64.b64encode(mask_bytes)

    return {
        'imageset_id': None,
        'image_index': None,
        'type': 'mask_1UC1',
        'annotation': {
            'data': mask_serialised,
            'width': w,
            'height': h,
        }
    }


def _reconstitute_mask(annotation):
    if 'annotation' in annotation.keys():
        annotation = annotation['annotation']

    data = annotation['data']
    w = annotation['width']
    h = annotation['height']

    decoded_data = base64.b64decode(data)
    bool_arr = np.array(Image.frombytes('1', (w, h), decoded_data), dtype=int) > 0

    return bool_arr
