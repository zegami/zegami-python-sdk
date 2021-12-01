# -*- coding: utf-8 -*-
# Copyright 2021 Zegami Ltd

"""Annotation functionality."""

import base64
import io
import os

import numpy as np
from PIL import Image


class _Annotation():
    """Base (abstract) class for annotations."""

    # Define the string annotation TYPE in child classes
    TYPE = None
    UPLOADABLE_DESCRIPTION = None

    def __init__(self, collection, annotation_data, source=None):
        """
        !! STOP !! Instantiate a non-hidden subclass instead.

        Each subclass should call this __init__ AFTER assignment of members
        so that checks can be performed.

        If making a new annotation to upload, use collection.upload_annotation
        instead.
        """

        self._collection = collection  # Collection instance
        self._source = source  # Source instance
        self._data = annotation_data  # { imageset_id, image_index, type, annotation }

        # Enforce abstract requirement
        if self.TYPE is None:
            raise TypeError(
                'Do not instantiate the base _Annotation class. It is '
                'intended to be an abstract class, try one of the non-hidden '
                'Annotation classes instead.')

    @property
    def collection():
        pass

    @collection.getter
    def collection(self):
        """The collection this annotation belongs to. """
        return self._collection

    @property
    def source():
        pass

    @source.getter
    def source(self):
        """The source this annotation belongs to in its collection. """
        return self._source

    @property
    def _image_index():
        pass

    @_image_index.getter
    def _image_index(self):
        """The image-space index of this annotation's owner's image. """

        if 'image_index' not in self._data.keys():
            raise ValueError('Annotation\'s _data did not contain '
                             '\'image_index\': {}'.format(self._data))
        return self._data['image_index']

    @property
    def row_index():
        pass

    @row_index.getter
    def row_index(self):
        """The data-row-space index of this annotation's owner. """

        lookup = self.collection._get_image_meta_lookup(self.source)
        return lookup.index(self._image_index)

    @property
    def _imageset_id():
        pass

    @_imageset_id.getter
    def _imageset_id(self):
        """Shortcut for the owning collection's (source's) imageset ID. """
        return self.collection._get_imageset_id(self.source)

    # -- Abstract/virtual, must be implemented in children --

    @classmethod
    def create_uploadable(cls) -> None:
        """Extend in children to include actual annotation data. """

        return {
            'type': cls.TYPE,
            'format': None,
            'annotation': None
        }

    def view(self):
        """Abstract method to view a representation of the annotation. """
        return NotImplementedError(
            '\'view\' method not implemented for annotation type: {}'
            .format(self.TYPE))


class AnnotationMask(_Annotation):
    """An annotation comprising a bitmask and some metadata.

    To view the masks an image, use mask.view().

    Note: Providing imageset_id and image_index is not mandatory and can be
    obtained automatically, but this is slow and can cause unnecessary
    re-downloading of data."""

    TYPE = 'mask'
    UPLOADABLE_DESCRIPTION = """
        'Mask annotation data includes the actual mask (as a base64 encoded
        'png string), a width and height, bounding box, and score if generated
        by a model (else None). """

    def __init__(self, collection, row_index, source=None, from_filepath=None,
                 from_url=None, imageset_id=None, image_index=None):
        super().__init__(self, collection, row_index, source, from_filepath, from_url, imageset_id, image_index)

    @classmethod
    def create_uploadable(cls, bool_mask, class_id):
        """Creates a data package ready to be uploaded with a collection's
        .upload_annotation().

        Note: The output of this is NOT an annotation, it is used to upload
        annotation data to Zegami, which when retrieved will form an
        annotation. """

        if type(bool_mask) != np.ndarray:
            raise TypeError('Expected bool_mask to be a numpy array, not a {}'
                            .format(type(bool_mask)))
        if bool_mask.dtype != bool:
            raise TypeError('Expected bool_mask.dtype to be bool, not {}'
                            .format(bool_mask.dtype))
        if len(bool_mask.shape) != 2:
            raise ValueError('Expected bool_mask to have a shape of 2 '
                             '(height, width), not {}'.format(bool_mask.shape))

        h, w = bool_mask.shape

        # Encode the mask array as a 1 bit PNG encoded as base64
        mask_image = Image.fromarray(bool_mask.astype('uint8') * 255).convert('1')
        mask_buffer = io.BytesIO()
        mask_image.save(mask_buffer, format='PNG')
        byte_data = mask_buffer.getvalue()
        mask_b64 = base64.b64encode(byte_data)
        mask_string = "data:image/png;base64,{}".format(mask_b64.decode("utf-8"))
        bounds = cls.get_bool_mask_bounds(bool_mask)
        roi = {
            'xmin': int(bounds['left']),
            'xmax': int(bounds['right']),
            'ymin': int(bounds['top']),
            'ymax': int(bounds['bottom']),
            'width': int(bounds['right'] - bounds['left']),
            'height': int(bounds['bottom'] - bounds['top'])
        }

        data = {
            'mask': mask_string,
            'width': int(w),
            'height': int(h),
            'score': None,
            'roi': roi
        }

        uploadable = super().create_uploadable()
        uploadable['format'] = '1UC1'
        uploadable['annotation'] = data
        uploadable['class_id'] = int(class_id)

        return uploadable

    def view(self):
        """View the mask as an image. """

        # NOT TESTED
        im = Image.fromarray(self.mask_uint8)
        im.show()

    @property
    def mask_uint8():
        pass

    @mask_uint8.getter
    def mask_uint8(self):
        """Mask data as a uint8 numpy array (0 -> 255). """

        return self.mask_bool.astype(np.uint8) * 255

    @property
    def mask_bool():
        pass

    @mask_bool.getter
    def mask_bool(self):
        """Mask data as a bool numpy array. """

        a = self._get_bool_arr()
        if len(a.shape) != 2:
            raise ValueError('Unexpected mask_bool shape: {}'.format(a.shape))
        if a.dtype != bool:
            raise TypeError('Unexpected mask_bool dtype: {}'.format(a.dtype))
        return a

    @staticmethod
    def _read_bool_arr(local_fp):
        """Reads the boolean array from a locally stored file. Useful for
        creation of an upload package. """

        # TODO - Not finished/tested
        assert os.path.exists(local_fp), 'File not found: {}'.format(local_fp)
        assert os.path.isfile(local_fp), 'Path is not a file: {}'.format(local_fp)
        arr = np.array(Image.open(local_fp), dtype='uint8')
        return arr

    @staticmethod
    def parse_bool_masks(bool_masks):
        """Checks the masks for correct data types, and ensures a shape of
        [h, w, N]. """

        if type(bool_masks) != np.ndarray:
            raise TypeError('Expected bool_masks to be a numpy array, not {}'
                            .format(type(bool_masks)))
        if bool_masks.dtype != bool:
            raise TypeError('Expected bool_masks to have dtype == bool, not {}'
                            .format(bool_masks.dtype))

        # If there is only one mask with no third shape value, insert one
        if len(bool_masks.shape) == 2:
            bool_masks = np.expand_dims(bool_masks, -1)

        return bool_masks

    @classmethod
    def get_bool_mask_bounds(cls, bool_mask):
        """Returns the { top, bottom, left, right } of the boolean array
        associated with this annotation, calculated from its array data. """

        bool_mask = cls.parse_bool_masks(bool_mask)[:, :, 0]

        rows = np.any(bool_mask, axis=1)
        cols = np.any(bool_mask, axis=0)

        try:
            top, bottom = np.where(rows)[0][[0, -1]]
            left, right = np.where(cols)[0][[0, -1]]
        except Exception:
            top, bottom, left, right = 0, 0, 0, 0

        return {'top': top, 'bottom': bottom, 'left': left, 'right': right}

    @staticmethod
    def base64_to_boolmask(b64_data):
        """Converts str base64 annotation data from Zegami into a boolean
        mask. """

        if type(b64_data) is not str:
            raise TypeError('b64_data should be a str, not {}'.format(type(b64_data)))
        if b64_data.startswith('data:'):
            b64_data = b64_data.split(',', 1)[-1]
        img = Image.open(io.BytesIO(base64.b64decode(b64_data)))
        img_arr = np.array(img)
        premax = img_arr.max()
        arr_int = np.array(np.array(img) * 255 if premax < 2 else np.array(img), dtype='uint8')
        return arr_int > 125
