# -*- coding: utf-8 -*-
# Copyright 2021 Zegami Ltd

"""Annotation functionality."""

import base64
import io
import os

import numpy as np
from PIL import Image
import cv2


class _Annotation():
    """Base class for annotations."""

    # Define the string annotation TYPE in child classes
    TYPE = None
    UPLOADABLE_DESCRIPTION = None

    def __init__(self, collection, annotation_data, source=None):
        """
        Base class for annotations.

        Subclasses should call super().__init__ AFTER assignment of members
        so that checks can be performed.

        If making a new annotation to upload, use collection.upload_annotation
        instead.
        """

        self._collection = collection  # Collection instance
        self._source = source  # Source instance

        # { imageset_id, image_index, type, annotation }
        self._data = annotation_data

        # Enforce abstract requirement
        if self.TYPE is None:
            raise TypeError(
                'Do not instantiate the base _Annotation class. It is an '
                'abstract class, try one of the non-hidden Annotation classes '
                'instead.')

    @property
    def collection():
        pass

    @collection.getter
    def collection(self):
        """The collection this annotation belongs to."""
        return self._collection

    @property
    def source():
        pass

    @source.getter
    def source(self):
        """The source this annotation belongs to in its collection."""
        return self._source

    @property
    def _image_index():
        pass

    @_image_index.getter
    def _image_index(self):
        """The image-space index of this annotation's owner's image."""

        if 'image_index' not in self._data.keys():
            raise ValueError('Annotation\'s _data did not contain '
                             '\'image_index\': {}'.format(self._data))
        return self._data['image_index']

    @property
    def row_index():
        pass

    @row_index.getter
    def row_index(self):
        return self._row_index

    @property
    def imageset_index():
        pass

    @imageset_index.getter
    def imageset_index(self):
        return self.collection.row_index_to_imageset_index(self.row_index)

    @property
    def _imageset_id():
        pass

    @_imageset_id.getter
    def _imageset_id(self):
        """Shortcut for the owning collection's (source's) imageset ID."""
        return self.collection._get_imageset_id(self.source)

    # -- Abstract/virtual, must be implemented in children --

    @classmethod
    def create_uploadable(cls) -> None:
        """Extend in children to include actual annotation data."""

        return {
            'type': cls.TYPE,
            'format': None,
            'annotation': None
        }

    def view(self):
        """Abstract method to view a representation of the annotation."""
        raise NotImplementedError(
            '\'view\' method not implemented for annotation type: {}'
            .format(self.TYPE))


class AnnotationMask(_Annotation):
    """
    An annotation comprising a bitmask and some metadata.

    To view the masks an image, use mask.view().

    Note: Providing imageset_id and image_index is not mandatory and can be
    obtained automatically, but this is slow and can cause unnecessary
    re-downloading of data.
    """

    TYPE = 'mask'
    UPLOADABLE_DESCRIPTION = """
        Mask annotation data includes the actual mask (as a base64 encoded
        png string), a width and height, bounding box, and score if generated
        by a model (else None).
    """

    @classmethod
    def create_uploadable(cls, bool_mask, class_id):
        """
        Creates a data package ready to be uploaded with a collection's
        .upload_annotation().

        Note: The output of this is NOT an annotation, it is used to upload
        annotation data to Zegami, which when retrieved will form an
        annotation.
        """

        if type(bool_mask) != np.ndarray:
            raise TypeError('Expected bool_mask to be a numpy array, not a {}'
                            .format(type(bool_mask)))
        if bool_mask.dtype != bool:
            raise TypeError('Expected bool_mask.dtype to be bool, not {}'
                            .format(bool_mask.dtype))
        if len(bool_mask.shape) != 2:
            raise ValueError('Expected bool_mask to have a shape of 2 '
                             '(height, width), not {}'.format(bool_mask.shape))

        # Ensure we are working with [h, w]
        bool_mask = cls.parse_bool_masks(bool_mask, shape=2)
        h, w = bool_mask.shape

        # Encode the mask array as a 1 bit PNG encoded as base64
        mask_image = Image.fromarray(bool_mask.astype('uint8') * 255).convert('1')
        mask_buffer = io.BytesIO()
        mask_image.save(mask_buffer, format='PNG')
        byte_data = mask_buffer.getvalue()
        mask_b64 = base64.b64encode(byte_data)
        mask_string = "data:image/png;base64,{}".format(mask_b64.decode("utf-8"))
        bounds = cls.find_bool_mask_bounds(bool_mask)
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
        """Returns mask data as a 0 -> 255 uint8 numpy array, [h, w]."""
        return self.mask_bool.astype(np.uint8) * 255

    @property
    def mask_bool():
        pass

    @mask_bool.getter
    def mask_bool(self):
        """Returns mask data as a False | True bool numpy array, [h, w]."""

        raise NotImplementedError('Not implemented, see annotation._data to obtain.')
        # return self.parse_bool_masks(self._get_bool_arr(), shape=2)

    @staticmethod
    def _read_bool_arr(local_fp):
        """
        Reads the boolean array from a locally stored file. Useful for
        creation of upload package.
        """

        # Check for a sensible local file
        if not os.path.exists(local_fp):
            raise FileNotFoundError('Mask not found: {}'.format(local_fp))
        if not os.path.isfile(local_fp):
            raise ValueError('Path is not a file: {}'.format(local_fp))

        # Convert whatever is found into a [h, w] boolean mask
        arr = np.array(Image.open(local_fp), dtype='uint8')
        if len(arr.shape) == 3:
            N = arr.shape[2]
            if N not in [1, 3, 4]:
                raise ValueError('Unusable channel count: {}'.format(N))
            if N == 1:
                arr = arr[:, :, 0]
            elif N == 3:
                arr = cv2.cvtColor(arr, cv2.COLOR_RGB2GRAY)
            elif N == 4:
                arr = cv2.cvtColor(arr, cv2.COLOR_RGBA2GRAY)
            if arr.any() and arr.max() == 1:
                arr *= 255

        return arr > 127

    @staticmethod
    def parse_bool_masks(bool_masks, shape=3):
        """
        Checks the masks for correct data types, and ensures a shape of
        [h, w, N].
        """

        if shape not in [2, 3]:
            raise ValueError("Invalid 'shape' - use shape = 2 or 3 for [h, w]"
                             " or [h, w, N].")

        if type(bool_masks) != np.ndarray:
            raise TypeError(
                'Expected bool_masks to be a numpy array, not {}'
                .format(type(bool_masks)))

        if bool_masks.dtype != bool:
            raise TypeError(
                'Expected bool_masks to have dtype == bool, not {}'
                .format(bool_masks.dtype))

        # If mismatching shape and mode, see if we can unambigously coerce
        # into desired shape
        if shape == 3 and len(bool_masks.shape) == 2:
            bool_masks = np.expand_dims(bool_masks, -1)
        elif shape == 2 and len(bool_masks.shape) == 3:
            if bool_masks.shape[2] > 1:
                raise ValueError(
                    'Got a multi-layer bool-mask with N > 1 while using shape'
                    ' = 2. In this mode, only [h, w] or [h, w, 1] are '
                    'permitted, not {}'.format(bool_masks.shape))
            bool_masks = bool_masks[:, :, 0]

        # Final check
        if len(bool_masks.shape) != shape:
            raise ValueError(
                'Invalid final bool_masks shape. Should be {} but was {}'
                .format(shape, bool_masks.shape))

        return bool_masks

    @classmethod
    def find_bool_mask_bounds(cls, bool_mask, fail_on_error=False) -> dict:
        """
        Returns a dictionary of { top, bottom, left, right } for the edges
        of the given boolmask. If fail_on_error is False, a failed result
        returns { 0, 0, 0, 0 }. Set to True for a proper exception.
        """

        bool_mask = cls.parse_bool_masks(bool_mask, shape=2)

        rows = np.any(bool_mask, axis=1)
        cols = np.any(bool_mask, axis=0)

        try:
            top, bottom = np.where(rows)[0][[0, -1]]
            left, right = np.where(cols)[0][[0, -1]]
        except Exception:
            top, bottom, left, right = 0, 0, 0, 0
            if fail_on_error:
                raise ValueError(
                    'Failed to find proper bounds for mask with shape {}'
                    .format(bool_mask.shape))

        return {'top': top, 'bottom': bottom, 'left': left, 'right': right}

    @staticmethod
    def base64_to_boolmask(b64_data):
        """
        Converts str base64 annotation data from Zegami into a boolean
        mask.
        """

        if type(b64_data) is not str:
            raise TypeError(
                'b64_data should be a str, not {}'.format(type(b64_data)))

        # Remove b64 typical prefix if necessary
        if b64_data.startswith('data:'):
            b64_data = b64_data.split(',', 1)[-1]

        img = Image.open(io.BytesIO(base64.b64decode(b64_data)))
        img_arr = np.array(img)

        # Correct for potential float->int scale error
        premax = img_arr.max()
        arr_int = np.array(np.array(img) * 255 if premax < 2 else
                           np.array(img), dtype='uint8')

        return arr_int > 127


class AnnotationBB(_Annotation):
    """
    An annotation comprising a bounding box and some metadata.

    Note: Providing imageset_id and image_index is not mandatory and can be
    obtained automatically, but this is slow and can cause unnecessary
    re-downloading of data.
    """

    TYPE = 'zc-boundingbox'
    UPLOADABLE_DESCRIPTION = """
        Bounding box annotation data includes the bounding box bounds,
        a width and height, and score if generated
        by a model (else None).
    """

    @classmethod
    def create_uploadable(cls, bounds: dict, class_id) -> dict:
        """
        Creates a data package ready to be uploaded with a collection's
        .upload_annotation().

        Input 'bounds' is a dictionary of { x, y, width, height }, where x and
        y are the coordinates of the top left point of the given bounding box.

        Note: The output of this is NOT an annotation, it is used to upload
        annotation data to Zegami, which when retrieved will form an
        annotation.
        """

        data = {
            'x': bounds['x'],
            'y': bounds['y'],
            'w': bounds['width'],
            'h': bounds['height'],
            'type': cls.TYPE,
            'score': None
        }

        uploadable = super().create_uploadable()
        uploadable['format'] = 'BB1'
        uploadable['annotation'] = data
        uploadable['class_id'] = int(class_id)

        return uploadable


class AnnotationPolygon(_Annotation):
    """
    An annotation comprising a polygon and some metadata.

    Note: Providing imageset_id and image_index is not mandatory and can be
    obtained automatically, but this is slow and can cause unnecessary
    re-downloading of data.
    """

    TYPE = 'zc-polygon'
    UPLOADABLE_DESCRIPTION = """
        Polygon annotation data includes the coordinates of the
        polygon points (vertices), and score if generated
        by a model (else None).
    """

    @classmethod
    def create_uploadable(cls, points: list, class_id) -> dict:
        """
        Creates a data package ready to be uploaded with a collection's
        .upload_annotation().

        Input 'points' is a list of (x, y) coordinates for each vertex of the polygon.

        Note: The output of this is NOT an annotation, it is used to upload
        annotation data to Zegami, which when retrieved will form an
        annotation.
        """

        data = {
            'points': points,
            'type': cls.TYPE,
            'score': None
        }

        uploadable = super().create_uploadable()
        uploadable['format'] = 'BB1'
        uploadable['annotation'] = data
        uploadable['class_id'] = int(class_id)

        return uploadable
