# -*- coding: utf-8 -*-
"""
Zegami Ltd.
Apache 2.0
"""

import numpy as np
from PIL import Image
import os


class _Annotation():
    ''' Base (abstract) class for annotations. '''
    
    # Define the string annotation TYPE in child classes
    TYPE = None
    
    def __init__(self, collection, annotation_data, source=None):
        ''' !! STOP !!\n\nInstantiate a non-hidden subclass instead.
        Each subclass should call this __init__ AFTER assignment of members
        so that checks can be performed.
        
        If making a new annotation to upload, use collection.upload_annotation
        instead. '''
        
        self._collection = collection # Collection instance
        self._source = source # Source instance
        self._data = annotation_data # { imageset_id, image_index, type, annotation }
                
                
    @property
    def collection(): pass
    @collection.getter
    def collection(self):
        return self._collection
    
    
    @property
    def source(): pass
    @source.getter
    def source(self):
        return self._source
    
    
    @property
    def _image_index(): pass
    @_image_index.getter
    def _image_index(self):
        assert 'image_index' in self._data.keys(), 'Annotation\'s _data did '\
            'not contain \'image_index\': {}'.format(self._data)
        return self._data['image_index']
    
    
    @property
    def row_index(): pass
    @row_index.getter
    def row_index(self):
        lookup = self.collection._get_image_meta_lookup(self.source)
        return lookup.index(self._image_index)
        
        
    @property
    def _imageset_id(): pass
    @_imageset_id.getter
    def _imageset_id(self):
        return self.collection._get_imageset_id(self.source)
    
    
    # -- Abstract/virtual, must be implemented in children --
    
    @classmethod
    def create_uploadable(cls) -> None:
        ''' Extend in children to include actual annotation data. '''
        return {
            'type' : cls.TYPE,
            'data' : None,
        }
    
    
    def view(self):
        ''' Abstract method to view a representation of the annotation. '''
        return NotImplementedError('\'view\' method not implemented for annotation type: {}'.format(self.TYPE))

        
class AnnotationMask(_Annotation):
    ''' An annotation comprising a bitmask and some metadata. To view the mask
    as an image, use mask.view().
    
    Note: Providing imageset_id and image_index is not mandatory and can be
    obtained automatically, but this is slow and can cause unnecessary
    re-downloading of data. '''
    
    TYPE = 'mask_1UC1'
    
    def __init__(self, collection, row_index, source=None, from_filepath=None, from_url=None, imageset_id=None, image_index=None):
        
        super(AnnotationMask, self).__init__(self, collection, row_index, source, from_filepath, from_url, imageset_id, image_index)
            
            
    @classmethod
    def create_uploadable(cls, bool_mask):
        ''' Creates a data package ready to be uploaded with
        a collection's .upload_annotation().
        
        Note: The output of this is NOT an annotation, it is used to upload
        annotation data to Zegami, which when retrieved will form an
        annotation. '''
        
        # NOT TESTED
        
        assert type(bool_mask) == np.ndarray,\
            'Expected bool_mask to be a numpy array, not a {}'.format(type(bool_mask))
        assert bool_mask.dtype == bool,\
            'Expected bool_mask.dtype to be bool, not {}'.format(bool_mask.dtype)
        assert len(bool_mask.shape) == 2,\
            'Expected bool_mask to have a shape of 2 (height, width), not {}'.format(bool_mask.shape)
            
        h, w = bool_mask.shape
        mask_bytes = Image.fromarray(bool_mask.astype('uint8') * 255).convert('1').tobytes()
        
        uploadable = super().create_uploadable()
        uploadable['annotation'] = {
            'data' : mask_bytes,
            'width' : w,
            'height' : h,
        }
        
        return uploadable
            
            
    def view(self):
        ''' View the mask as an image. '''
        
        # NOT TESTED
        
        im = Image.fromarray(self.mask_uint8)
        im.show()
            
            
    @property
    def mask_uint8(): pass
    @mask_uint8.getter
    def mask_uint8(self):
        return self.mask_bool.astype(np.uint8) * 255
    
        
    @property
    def mask_bool(): pass
    @mask_bool.getter
    def mask_bool(self):
        a = self._get_bool_arr()
        assert len(a.shape) == 2, 'Invalid mask_bool shape: {}'.format(a.shape)
        assert a.dtype == bool, 'Invalid mask_bool dtype: {}'.format(a.dtype) 
        return a
        
        
    @staticmethod
    def _read_bool_arr(local_fp):
        ''' Reads the boolean array from a locally stored file. Useful for
        creation of upload package. '''
        
        # NOT FINISHED
        
        assert os.path.exists(local_fp), 'File not found: {}'.format(local_fp)
        assert os.path.isfile(local_fp), 'Path is not a file: {}'.format(local_fp)
        
        arr = np.array(Image.open(local_fp), dtype='uint8')
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        