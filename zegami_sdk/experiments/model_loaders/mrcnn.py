# -*- coding: utf-8 -*-

import os
import numpy as np

from zegami_sdk.experiments.model_loaders.base import _BaseModelLoader

# TODO - Potentially remove these dependencies
from zegami_ai.mrcnn.architecture.model import MaskRCNN
from zegami_ai.mrcnn.architecture.utils import download_trained_weights
from zegami_ai.mrcnn.dataset import MrcnnDataset
from zegami_ai.mrcnn.config import MrcnnConfig


class MrcnnModelLoader(_BaseModelLoader):

    """
    Used experiment.misc_options:
        BASE_WEIGHTS ~ 'coco'

        TRAIN_LAYERS ~ 'all'
        EPOCHS ~ 20
        LEARNING_RATE ~ 0.001
        CALLBACKS ~ []

        CACHE_SIZE ~ 10e+9
        MIN_MASK_AREA ~ 0
        IMAGES_PER_GPU ~ 1
        GPU_COUNT ~ 1
    """

    def __init__(self, **kwargs):

        super().__init__()

    def get_model_filename(self) -> str:
        return 'mrcnn_model.h5'

    def save_model(self, **kwargs) -> None:
        print('Saving happens automatically during training')

    def load_model_train(self, **kwargs):

        # Load the model (training mode)
        self._loaded_model = MaskRCNN(
            'training',
            self._train_config,
            model_dir=self.experiment.model_dir
        )

        # Determine weights to use - try continue -> base -> None
        continue_weights = os.path.join(
            self.experiment.model_dir, self.get_model_filename()
        )

        base_weights = self.experiment.misc_options.get('BASE_WEIGHTS', 'coco')

        # Option A: [CONTINUE TRAINING WEIGHTS]
        if os.path.exists(continue_weights):

            print('[TRAIN CONTINUE WEIGHTS]: Weights found, loading up old weights')
            weights_fp = continue_weights
            mrcnn_exclusion = []

        # Option B: [OBTAIN BASE WEIGHTS]
        elif base_weights:

            # Obtain coco base weights, saved or download
            if base_weights == 'coco':
                weights_fp = os.path.join(
                    self.experiment.model_dir, '[base_coco].h5'
                )
                if not os.path.exists(weights_fp):
                    download_trained_weights(weights_fp)

            else:
                raise ValueError(
                    "Don't know how to handle base_weights: {}"
                    .format(base_weights))

            mrcnn_exclusion = [
                'mrcnn_class_logits', 'mrcnn_bbox_fc', 'mrcnn_bbox',
                'mrcnn_mask', 'rpn_model'
            ]

            print('[TRAIN BASE WEIGHTS]: Loaded weights: "{}"'.format(weights_fp))

        # Option C: [NO WEIGHTS]
        else:
            print('\n[TRAIN NO WEIGHTS]: No weights loaded')

        self._loaded_model.load_weights(
            weights_fp, by_name=True, exclude=mrcnn_exclusion
        )

        print('Train model ready')

    def load_model_inference(self, **kwargs):

        if self.experiment.inference_model_path is not None:
            weights_fp = self.experiment.inference_model_path
        else:
            weights_fp = os.path.join(
                self.experiment.model_dir, self.get_model_filename()
            )

        if not os.path.exists(weights_fp):
            raise FileNotFoundError('"{}" weights file does not exist'.format(weights_fp))

        model = MaskRCNN('inference', self.generate_val_mrcnn_config(), self.experiment.model_dir)
        model.load_weights(weights_fp, by_name=True)

        self._loaded_model = model

        print('Inference model ready')

    def load_train_data(self, **kwargs):
        """
        Mask-RCNN data uses AnnotationTypes.MASK for 'mask' annotations.
        Datasets should be turned into datagenerators using the annotations
        available from experiment.collection_config.train_rows/annotations.
        """

        msc = self.experiment.misc_options

        self._dataset_train = MrcnnDataset(
            self.experiment.collection,
            self.experiment.collection_config.val_tag,
            allow_caching=True,
            image_cache_limit=msc.get('CACHE_SIZE', 10e+9),
            min_mask_area=msc.get('MIN_MASK_AREA', 0)
        )

        self._dataset_train.prepare()

        print('Training data prepared')

    def load_val_data(self, **kwargs):

        msc = self.experiment.misc_options

        self._dataset_val = MrcnnDataset(
            self.experiment.collection,
            self.experiment.collection_config.val_tag,
            allow_caching=True,
            image_cache_limit=msc.get('CACHE_SIZE', 10e+9),
            min_mask_area=msc.get('MIN_MASK_AREA', 0)
        )

        print('Validation data prepared')

    def train(self, **kwargs):

        print('Beginning training')

        msc = self.experiment.misc_options

        return self._loaded_model.train(
            self._dataset_train,
            self._dataset_val,
            layers=msc.get('TRAIN_LAYERS', 'all'),
            epochs=msc.get('EPOCHS', 20),
            learning_rate=msc.get('LEARNING_RATE', 0.001),
            custom_callbacks=msc.get('CALLBACKS', [])
        )

    def infer(self, **kwargs):
        raise NotImplementedError()

    def validity_test(self, **kwargs) -> bool:
        raise NotImplementedError()

    # == Utility ==

    def generate_train_mrcnn_config(self):

        num_classes = len(self.experiment.collection.classes)
        if num_classes == 0:
            raise ValueError(
                'Collection has no classes set')
        class_names = [c['name'] for c in self.experiment.collection.classes]

        # Obtain misc options
        msc = self.experiment.misc_options

        images_per_gpu = msc.get('IMAGES_PER_GPU', 1)
        gpu_count = msc.get('GPU_COUNT', 1)
        batch_size = images_per_gpu * gpu_count
        learning_rate = msc.get('LEARNING_RATE', 0.001)

        steps_train = int(np.floor(len(self._dataset_train) / batch_size))
        steps_val = int(np.floor(len(self._dataset_val) / batch_size))

        class TrainConfig(MrcnnConfig):

            NUM_CLASSES = 1 + num_classes
            CLASS_NAMES = ['BG'] + class_names
            NAME = 'MrcnnExperimentTrainConfig_{}'.format(self.experiment.name)
            IMAGES_PER_GPU = images_per_gpu
            GPU_COUNT = gpu_count
            LEARNING_RATE = learning_rate
            STEPS_PER_EPOCH = steps_train
            VALIDATION_STEPS = steps_val

        train_cfg = TrainConfig()

        print(
            '\nConfig initialized:\n'
            'Classes (incl. BG): {}\nBatch size: {}\nSteps/epoch T: {}, V: {}'
            .format(train_cfg.NUM_CLASSES, batch_size,
                    train_cfg.STEPS_PER_EPOCH, train_cfg.VALIDATION_STEPS)
        )

        return train_cfg

    def generate_val_mrcnn_config(self):

        inf_cfg = self.generate_train_mrcnn_config()

        # Force one at a time
        inf_cfg.IMAGES_PER_GPU = 1
        inf_cfg.GPU_COUNT = 1

        return inf_cfg
