
import os
import sys
from glob import glob
from importlib.util import spec_from_file_location, module_from_spec
from PIL import Image

from zegami_sdk.trained_models import TrainedModel

from zegami_ai.mrcnn.architecture.model import MaskRCNN
from zegami_ai.mrcnn.utils import parse_image, parse_bool_masks
from zegami_ai.mrcnn.visualize import stamp_instances_on_subject


class MrcnnTrainedModel(TrainedModel):

    TYPE = 'mrcnn'

    def __init__(self, save_path, **kwargs):
        """
        - save_path:
            Path to a directory containing all relevant saved mrcnn inference
            loading data. Expected:
                - A .h5 file (automatically uses last of a **/*.h5 glob query)
                - A python file called 'inference_config.py' with a class
                  in it called 'InferenceConfig()' to instantiate from.

        Optional kwargs:

        - use_mrcnn_exclusion:
            This is required for base weights (coco), not manually trained
            weights. It is like this won't ever be needed if this trained
            model always uses manually training results.
        """

        # Check basic requirements and save_path existence
        super().__init__(save_path, **kwargs)

        # Load data
        self.load_config()
        self.load_model()

    def load_config(self):
        """
        Loads the inference configuration, looking for a file in save_path
        called 'inference_config.py'.
        """

        print('\n[Loading Config]')

        if self.kwargs.get('config_path'):
            fp = os.path.join(self.kwargs.get('config_path'), 'inference_config.py')
        else:
            fp = os.path.join(self.save_path, 'inference_config.py')
        if not os.path.exists(fp):
            raise FileNotFoundError(
                'Expected to find configuration file at "{}"'.format(fp))

        # Load the file as a module
        spec = spec_from_file_location('inference_config', fp)
        inference_config = module_from_spec(spec)
        sys.modules['inference_config'] = inference_config
        spec.loader.exec_module(inference_config)

        # Instantiate the config
        self.config = inference_config.InferenceConfig()

        # Readout
        print('Inference Config:')
        for k in [_ for _ in dir(self.config) if not _.startswith('__') and not callable(getattr(self.config, _))]:
            print('{:<30} {}'.format(k, getattr(self.config, k)))

    def load_model(self):
        """
        Loads the model using the already-instantiated inference config and
        last-found .h5 weights file in the save directory.
        """

        print('\n[Loading Model]')

        if not self.kwargs.get('is_file'):
            # Ensure weights
            regex = '{}**/*.h5'.format(self.save_path)
            candidates = [str(g) for g in glob(regex, recursive=True)]
            if len(candidates) == 0:
                raise FileNotFoundError(
                    'No weights files found in "{}"'.format(self.save_path))
            elif len(candidates) > 1:
                wfp = candidates[-1]
                print('Multiple weights found: "{}"'.format(candidates))
                print('Using weights: "{}"'.format(candidates[-1]))
            else:
                wfp = candidates[0]
                print('Using weights: "{}"'.format(os.path.basename(wfp)))
        else:
            wfp = self.save_path

        # Potentially use mrcnn exclusion (used with fresh coco weights)
        mrcnn_exclusion = []
        use_mrcnn_exclusion = self.kwargs.get('use_mrcnn_exclusion', False)
        if use_mrcnn_exclusion:
            mrcnn_exclusion = [
                'mrcnn_class_logits', 'mrcnn_bbox_fc', 'mrcnn_bbox',
                'mrcnn_mask', 'rpn_model']
            print('Using mrcnn exclusion (untouched external weights)')

        # Load the model
        print('Instantiating model')
        model = MaskRCNN('inference', self.config, '.')

        print('Loading weights')
        model.load_weights(wfp, by_name=True, exclude=mrcnn_exclusion)

        self.model = model

    def infer(self, image, return_visualization=True, return_raw=False) -> dict:
        """
        Performs inference on the provided image (array, PIL image or path).

        - return_visualization:
            Includes a 'visualization' key to the returned dictionary with
            a useful representation of detection.

        - return_raw:
            Includes a 'raw' key to the returned dictionary containing the
            raw output from the MaskRCNN detect() method.
        """

        # Parse the image into an array
        image = parse_image(image)

        # Run inference
        result = self.model.detect([image])[0]

        # Get results and figure out names from IDs
        ids = result['class_ids']
        scores = result['scores']
        names = [self.config.CLASS_NAMES[i] for i in ids]
        N = len(ids)

        # Define masks array, None if there were no hits
        if N == 0:
            masks = None
        else:
            masks = parse_bool_masks(result['masks'])

        # Define the return output
        ret = {
            'bool_masks': masks,
            'scores': scores,
            'class_ids': ids,
            'class_names': names
        }

        # Include a visualization if specified
        if return_visualization:
            if N == 0:
                vis = Image.fromarray(image)
            else:
                vis = Image.fromarray(stamp_instances_on_subject(
                    image, result['masks'],
                    class_names=names,
                    class_confidences=result['scores']
                ))
            ret['visualization'] = vis

        # Include raw detection result if specified
        if return_raw:
            ret['raw'] = result

        return ret
