
import os


SUPPORTED_MODEL_TYPES = [
    'mrcnn'
]


class TrainedModel():
    """
    Wrapper for a model that has been trained, with saved weights and a
    mechanism for loading the model up for inference. Distinctly different to
    a model training environment/experiment where parameters for training
    need to be stored.
    """

    TYPE = None

    def __init__(self, save_path, **kwargs):
        """
        - save_path:
            Path to the trained weights or fully saved model.

        - configuration:
            A type-specific configuration required by most model
            implementations.
        """

        if self.TYPE not in SUPPORTED_MODEL_TYPES:
            raise ValueError(
                'TrainedModel type "{}" not supported'.format(self.TYPE))

        save_path = save_path.replace('\\', '/').strip('/')
        if not os.path.exists(save_path):
            raise FileNotFoundError(
                'Could not find model save_path: "{}"'.format(save_path))

        self.save_path = save_path
        self.kwargs = kwargs
