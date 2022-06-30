from enum import Enum

from zegami_sdk.experiments.model_loaders.mrcnn import MrcnnModelLoader


class ModelLoaderTypes(Enum):
    """
    Enumeration of model loader types, with values consisting of model class.
    """
    MRCNN = MrcnnModelLoader
