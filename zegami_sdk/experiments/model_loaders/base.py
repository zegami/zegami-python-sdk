

class _BaseModelLoader():
    """
    Base class, override relevant methods to build functionality for model:
        - Saving
        - Loading (train)
        - Loading (inference)
        - Inference
        - Validity testing
    """

    def __init__(self, **kwargs):

        # Set post-construction by owning Experiment
        self._experiment = None
        self._loaded_model = None

    def get_model_filename(self) -> str:
        raise NotImplementedError()

    def save_model(self, **kwargs) -> None:
        raise NotImplementedError()

    def load_model_train(self, **kwargs):
        raise NotImplementedError()

    def load_model_inference(self, **kwargs):
        raise NotImplementedError()

    def load_train_data(self, **kwargs):
        raise NotImplementedError()

    def load_val_data(self, **kwargs):
        raise NotImplementedError()

    def train(self, **kwargs):
        raise NotImplementedError()

    def infer(self, **kwargs):
        raise NotImplementedError()

    def validity_test(self, **kwargs) -> bool:
        raise NotImplementedError()

    # == Operators ==

    def __repr__(self):
        return '<ModelLoader ({}) of Experiment "{}">'.format(
            type(self).__name__, self.experiment.name)

    # == Properties ==

    @property
    def experiment():
        pass

    @experiment.getter
    def experiment(self):
        return self._experiment

    @property
    def save_path():
        pass

    @save_path.getter
    def save_path(self):
        """
        Gets the expected filepath of the saved model based on the location of
        the experiment and the expected filename, as defined by subclasses.
        """

        return '{}{}'.format(
            self.experiment.model_dir, self.get_model_filename())

    @property
    def loaded_model():
        pass

    @loaded_model.getter
    def loaded_model(self):
        return self._loaded_model
