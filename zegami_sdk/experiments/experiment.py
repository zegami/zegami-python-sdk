# -*- coding: utf-8 -*-
import os

from zegami_sdk.collection import Collection


class Experiment():
    """
    Description:

    An Experiment acts as an umbrella entity for a training run, encapsulating
    all relevant information and references used to produce the output model.
    It also serves as a useful API for interacting with the SDK and Zegami
    backend.


    Requirements:

    In order to design an experiment, the following requirements need to be
    met:
        - A unique name
        - A directory in which to create a new root directory 'name'
        - A valid Collection() instance used to drive training
        - A model loader, use:
            from zegami_sdk.experiments.model_loaders import ModelLoaders
            ModelLoaders.ENUM_TYPE_YOU_NEED
        - After construction, create a CollectionConfig() instance and
            use experiment.assign_collection_config(your_config).
        - misc_options is used to feed global information relevant to each
            experiment down the chain. See model subclasses for more info.

    To check that all is ready, query experiment.is_ready
    """

    def __init__(self, collection, model_loader_enum,
                 inference_model_path=None, directory='./', name='experiment', misc_options={}):

        # Format to '/' trailing directory
        directory = os.path.abspath(str(directory))\
            .replace('\\', '/').strip('/') + '/' + str(name) + '/'

        if inference_model_path is None:
            if os.path.exists(directory):
                raise FileExistsError(
                    'The directory "{}" already exists.'.format(directory))
            os.makedirs(directory)

        if not isinstance(collection, Collection):
            raise TypeError(
                'collection: Must be a zegami_sdk.collection.Collection')

        # Construct and init model loader
        model_loader = model_loader_enum.value()
        model_loader._experiment = self

        self._name = str(name)
        self._root = directory
        self._collection = collection
        self._model_loader = model_loader
        self.inference_model_path = inference_model_path
        self.misc_options = misc_options

    def assign_collection_config(self, collection_config):
        """
        Required after Experiment() creation. Use:
        from zegami_sdk.experiments.collection_config import CollectionConfig
        to create a config instance, then provide that to this method.
        """

        if collection_config._experiment is not None:
            raise ValueError(
                'This config is already being used by another experiment.')

        self._collection_config = collection_config
        self.collection_config._experiment = self

    # == Operators ==

    def __repr__(self):
        return '<Zegami Experiment>'.format()

    # == Properties ==

    @property
    def is_ready():
        pass

    @is_ready.getter
    def is_ready(self) -> bool:
        """
        Runs some checks to make sure everything is ready and initialized for
        working.
        """

        # Check if the config is assigned
        try:
            self.collection_config
        except ValueError as e:
            print(e)
            return False

        # TODO - Add more testing

        return True

    @property
    def name():
        pass

    @name.getter
    def name(self):
        return self._name

    @property
    def root():
        pass

    @root.getter
    def root(self):
        """Root local directory for this experiment."""
        return self._root

    @property
    def collection():
        pass

    @collection.getter
    def collection(self):
        return self._collection

    @property
    def workspace():
        pass

    @workspace.getter
    def workspace(self):
        return self.collection.workspace

    @property
    def client():
        pass

    @client.getter
    def client(self):
        return self.collection.client

    @property
    def model_loader():
        pass

    @model_loader.getter
    def model_loader(self):
        return self._model_loader

    @property
    def collection_config():
        pass

    @collection_config.getter
    def collection_config(self):

        if self._collection_config is None:
            raise ValueError(
                'No CollectionConfig has been assigned yet.')

        return self._collection_config

    # Directories

    @property
    def model_dir():
        pass

    @model_dir.getter
    def model_dir(self):
        """Directory for model storage - creates if it doesn't exist yet."""

        d = '{}model/'.format(self.root)
        if not os.path.exists(d):
            os.makedirs(d)

        return d

    @property
    def configuration_dir():
        pass

    @configuration_dir.getter
    def configuration_dir(self):
        """
        Directory for configuration storage - creates if it doesn't exist yet.
        """

        d = '{}configuration/'.format(self.root)
        if not os.path.exists(d):
            os.makedirs(d)

        return d
