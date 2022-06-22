import pandas as pd

from zegami_sdk.collection import Collection
from zegami_sdk.annotation import AnnotationTypes


class CollectionConfig():
    """
    This configuration is used to tell the experiment how to perform training
    on data stored in a collection.

    Override attributes as necessary when instancing.

    Annotation information can be passed via two main categories:

        AnnotationTypes.DATA:
            The annotation is contained within the data

        AnnotationTypes.ANYTHING_ELSE:

    """

    def __init__(
            self,
            collection,
            train_tag='train',
            val_tag='val',
            anno_type=AnnotationTypes.MASK,
            **kwargs
            ):

        if not isinstance(collection, Collection):
            raise TypeError(
                'collection: Must be a Zegami Collection instance.')

        self._experiment = None
        self._anno_type_and_class = anno_type.value  # Extract the enum value
        self.train_tag = train_tag
        self.val_tag = val_tag

    def record(self):
        """
        Records a snapshot of configuration information for the sake of
        experiment record keeping. The record details:
            - How many annotations exist (train and val)
            - The collection used (ID, name)
            - Date/time of creation

        This will be stored in the configuration directory.
        """

        # TODO - Create a snapshot of collection/data information

    # == Operators ==

    def __repr__(self):
        return '<CollectionConfig for Collection "{}" containing data>'\
            .format(self.collection.name)

    # == Properties ==

    @property
    def experiment():
        pass

    @experiment.getter
    def experiment(self):
        if self._experiment is None:
            raise ValueError(
                'CollectionConfig: self.experiment not set by owning '
                'experiment. Should be done after construction.')
        return self._experiment

    @property
    def collection():
        pass

    @collection.getter
    def collection(self):
        return self.experiment.collection

    @property
    def anno_type():
        pass

    @anno_type.getter
    def anno_type(self) -> str:
        return self._anno_type_and_class[0]

    @property
    def train_rows():
        pass

    @train_rows.getter
    def train_rows(self) -> pd.DataFrame:
        """(DataFrame) Subset of collection rows of the train-tagged items."""

        return self.collection.get_rows_by_tags([self.train_tag])

    @property
    def val_rows():
        pass

    @val_rows.getter
    def val_rows(self):
        """(DataFrame) Subset of collection rows  of the val-tagged items."""

        return self.collection.get_rows_by_tags([self.val_tag])
