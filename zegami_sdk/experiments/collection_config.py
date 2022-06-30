import pandas as pd

from zegami_sdk.collection import Collection
from zegami_sdk.annotation import AnnotationTypes


class CollectionConfig():
    """
    This configuration is used to tell the experiment how to perform training
    on data stored in a collection.

    Annotation information can be passed via two main categories:

        AnnotationTypes.DATA:

            The annotation is contained within the data.
            REQUIRES 'class_columns' - which columns inform the model? (list)

        AnnotationTypes.ANYTHING_ELSE:

            The annotation type is checked, if the collection contains no
            relevant annotations, an error is thrown.

    """

    def __init__(
            self,
            collection,
            train_tag='train',
            val_tag='val',
            anno_type=AnnotationTypes.MASK,
            class_columns=None,
            **kwargs):

        if not isinstance(collection, Collection):
            raise TypeError(
                'collection: Must be a Zegami Collection instance.')

        if anno_type not in AnnotationTypes:
            raise ValueError(
                'Provided anno_type is not in AnnotationTypes. Provide a '
                'valid enum from that enumerator.')

        if anno_type == AnnotationTypes.DATA:
            self._check_data_oriented(collection, class_columns)
            self._is_data_oriented = True

        else:
            self._check_anno_oriented(collection, anno_type)
            self._is_data_oriented = False

        self._experiment = None
        self._collection = collection
        self._class_columns = class_columns
        self._anno_type_and_class = anno_type.value  # Extract the enum value
        self._train_tag = train_tag
        self._val_tag = val_tag

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

    @staticmethod
    def _check_data_oriented(collection, class_columns):
        """
        Checks for sensible class_columns input for the given collection.
        Used for row-data-oriented annotations.
        """

        if type(class_columns) != list:
            raise ValueError(
                'If using data-generated annotations, please provide a '
                'list of relevant class_columns.')

        if len(class_columns < 1):
            raise ValueError(
                'Using data-generated annotations, but no class_columns '
                'have been provided.')

        cols = collection.rows.columns
        for col in class_columns:
            if col not in cols:
                raise ValueError(
                    "Provided class_column '{}' does not exist in the "
                    "provided collection's columns.".format(col))

    @staticmethod
    def _check_anno_oriented(collection, anno_enum):
        """
        Checks for sensible annotation input for the given collection.
        Used for SDK-annotation-oriented annotations.
        """

        anno_type, anno_class = anno_enum.value
        df_annos = pd.DataFrame(collection.get_annotations())

        # Ensure at least one annotation of the right type
        err = ValueError(
            'No "{}" type annotations found in the collection.'
            .format(anno_type))

        try:
            matches = df_annos['type'].value_counts()[anno_type]
            if matches < 1:
                raise err
        except KeyError:
            raise err

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
        return self._collection

    @property
    def class_columns():
        pass

    @class_columns.getter
    def class_columns(self):
        return self._class_columns

    @property
    def anno_type():
        pass

    @anno_type.getter
    def anno_type(self) -> str:
        return self._anno_type_and_class[0]

    @property
    def train_tag():
        pass

    @train_tag.getter
    def train_tag(self) -> str:
        return self._train_tag

    @property
    def val_tag():
        pass

    @val_tag.getter
    def val_tag(self) -> str:
        return self._val_tag

    @property
    def train_rows():
        pass

    @train_rows.getter
    def train_rows(self) -> pd.DataFrame:
        """(DataFrame) Subset of collection rows of the train-tagged items."""

        return self.collection.get_rows_by_tags([self._train_tag])

    @property
    def val_rows():
        pass

    @val_rows.getter
    def val_rows(self):
        """(DataFrame) Subset of collection rows  of the val-tagged items."""

        return self.collection.get_rows_by_tags([self._val_tag])

    @property
    def train_annotations():
        pass

    @train_annotations.getter
    def train_annotations(self) -> pd.DataFrame:
        """
        Fetches training annotation data. Data rows/annotations will only be
        returned if the corresponding image is tagged with the provided
        train_tag.

        .DATA:
            If using row-data-oriented, returns a dataframe of only the
            relevant columns, in the order given  by 'class_columns' on
            construction.

        .ANYTHING_ELSE:
            If using SDK-annotation-oriented, returns a dataframe of all
            training annotations.
        """

        # .DATA: Just return the relevant columns of the training rows
        if self._is_data_oriented:
            return self.train_rows[self.class_columns]

        # .ANYTHING_ELSE: Get annotations belonging only to the train rows
        anno_type = self._anno_type_and_class[0]
        all_annos = pd.DataFrame(self.collection.get_annotations())
        mask = all_annos['type'] == anno_type
        relevant_annos = all_annos[mask]

        # Filter to annos belonging to train
        train_indices = self.train_rows.index
        relevant_annos['row_index'] = relevant_annos['image_index'].apply(
            self.collection.imageset_index_to_row_index)

        train_annos = relevant_annos[
            relevant_annos['row_index'].isin(train_indices)
        ]

        return train_annos

    @property
    def val_annotations():
        pass

    @val_annotations.getter
    def val_annotations(self) -> pd.DataFrame:
        """
        Fetches validation annotation data. Data rows/annotations will only be
        returned if the corresponding image is tagged with the provided
        val_tag.

        .DATA:
            If using row-data-oriented, returns a dataframe of only the
            relevant columns, in the order given  by 'class_columns' on
            construction.

        .ANYTHING_ELSE:
            If using SDK-annotation-oriented, returns a dataframe of all
            training annotations.
        """

        # .DATA: Just return the relevant columns of the training rows
        if self._is_data_oriented:
            return self.val_rows[self.class_columns]

        # .ANYTHING_ELSE: Get annotations belonging only to the train rows
        anno_type = self._anno_type_and_class[0]
        all_annos = pd.DataFrame(self.collection.get_annotations())
        mask = all_annos['type'] == anno_type
        relevant_annos = all_annos[mask]

        # Filter to annos belonging to train
        val_indices = self.val_rows.index
        relevant_annos['row_index'] = relevant_annos['image_index'].apply(
            self.collection.imageset_index_to_row_index)

        val_annos = relevant_annos[
            relevant_annos['row_index'].isin(val_indices)
        ]

        return val_annos
