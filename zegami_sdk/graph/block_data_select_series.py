from .block import Block
from .node_series import SeriesNode
from .node_data import DataNode


class DataSelectSeriesBlock(Block):
    """
    The data select series block picks a column from the dataframe.
    The actual column being extracted needs to be provided.

    Params:
        - 'Column' str|int (Required): The chosen series/column name. Using an
        int will attempt to return the columns[int] column instead.
    """

    NAME = 'Data To Series Block'
    INPUT_NODES = {'Data': DataNode}
    OUTPUT_NODES = {'Series': SeriesNode}
    PARAMS = {'Column': '_REQUIRED_'}

    def execute(self):

        col = self.get_param('Column')
        data = self.input_nodes['Data'].fetch()

        if type(col) == int:
            try:
                col = data.columns[col]
            except IndexError as e:
                raise IndexError('{}: {}'.format(self, e))

        if col not in data.columns:
            raise KeyError(
                '{}: Column name "{}" not in dataframe\'s columns: {}'
                .format(self, col, data.columns))

        self.output_nodes['Series'].data = data[col]
