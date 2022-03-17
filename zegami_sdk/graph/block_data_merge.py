from .block import Block
from .node_data import DataNode


class DataMergeBlock(Block):
    """
    The data merge block takes dataframes A and B and merges their columns
    into one complete dataframe based on the 'Merge Column'.

    Params:
        - 'Merge Column' : The column in common between the two dataframes
        to merge on.
    """

    NAME = 'Data Merge Block'
    INPUT_NODES = {
        'Data A': DataNode,
        'Data B': DataNode
    }
    OUTPUT_NODES = {'Data': DataNode}
    PARAMS = {'Merge Column': 'Filename'}

    def execute(self):

        a = self.input_nodes['Data A'].fetch()
        b = self.input_nodes['Data B'].fetch()

        merge_col = self.get_param('Merge Column')

        for df in [a, b]:
            if merge_col not in df.columns:
                raise ValueError(
                    '{}: Merge Column "{}" missing from dataframe cols: {}'
                    .format(self, merge_col, df.columns))

        merged = a.merge(b, on=self._params['Merge Column'])

        self.output_nodes['Data'].data = merged
