from .block import Block
from .node_data import DataNode
from .node_series import SeriesNode


class DataInsertBlock(Block):
    """
    The data insert block bolts the input series onto the input dataframe as
    a new column using their respective indices.
    """

    NAME = 'Data Append Block'
    INPUT_NODES = {
        'Data': DataNode,
        'Series': SeriesNode
    }
    OUTPUT_NODES = {'Data': DataNode}

    def execute(self):

        data = self.input_nodes['Data'].fetch().copy()
        series = self.input_nodes['Series'].fetch()

        data[series.name] = series

        self.output_nodes['Data'].data = data
