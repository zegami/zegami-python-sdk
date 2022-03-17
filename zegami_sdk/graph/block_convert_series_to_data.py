from .block import Block
from .node_series import SeriesNode
from .node_data import DataNode

from pandas import DataFrame


class ConvertSeriesToDataBlock(Block):
    """
    The series to data conversion block wraps the input series up as a
    dataframe.
    """

    NAME = 'Series To Data Block'
    INPUT_NODES = {'Series': SeriesNode}
    OUTPUT_NODES = {'Data': DataNode}

    def execute(self):

        series = self.input_nodes['Series'].fetch()

        self.output_nodes['Data'].data = DataFrame(series)
