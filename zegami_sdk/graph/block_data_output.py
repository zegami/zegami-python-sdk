from .block import Block
from .node_data import DataNode


class DataOutputBlock(Block):
    """
    Currently, the data output block returns the resultant dataframe of
    the graph. Eventually, this should update Zegami's data.
    """

    NAME = 'Data Output Block'
    INPUT_NODES = {'Data': DataNode}

    def execute(self):

        return self.input_nodes['Data'].fetch()
