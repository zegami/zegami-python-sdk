from .block import Block
from .node_data import DataNode


class DataInputBlock(Block):
    """
    The data input block executes by fetching row data from the Zegami
    collection.
    """

    NAME = 'Data Input Block'
    OUTPUT_NODES = {'Data': DataNode}

    def execute(self):

        self.output_nodes['Data'].data = self.graph.collection.rows
