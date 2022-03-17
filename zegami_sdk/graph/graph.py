from .block_data_input import DataInputBlock
from .block_data_output import DataOutputBlock


class Graph():

    def __init__(self, collection):

        self._collection = collection

        # Set up initial single fixed input/output data blocks
        self._input_block = DataInputBlock(self)
        self._output_block = DataOutputBlock(self)
        self._blocks = [self.input_block, self.output_block]

        # By default, link the input and output block nodes
        self.output_block.input_nodes['Data'].link(
            self.input_block.output_nodes['Data'])

    @property
    def collection():
        pass

    @collection.getter
    def collection(self):
        return self._collection

    @property
    def client():
        pass

    @client.getter
    def client(self):
        return self.collection.workspace.client

    @property
    def blocks(self):
        pass

    @blocks.getter
    def blocks(self):
        """
        All blocks contained within the graph. Each graph contains at least
        an input data block and an output data block.
        """

        return self._blocks

    @property
    def input_block():
        pass

    @input_block.getter
    def input_block(self):
        """The singular input data block that every graph contains."""

        return self._input_block

    @property
    def output_block():
        pass

    @output_block.getter
    def output_block(self):
        """The singular output data block that every graph contains."""

        return self._output_block

    def add_block(self, block):
        """Adds a block to the graph."""

        if type(block) == DataInputBlock:
            raise TypeError('Adding more data input nodes is restricted')

        if type(block) == DataOutputBlock:
            raise TypeError('Adding more data output nodes is restricted')

        # Don't allow re-adding of the same block instance
        if block in self.blocks:
            raise ValueError('Block instance already exists within graph.')

        self._blocks.append(block)

        return block

    def execute(self):
        """
        Executes the graph from output -> input, calling a chain of blocks
        to arrive at the final required outcome.
        """

        return self.output_block.execute()
