from .block import Block
from .node_series import SeriesNode


class SeriesAddBlock(Block):
    """
    The series add block takes adds series A and B together and returns the
    result.

    Params:
        - 'Name' str (Required): The series/column name of the output series.
    """

    NAME = 'Series Add Block'
    INPUT_NODES = {
        'Series A': SeriesNode,
        'Series B': SeriesNode
    }
    OUTPUT_NODES = {'Series': SeriesNode}
    PARAMS = {'Name': '_REQUIRED_'}

    def execute(self):

        name = self.get_param('Name')

        if type(name) != str:
            raise TypeError(
                '{}: Name should be a str, not {}'.format(self, type(name)))

        a = self.input_nodes['Series A'].fetch()
        b = self.input_nodes['Series B'].fetch()

        added = a + b
        added.name = name

        self.output_nodes['Series'].data = added
