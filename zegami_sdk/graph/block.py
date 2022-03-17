

class Block():

    NAME = 'Abstract Block'
    INPUT_NODES = {}
    OUTPUT_NODES = {}
    PARAMS = {}

    def __repr__(self):
        return '<block "{}": {}>'.format(self.name, self.NAME)

    def __init__(self, graph, name='Block'):
        """
        The base abstract block class. Blocks are inserted into graphs to
        act as data fetchers, senders or processors.

        Subclasses should define lists of input/output node types as lists
        of the classes required.
        """

        self._graph = graph
        self.name = name

        self._input_nodes = {}
        self._output_nodes = {}
        self._params = {}

        # Graphs cannot have no nodes
        if len(self.INPUT_NODES) == 0 and len(self.OUTPUT_NODES) == 0:
            raise ValueError(
                'Block class {} has no specified input or output nodes.'
                .format(type(self)))

        self._construct_nodes()
        self._set_default_params()

    @property
    def graph():
        pass

    @graph.getter
    def graph(self):
        """The owning graph utilising this block."""

        return self._graph

    @property
    def input_nodes():
        pass

    @input_nodes.getter
    def input_nodes(self):
        return self._input_nodes

    @property
    def output_nodes():
        pass

    @output_nodes.getter
    def output_nodes(self):
        return self._output_nodes

    def _construct_nodes(self):
        """
        Instantiates and populates nodes based on INPUT_NODES/OUTPUT_NODES.
        """

        # Build input nodes
        self._input_nodes = {
            k: v(self, True) for k, v in self.INPUT_NODES.items()}

        # Build output nodes
        self._output_nodes = {
            k: v(self, False) for k, v in self.OUTPUT_NODES.items()}

    def _set_default_params(self):
        """
        Sets up parameters using defaults in self.PARAMS. These can be altered
        after instantiation using configure_param(key, value).
        """

        self._params = {k: v for k, v in self.PARAMS.items()}

    def set_param(self, key, value):
        """
        Configure a parameter appropraite to this block. Will fail if the
        parameter key is invalid.
        """

        if key not in self._params.keys():
            raise KeyError(
                'Parameter "{}" not appropraite for {} block'
                .format(key, self.NAME))

        self._params[key] = value

    def get_param(self, key):
        """
        Get a parameter by its key. If it is an invalid key or the value
        is _REQUIRED_, throws an error. _REQUIRED_ params are not
        default-appropriate and must be set before graph execution.
        """

        if key not in self._params.keys():
            raise KeyError(
                '{}: Parameter "{}" not appropriate'.format(self, key))

        v = self._params[key]
        if v == '_REQUIRED_':
            raise ValueError(
                '{}: Parameter "{}" is marked as required, use set_param'
                .foramt(self, key))

        return v

    def link(self, out_node_name, other_block, other_block_in_node_name):
        """
        Shortcut for linking the an output to the input of another block,
        such as .link('Data', other, 'Data'). Fails on invalid names.
        """

        out = self.output_nodes[out_node_name]
        inp = other_block.input_nodes[other_block_in_node_name]

        out.link(inp)

    def execute(self):
        """
        Executes the block, utilising inputs to generate an output.
        Override in subclasses to add block functionality. Execution is
        controlled by the graph (back to front).

        Once complete, should assign data to all output nodes .data attributes.
        """

        return None
