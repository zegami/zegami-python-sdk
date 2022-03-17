

class Node():

    NAME = 'Abstract Node'

    def __repr__(self):
        return '<node: {} ({})>'.format(
            self.NAME, 'Input' if self.is_input else 'Output')

    def __init__(self, block, is_input=True):

        self._block = block
        self._is_input = is_input
        self.data = None

        # For input
        self._linked_output = None

        # For output
        self._linked_inputs = []

    @property
    def is_input():
        pass

    @is_input.getter
    def is_input(self):
        """
        Whether or not this node is on the input side of the block, as opposed
        to the output side. Must be one or the other.
        """

        return self._is_input

    @property
    def block():
        pass

    @block.getter
    def block(self):
        return self._block

    def _handle_link(self, node):
        if self.partner:
            raise ValueError(
                '{}: _handle_link() called while already having a partner.'
                .format(self.NAME))
        self._partner = node

    def unlink(self):
        """
        Unlinks this node from the other, setting both nodes' partners to
        None.
        """

        out = self._linked_output
        ins = self._linked_inputs

        if self.is_input:
            if out and self in out._linked_inputs:
                out._linked_inputs.remove(self)
            self._linked_output = None

        else:
            for inp in ins:
                if inp._linked_output is self:
                    inp._linked_output is None
            self._linked_inputs = []

    def link(self, node):
        """
        Link an input to an output node. Output nodes may share with as many
        inputs as they like, but each input may only read from one output.
        """

        # Input|Output only
        if node.is_input == self._is_input:
            me = 'input' if self._is_input else 'output'
            you = 'input' if not self._is_input else 'output'
            raise ValueError(
                '{}: Cannot link {} node to this {} node'
                .format(self.NAME, you, me))

        # Nodes must be the same type to link
        if str(type(self)) != str(type(node)):
            raise TypeError(
                'Nodes trying to link with mismatched types: {} vs {}'
                .format(type(self), type(node)))

        if self.is_input:
            if self._linked_output:
                self.unlink()
            self._linked_output = node
            node._linked_inputs.append(self)

        else:
            if node._linked_output and node._linked_output is not self:
                node.unlink()
            self._linked_inputs.append(node)
            node._linked_output = self

    def fetch(self):
        """
        As an output node, fetches the data linked to this node. As an input
        node, calls fetch on the linked output node.
        """

        if self.is_input:
            if self._linked_output:
                return self._linked_output.fetch()
            else:
                raise ValueError(
                    'Node fetch error: No output linked to input {} node'
                    .format(self.NAME))

        if self.data is None:
            self.block.execute()
            if self.data is None:
                raise ValueError(
                    'Node fetch error: No data present, despite requesting '
                    'execution from owning block ({})'
                    .format(self.NAME))

        return self.data
