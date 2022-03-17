"""
# The following graph selects from the column 'Random', adds it with itself,
# and inserts it into the original data as the column 'Random Doubled'.
"""

from zegami_sdk.client import ZegamiClient
from zegami_sdk.graph.graph import Graph
from zegami_sdk.graph.blocks import\
    DataInsertBlock, DataSelectSeriesBlock, SeriesAddBlock


zc = ZegamiClient()
w = zc.get_workspace_by_id('UUmCmTqp')
c = w.get_collection_by_id('621eecc8ead36612d0968d83')

graph = Graph(c)

inp = graph.input_block
out = graph.output_block

# Add a merge block
select = graph.add_block(DataSelectSeriesBlock(graph))
add = graph.add_block(SeriesAddBlock(graph))
insert = graph.add_block(DataInsertBlock(graph))

# Link the nodes up
inp.link('Data', select, 'Data')
select.set_param('Column', 'Random')
select.link('Series', add, 'Series A')
select.link('Series', add, 'Series B')
add.set_param('Name', 'Random Doubled')
inp.link('Data', insert, 'Data')
add.link('Series', insert, 'Series')
insert.link('Data', out, 'Data')
