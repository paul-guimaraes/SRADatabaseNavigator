from pyvis.network import Network
from pyvis.edge import Edge


class PyvisNetwork(Network):
    def __init__(self,
                 height="600px",
                 width="100%",
                 directed=False,
                 notebook=False,
                 neighborhood_highlight=False,
                 select_menu=False,
                 filter_menu=False,
                 bgcolor="#ffffff",
                 font_color=False,
                 layout=None,
                 heading="",
                 cdn_resources="local"):
        super().__init__(
            height=height,
            width=width,
            directed=directed,
            notebook=notebook,
            neighborhood_highlight=neighborhood_highlight,
            select_menu=select_menu,
            filter_menu=filter_menu,
            bgcolor=bgcolor,
            font_color=font_color,
            layout=layout,
            heading=heading,
            cdn_resources=cdn_resources
        )
        self.__added_from = {}
        self.__added_dest = {}

    def add_edge(self, source, to, **options):
        """

        Adding edges is done based off of the IDs of the nodes. Order does
        not matter unless dealing with a directed graph.

        >>> nt.add_edge(0, 1) # adds an edge from node ID 0 to node ID
        >>> nt.add_edge(0, 1, value = 4) # adds an edge with a width of 4


        :param arrowStrikethrough: When false, the edge stops at the arrow.
                                   This can be useful if you have thick lines
                                   and you want the arrow to end in a point.
                                   Middle arrows are not affected by this.

        :param from: Edges are between two nodes, one to and one from. This
                     is where you define the from node. You have to supply
                     the corresponding node ID. This naturally only applies
                     to individual edges.

        :param hidden: When true, the edge is not drawn. It is part still part
                       of the physics simulation however!

        :param physics:	When true, the edge is part of the physics simulation.
                        When false, it will not act as a spring.

        :param title: The title is shown in a pop-up when the mouse moves over
                      the edge.

        :param to: Edges are between two nodes, one to and one from. This is
                   where you define the to node. You have to supply the
                   corresponding node ID. This naturally only applies to
                   individual edges.

        :param value: When a value is set, the edges' width will be scaled
                      using the options in the scaling object defined above.

        :param width: The width of the edge. If value is set, this is not used.


        :type arrowStrikethrough: bool
        :type from: str or num
        :type hidden: bool
        :type physics: bool
        :type title: str
        :type to: str or num
        :type value: num
        :type width: num
        """
        edge_exists = False

        # verify nodes exists
        assert source in self.get_nodes(), \
            "non existent node '" + str(source) + "'"

        assert to in self.get_nodes(), \
            "non existent node '" + str(to) + "'"

        # we only check existing edge for undirected graphs
        if not self.directed:
            if source in self.__added_from:
                if to == self.__added_from[source]:
                    edge_exists = True
            elif source in self.__added_dest:
                if to == self.__added_dest[source]:
                    edge_exists = True
        if not edge_exists:
            e = Edge(source, to, self.directed, **options)
            self.edges.append(e.options)
            # for verification of existing edges
            self.__added_from[source] = to
            if not self.directed:
                self.__added_dest[to] = source
