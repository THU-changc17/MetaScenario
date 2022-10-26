import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.lines as mlines
import networkx as nx
from relation_extractor import Relations, RelationExtractor, RELATION_COLORS, NODE_NTYPE, NODE_COLORS
from enum import Enum
from DBtools.init_db import init_DB


class NodeType(Enum):
    car = 0
    node = 1
    lane = 2


class GraphNode:
    def __init__(self, id, type=None):
        self.id = str(id)
        self.type = type.value if type != None else None

    def __repr__(self):
        return "%s" %(self.id)


class SceneGraph:
    def __init__(self, cursor, ego_node, time_stamp, table):
        self.cursor = cursor
        self.g = nx.MultiDiGraph()  # initialize scenegraph as networkx graph
        self.ego_node = ego_node
        self.relation_extractor = RelationExtractor(cursor, ego_node, table)
        self.timestamp = time_stamp
        self.vehicle_fill_color = list()


    def add_vehicle_vehicle_relation(self):
        self.relation_extractor.get_vehicle_relation(self.timestamp)
        rel_vehicle_list = self.relation_extractor.relation_vehicle
        self.relation_extractor.get_vehicle_vehicle_relation(self.timestamp)
        self.g.add_node(self.ego_node,id=self.ego_node,type=NodeType.car.value)
        for rel_vehicle in rel_vehicle_list:
            self.g.add_node(rel_vehicle,id=rel_vehicle,type=NodeType.car.value)

        fillcolor = ["red"] + ["blue"] * len(rel_vehicle_list)
        self.vehicle_fill_color = fillcolor

        rel_list = self.relation_extractor.relation_V2V_list
        for rel in rel_list:
            self.g.add_edge(rel[0],rel[2],object=rel[1],rel_type=rel[1].value)


    def add_vehicle_node_relation(self):
        self.relation_extractor.get_node_relation(self.timestamp)
        self.relation_extractor.get_vehicle_node_relation(self.timestamp)
        rel_node_list = self.relation_extractor.relation_node
        for rel_node in rel_node_list:
            self.g.add_node(rel_node,id=rel_node,type=NodeType.node.value)
        self.vehicle_fill_color = self.vehicle_fill_color + ["green"] * len(rel_node_list)
        rel_list = self.relation_extractor.relation_V2N_list
        for rel in rel_list:
            self.g.add_edge(rel[0], rel[2], object=rel[1], rel_type=rel[1].value)


    def add_vehicle_lane_relation(self):
        self.relation_extractor.get_vehicle_lane_relation(self.timestamp)
        self.relation_extractor.get_lane_lane_relation()
        rel_lane_list = self.relation_extractor.relation_lane
        for rel_lane in rel_lane_list:
            self.g.add_node("lane "+str(rel_lane),id=rel_lane,type=NodeType.lane.value)
        self.vehicle_fill_color = self.vehicle_fill_color + ["yellow"] * len(rel_lane_list)
        rel_V2L_list = self.relation_extractor.relation_V2L_list
        for rel in rel_V2L_list:
            self.g.add_edge(rel[0], "lane "+str(rel[2]), object=rel[1], rel_type=rel[1].value)
        rel_L2L_list = self.relation_extractor.relation_L2L_list
        for rel in rel_L2L_list:
            self.g.add_edge("lane "+str(rel[0]), "lane "+str(rel[2]), object=rel[1], rel_type=rel[1].value)


    def visualize(self):
        plt.figure(figsize=(8,8))
        pos = nx.kamada_kawai_layout(self.g)
        nx.draw_networkx_nodes(self.g, pos, node_size=100, node_color=self.vehicle_fill_color)
        ax = plt.gca()
        for e in self.g.edges:
            relation_value = int(self.g.get_edge_data(e[0], e[1])[e[2]]["object"].value)
            ax.annotate("",
                        xy=pos[e[1]], xycoords='data',
                        xytext=pos[e[0]], textcoords='data',
                        arrowprops=dict(arrowstyle="->", color=RELATION_COLORS[relation_value],
                                        shrinkA=5, shrinkB=5,
                                        patchA=None, patchB=None,
                                        connectionstyle="arc3,rad=rrr".replace('rrr', str(0.1 * (e[2] + 1))),
                                        )
                        )
            ax.text(pos[e[1]][0], pos[e[1]][1], e[1], zorder=20, fontsize=13)
            ax.text(pos[e[0]][0], pos[e[0]][1], e[0], zorder=20, fontsize=13)

        legend_edge = [mlines.Line2D([],[], color=RELATION_COLORS[int(i.value)], label="{:s}".format(i.name)) for i in Relations]
        box = ax.get_position()
        ax.set_position([box.x0, box.y0, box.width, box.height * 0.8])
        ax.legend(handles=legend_edge, bbox_to_anchor=(1.0, 1.2), ncol=4)
        plt.axis('off')

        # ax2 add nodes legend
        legend_node = [mpatches.Circle((),radius=0.05,color=NODE_COLORS[i], label="{:s}".format(NODE_NTYPE[i])) for i in
                       range(len(NODE_NTYPE))]
        ax2 = ax.twinx()
        ax2.legend(handles=legend_node, bbox_to_anchor=(1.0, 1.0), ncol=4)

        plt.axis('off')
        # plt.savefig('../%s_%s.eps' % (self.ego_node, self.timestamp))
        plt.show()


if __name__=='__main__':
    # conn, cursor = init_DB("HighD_I_Scenario_DB")
    # Graph = SceneGraph(cursor, 211, 185240, "_1")
    conn, cursor = init_DB("Interaction_MergingZS_Scenario_DB")
    Graph = SceneGraph(cursor, 11, 100, "_8")
    Graph.add_vehicle_vehicle_relation()
    Graph.add_vehicle_node_relation()
    Graph.add_vehicle_lane_relation()
    Graph.visualize()
