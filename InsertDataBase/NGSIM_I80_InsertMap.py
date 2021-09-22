import pandas
import numpy as np
import pymysql
import json


conn = pymysql.connect(
    host='localhost',
    user="root",
    passwd="123456",
    db="NGSIM_I80_Scenario_DB")
cursor = conn.cursor()


def CreateWayInfoTable():
    cursor.execute('drop table if exists Way_Info')
    WayInfoTable = """CREATE TABLE IF NOT EXISTS `Way_Info` (
        	  `way_id` bigint NOT NULL,
        	  `way_type` json,
        	  `road_channelization` json,
        	  `dynamic_facility` json,
        	  `static_facility` json,
        	  `l_border_of` varchar(32),
        	  `r_border_of` varchar(32),
        	  `center_line_of` varchar(32),
        	  `l_neighbor_id` bigint,
        	  `r_neighbor_id` bigint,
        	  `predecessor` bigint,
        	  `successor` bigint,
        	  PRIMARY KEY (`way_id`)
        	) ENGINE=InnoDB  DEFAULT CHARSET=utf8"""
    cursor.execute(WayInfoTable)


def CreateAdditionalConditionTable():
    cursor.execute('drop table if exists Additional_Condition')
    WayInfoTable = """CREATE TABLE IF NOT EXISTS `Additional_Condition` (
        	  `weather` varchar(32),
        	  `lighting` varchar(32),
        	  `temperature` varchar(32),
        	  `humidity` varchar(32)
        	) ENGINE=InnoDB  DEFAULT CHARSET=utf8"""
    cursor.execute(WayInfoTable)


def CreateNodeInfoTable():
    cursor.execute('drop table if exists Node_Info')
    NodeInfoTable = """CREATE TABLE IF NOT EXISTS `Node_Info` (
            	  `node_id` bigint NOT NULL,
            	  `local_x` decimal(11,3),
    	          `local_y` decimal(11,3),
    	          `global_x` decimal(11,3),
    	          `global_y` decimal(11,3),
            	  PRIMARY KEY (`node_id`)
            	) ENGINE=InnoDB  DEFAULT CHARSET=utf8"""
    cursor.execute(NodeInfoTable)


def CreateNodeToWayTable():
    cursor.execute('drop table if exists Node_To_Way')
    NodeToWayTable = """CREATE TABLE IF NOT EXISTS `Node_To_Way` (
                      `id` bigint NOT NULL AUTO_INCREMENT,
                	  `way_id` bigint NOT NULL,
                	  `node_id` bigint NOT NULL,
                	  PRIMARY KEY (`id`)
                	) ENGINE=InnoDB  DEFAULT CHARSET=utf8 AUTO_INCREMENT=0"""
    cursor.execute(NodeToWayTable)


def CreateLaneMetaTable():
    cursor.execute('drop table if exists Lane_Meta')
    LaneMetaTable = """CREATE TABLE IF NOT EXISTS `Lane_Meta` (
                      `lane_id` varchar(32) NOT NULL,
                	  `lane_property` json,
                	  `l_adj_lane` varchar(32),
                	  `r_adj_lane` varchar(32),
                	  PRIMARY KEY (`lane_id`)
                	) ENGINE=InnoDB  DEFAULT CHARSET=utf8 AUTO_INCREMENT=0"""
    cursor.execute(LaneMetaTable)


y_sign = -1
upper_lanes = np.array([-2, 11.5, 23.5, 35, 48, 60, 75, 90])
upper_lanes_shape = upper_lanes.shape


class Node:
    def __init__(self):
        self.id = None
        self.x = None
        self.y = None


node_x_list = list(np.linspace(0, 2000, 100))
node_y_list = list([y_sign * upper_lanes[0]]) * len(node_x_list)
way_node = list()
temp_way = list()
type_list = list()
id_sum = 0
for i in range(1000, len(node_x_list) + 1000):
    node = Node()
    node.id = i
    node.x = node_x_list[i - 1000]
    node.y = node_y_list[i - 1000]
    temp_way.append(node)
way_node.append(temp_way)
type_list.append("road_border")
id_sum = id_sum + len(temp_way)


for i in range(1, upper_lanes_shape[0] - 1):
    node_x_list = list(np.linspace(0, 2000, 100))
    node_y_list = list([y_sign * upper_lanes[i]]) * len(node_x_list)
    temp_way = list()
    for j in range(id_sum, len(node_x_list) + id_sum):
        node = Node()
        node.id = 1000 + j
        node.x = node_x_list[j - id_sum]
        node.y = node_y_list[j - id_sum]
        temp_way.append(node)
    way_node.append(temp_way)
    id_sum = id_sum + len(temp_way)
    type_list.append("line_thin;dashed")


node_x_list = list(np.linspace(0, 2000, 100))
node_y_list = list([y_sign * upper_lanes[-1]]) * len(node_x_list)
temp_way = list()
for i in range(id_sum, len(node_x_list) + id_sum):
    node = Node()
    node.id = 1000 + i
    node.x = node_x_list[i - id_sum]
    node.y = node_y_list[i - id_sum]
    temp_way.append(node)
way_node.append(temp_way)
id_sum = id_sum + len(temp_way)
type_list.append("road_border")


for i in range(len(way_node)):
    print(i)
    for j in way_node[i]:
        print(j.id)


if __name__ == '__main__':
    insertNodeInfoSql = "insert into Node_Info(node_id,local_x,local_y) " \
                          "values(%s,%s,%s)"
    insertWayInfoSql = "insert into Way_Info(way_id,way_type,road_channelization,l_neighbor_id,r_neighbor_id) " \
                            "values(%s,%s,%s,%s,%s)"
    insertNodeToWaySql = "insert into Node_To_Way(way_id,node_id) " \
                            "values(%s,%s)"
    insertLaneMetaSql = "insert into Lane_Meta(lane_id,lane_property,l_adj_lane,r_adj_lane) " \
                         "values(%s,%s,%s,%s)"
    UpdateWayLeftBorderSql = "Update Way_Info set l_border_of = %s where way_id = %s"
    UpdateWayRightBorderSql = "Update Way_Info set r_border_of = %s where way_id = %s"
    CreateWayInfoTable()
    CreateNodeToWayTable()
    CreateNodeInfoTable()
    CreateLaneMetaTable()
    CreateAdditionalConditionTable()
    l_border_dict = {1:0, 2:1, 3:2, 4:3, 5:4, 6:5, 7:6}
    r_border_dict = {1:1, 2:2, 3:3, 4:4, 5:5, 6:6, 7:7}
    l_adj_lane = {2:1, 3:2, 4:3, 5:4, 6:5 ,7:6}
    r_adj_lane = {1:2, 2:3, 3:4, 4:5, 5:6, 6:7}
    for i in [1,2,3,4,5,6,7]:
        type_dict = dict()
        type_dict["base"] = "lanelet"
        cursor.execute(insertLaneMetaSql, (str(i), json.dumps(type_dict), l_adj_lane.get(i), r_adj_lane.get(i)))

    for i in range(len(way_node)):
        way = way_node[i]
        type_dict = dict()
        road_channelization_dict = dict()
        type_dict["base"] = "lanelet"
        type_split = type_list[i].split(";")
        if (len(type_split) == 1):
            road_channelization_dict[type_split[0]] = "yes"
        elif (len(type_split) == 2):
            type_dict["type"] = type_split[0]
            type_dict["subtype"] = type_split[1]
        l_neighbor_id = None
        r_neighbor_id = None
        if(i>0):
            l_neighbor_id = 10000+i-1
        if(i<len(way_node)-1):
            r_neighbor_id = 10000+i+1
        way_id = int(10000+i)
        cursor.execute(insertWayInfoSql, (way_id,json.dumps(type_dict),json.dumps(road_channelization_dict),
                       l_neighbor_id,r_neighbor_id))
        for node in way:
            print(node.id)
            cursor.execute(insertNodeInfoSql, (node.id, round(node.x,3),round(node.y,3)))
            cursor.execute(insertNodeToWaySql, (way_id, node.id))

    for key, value in l_border_dict.items():
        cursor.execute(UpdateWayLeftBorderSql%(key, 10000 + value))
    for key, value in r_border_dict.items():
        cursor.execute(UpdateWayRightBorderSql%(key, 10000 + value))

    cursor.close()
    conn.commit()
    conn.close()