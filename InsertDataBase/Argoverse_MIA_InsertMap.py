import pymysql
import math
import pyproj
import xml.etree.ElementTree as xml
import json

conn = pymysql.connect(
    host='localhost',
    user="root",
    passwd="123456",
    db="Argoverse_MIA_Scenario_DB")
cursor = conn.cursor()


class Point:
    def __init__(self):
        self.x = None
        self.y = None


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


def get_type(element):
    type_dict = dict()
    type_list = ["line_thin", "curbstone", "virtual"]
    type_dict["base"] = "center_line"
    for tag in element.findall("tag"):
        if tag.get("k") == "type" and tag.get("v") in type_list:
            type_dict["type"] = tag.get("v")
        if tag.get("k") == "subtype":
            type_dict["subtype"] = tag.get("v")
        if tag.get("k") == "highway":
            type_dict["highway"] = tag.get("v")
        if tag.get("k") == "oneway":
            type_dict["oneway"] = tag.get("v")
    type_dict["type"] = "line_thin"
    type_dict["subtype"] = "dashed"
    return type_dict


def get_road_channelization(element):
    road_channelization_dict = dict()
    channelization_list = ["pedestrian_marking", "stop_line", "road_border", "guard_rail", "is_intersection"]
    for tag in element.findall("tag"):
        if tag.get("k") == "stop_line":
            road_channelization_dict["stop_line"] = tag.get("v")
        if tag.get("k") == "turn_direction" and tag.get("v")!="NONE":
            road_channelization_dict["turn_direction"] = tag.get("v")
        if tag.get("k") == "road_border":
            road_channelization_dict["road_border"] = tag.get("v")
        if tag.get("k") == "pedestrian_marking":
            road_channelization_dict["pedestrian_marking"] = tag.get("v")
        if tag.get("k") == "guard_rail":
            road_channelization_dict["guard_rail"] = tag.get("v")
        if tag.get("k") == "is_intersection" and tag.get("v")!="False":
            road_channelization_dict["is_intersection"] = tag.get("v")
        if tag.get("k") == "type" and tag.get("v") in channelization_list:
            road_channelization_dict[tag.get("v")] = "yes"
    return road_channelization_dict


def get_dynamic_facility(element):
    dynamic_facility_dict = dict()
    for tag in element.findall("tag"):
        if tag.get("k") == "traffic_light":
            dynamic_facility_dict["traffic_light"] = tag.get("v")
    return dynamic_facility_dict


def get_static_facility(element):
    static_facility_dict = dict()
    for tag in element.findall("tag"):
        if tag.get("k") == "maxspeed":
            static_facility_dict["maxspeed"] = tag.get("v")
        if tag.get("k") == "traffic_sign":
            static_facility_dict["traffic_sign"] = tag.get("v")
    return static_facility_dict


def get_node_lists(element, point_dict):
    node_list = list()
    for nd in element.findall("nd"):
        pt_id = int(nd.get("ref"))
        node_list.append(pt_id)
    return node_list


def get_adjacent_way(element):
    adjacent_way_dict = dict()
    for tag in element.findall("tag"):
        if tag.get("k") == "l_neighbor_id" and tag.get("v")!="None":
            adjacent_way_dict["l_neighbor_id"] = int(tag.get("v"))
        if tag.get("k") == "r_neighbor_id" and tag.get("v")!="None":
            adjacent_way_dict["r_neighbor_id"] = int(tag.get("v"))
        if tag.get("k") == "predecessor" and tag.get("v")!="None":
            adjacent_way_dict["predecessor"] = int(tag.get("v"))
        if tag.get("k") == "successor" and tag.get("v")!="None":
            adjacent_way_dict["successor"] = int(tag.get("v"))
    return adjacent_way_dict


def InsertMapTable():
    e = xml.parse('../DataSet/Argoverse-Dataset/pruned_argoverse_MIA_10316_vector_map.xml').getroot()

    point_dict = dict()
    for node in e.findall("node"):
        point = Point()
        point.x, point.y = float(node.get('x')), float(node.get('y'))
        point_dict[int(node.get('id'))] = point

    # insert node information
    insertNodeInfoSql = "insert into Node_Info(node_id,local_x,local_y) " \
                      "values(%s,%s,%s)"
    for node_id,point in point_dict.items():
        cursor.execute(insertNodeInfoSql,(node_id,round(point.x,3),round(point.y,3)))
        print(node_id)

    # insert way information
    insertWayInfoSql = "insert into Way_Info(way_id,way_type,road_channelization,dynamic_facility,static_facility,center_line_of,l_neighbor_id,r_neighbor_id,predecessor,successor) " \
                        "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    insertNodeToWaySql = "insert into Node_To_Way(way_id,node_id) " \
                        "values(%s,%s)"
    insertLaneSql = "insert into Lane_Meta(lane_id,lane_property,l_adj_lane,r_adj_lane) values(%s,%s,%s,%s)"
    for way in e.findall('way'):
        way_type_dict = get_type(way)
        road_channelization_dict = get_road_channelization(way)
        dynamic_facility_dict = get_dynamic_facility(way)
        static_facility_dict = get_static_facility(way)
        adjacent_way_dict = get_adjacent_way(way)
        cursor.execute(insertWayInfoSql,(int(way.get('lane_id')),json.dumps(way_type_dict),json.dumps(road_channelization_dict),
                                         json.dumps(dynamic_facility_dict),json.dumps(static_facility_dict),str(way.get('lane_id')),adjacent_way_dict.get("l_neighbor_id"),
                                         adjacent_way_dict.get("r_neighbor_id"),adjacent_way_dict.get("predecessor"),adjacent_way_dict.get("successor")))

        # insert way corresponding to nodes
        node_list = get_node_lists(way,point_dict)
        for node_id in node_list:
            cursor.execute(insertNodeToWaySql,(int(way.get('lane_id')),node_id))
        cursor.execute(insertLaneSql,(str(way.get('lane_id')), json.dumps(way_type_dict), adjacent_way_dict.get("l_neighbor_id"), adjacent_way_dict.get("r_neighbor_id")))
        print(int(way.get('lane_id')))


if __name__ == '__main__':
    CreateNodeInfoTable()
    CreateWayInfoTable()
    CreateNodeToWayTable()
    CreateLaneMetaTable()
    CreateAdditionalConditionTable()
    InsertMapTable()

    cursor.close()
    conn.commit()
    conn.close()