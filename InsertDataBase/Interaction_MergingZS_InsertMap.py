import pymysql
import math
import pyproj
import pandas as pd
import numpy as np
import xml.etree.ElementTree as xml
import json
import pylab
import matplotlib.pyplot as plt
from InsertDataBase.CreateTables import *
from DBtools.init_db import init_DB


LaneCurve = [[[0.002024, -4.345, 3293], [0.001608, -3.417, 2779]],
            [[0.002086, -4.463, 3345], [0.002024, -4.345, 3293]],
            [[0.001798, -3.87, 3035], [0.002086, -4.463, 3345]],
            [[0.001814, -3.905, 3051], [0.001798, -3.87, 3035]],
            [[0.001805, -3.884, 3037], [0.001793, -3.858, 3020]],
            [[0.001793, -3.858, 3020], [0.001726, -3.719, 2944]],
            [[0.001726, -3.719, 2944], [0.002137, -4.565, 3374]]]

class Point:
    def __init__(self):
        self.x = None
        self.y = None


class LL2XYProjector:
    def __init__(self, lat_origin, lon_origin):
        self.lat_origin = lat_origin
        self.lon_origin = lon_origin
        # works for most tiles, and for all in the dataset
        self.zone = math.floor((lon_origin + 180.) / 6) + 1
        self.p = pyproj.Proj(
            proj='utm',
            ellps='WGS84',
            zone=self.zone,
            datum='WGS84')
        [self.x_origin, self.y_origin] = self.p(lon_origin, lat_origin)

    def latlon2xy(self, lat, lon):
        [x, y] = self.p(lon, lat)
        return [x - self.x_origin, y - self.y_origin]


def get_type(element):
    type_dict = dict()
    type_list = ["line_thin", "curbstone", "virtual"]
    type_dict["base"] = "lanelet"
    for tag in element.findall("tag"):
        if tag.get("k") == "type" and tag.get("v") in type_list:
            type_dict["type"] = tag.get("v")
        if tag.get("k") == "subtype":
            type_dict["subtype"] = tag.get("v")
        if tag.get("k") == "highway":
            type_dict["highway"] = tag.get("v")
        if tag.get("k") == "oneway":
            type_dict["oneway"] = tag.get("v")
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
        if tag.get("k") == "lane_change":
            road_channelization_dict["lane_change"] = tag.get("v")
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


def update_lane_border(border_list, lane_left, lane_right):
    for way in border_list:
        if lane_left:
            UpdateLeftLaneSql = "Update Way_Info set l_border_of = %s where way_id = %s"
            cursor.execute(UpdateLeftLaneSql % (lane_left, way))
        if lane_right:
            UpdateRightLaneSql = "Update Way_Info set r_border_of = %s where way_id = %s"
            cursor.execute(UpdateRightLaneSql % (lane_right, way))


def UpdateLaneWayConnection():
    border_1_2 = [10054, 10026, 10002, 10006]
    border_1_1_2_2 = [10005, 10019, 10007, 10001, 10054]
    border_2_1_3_2 = [10020, 10055, 10004, 10061, 10057]
    border_3_1_4_2 = [10052, 10021, 10041, 10003, 10009, 10011]
    border_4_1 = [10067, 10070, 10066, 10064, 10069]
    border_5_1 = [10062, 10047, 10018, 10042, 10012]
    border_5_2_6_1 = [10045, 10027, 10024, 10030, 10017, 10013]
    border_6_2_7_1 = [10044, 10028, 10022, 10031, 10016, 10014]
    border_7_2 = [10043, 10029, 10015, 10032, 10036, 10059, 10025]
    update_lane_border(border_1_2, None, str(1))
    update_lane_border(border_1_1_2_2, str(1), str(2))
    update_lane_border(border_2_1_3_2, str(2), str(3))
    update_lane_border(border_3_1_4_2, str(3), str(4))
    update_lane_border(border_4_1, str(4), None)
    update_lane_border(border_5_1, str(5), None)
    update_lane_border(border_5_2_6_1, str(6), str(5))
    update_lane_border(border_6_2_7_1, str(7), str(6))
    update_lane_border(border_7_2, None, str(7))


def FittingLaneCurve(lane_id):
    SearchLeftLaneNodesql = '''select Node_Info.node_id,local_x,local_y from (Node_Info join Node_To_Way on Node_Info.node_id = Node_To_Way.node_id)
        join Way_Info on Node_To_Way.way_id = Way_Info.way_id
        where l_border_of = %s group by Node_Info.node_id,local_x,local_y'''%(lane_id)
    cursor.execute(SearchLeftLaneNodesql)
    left_node = list()
    left_node_x = list()
    left_node_y = list()
    for i in cursor.fetchall():
        left_node.append(i[0])
        left_node_x.append(float(i[1]))
        left_node_y.append(float(i[2]))

    zipped = zip(left_node, left_node_x, left_node_y)
    sorted_zip = sorted(zipped, key=lambda x:(x[1],x[2]))
    result = zip(*sorted_zip)
    left, left_node_x, left_node_y = [list(x) for x in result]

    left_z = np.polyfit(left_node_x, left_node_y, 2)
    left_p = np.poly1d(left_z)
    y_pred_left = left_p(left_node_x)
    plot1 = pylab.plot(left_node_x, left_node_y, '*', label='original values')
    plot2 = pylab.plot(left_node_x, y_pred_left, 'r', label='fit values')
    print(lane_id, "left_border: ", left_p)


    SearchRightLaneNodesql = '''select Node_Info.node_id,local_x,local_y from (Node_Info join Node_To_Way on Node_Info.node_id = Node_To_Way.node_id)
            join Way_Info on Node_To_Way.way_id = Way_Info.way_id
            where r_border_of = %s group by Node_Info.node_id,local_x,local_y''' % (lane_id)
    cursor.execute(SearchRightLaneNodesql)
    right_node = list()
    right_node_x = list()
    right_node_y = list()
    for i in cursor.fetchall():
        right_node.append(i[0])
        right_node_x.append(float(i[1]))
        right_node_y.append(float(i[2]))

    zipped = zip(right_node, right_node_x, right_node_y)
    sorted_zip = sorted(zipped, key=lambda x: (x[1], x[2]))
    result = zip(*sorted_zip)
    right_node, right_node_x, right_node_y = [list(x) for x in result]

    right_z = np.polyfit(right_node_x, right_node_y, 2)
    right_p = np.poly1d(right_z)
    y_pred_right = right_p(right_node_x)
    plot1 = pylab.plot(right_node_x, right_node_y, 'o', label='original values')
    plot2 = pylab.plot(right_node_x, y_pred_right, 'y', label='fit values')
    print(lane_id, "right_border: ", right_p)
    pylab.show()
    return left_node_x, left_node_y, y_pred_left, right_node_x, right_node_y, y_pred_right


def InsertMapTable(cursor):
    lat_origin = 0
    lon_origin = 0
    projector = LL2XYProjector(lat_origin, lon_origin)

    e = xml.parse('../DataSet/INTERACTION-Dataset-DR-v1_1/maps/DR_CHN_Merging_ZS.osm').getroot()

    point_dict = dict()
    for node in e.findall("node"):
        point = Point()
        point.x, point.y = projector.latlon2xy(float(node.get('lat')), float(node.get('lon')))
        point_dict[int(node.get('id'))] = point

    insertNodeInfoSql = "insert into Node_Info(node_id,local_x,local_y) " \
                      "values(%s,%s,%s)"
    for node_id,point in point_dict.items():
        cursor.execute(insertNodeInfoSql,(node_id,round(point.x,3),round(point.y,3)))

    insertWayInfoSql = "insert into Way_Info(way_id,way_type,road_channelization,dynamic_facility,static_facility,l_neighbor_id,r_neighbor_id,predecessor,successor) " \
                        "values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    insertNodeToWaySql = "insert into Node_To_Way(way_id,node_id) " \
                        "values(%s,%s)"
    for way in e.findall('way'):
        way_type_dict = get_type(way)
        road_channelization_dict = get_road_channelization(way)
        dynamic_facility_dict = get_dynamic_facility(way)
        static_facility_dict = get_static_facility(way)
        adjacent_way_dict = get_adjacent_way(way)
        cursor.execute(insertWayInfoSql,
                       (int(way.get('id')), json.dumps(way_type_dict), json.dumps(road_channelization_dict),
                        json.dumps(dynamic_facility_dict), json.dumps(static_facility_dict),
                        adjacent_way_dict.get("l_neighbor_id"),
                        adjacent_way_dict.get("r_neighbor_id"), adjacent_way_dict.get("predecessor"),
                        adjacent_way_dict.get("successor")))

        node_list = get_node_lists(way,point_dict)
        for node_id in node_list:
            cursor.execute(insertNodeToWaySql,(int(way.get('id')), node_id))
        print(int(way.get('id')))

    insertLaneMetaSql = "insert into Lane_Meta(lane_id,lane_property,l_adj_lane,r_adj_lane) " \
                        "values(%s,%s,%s,%s)"
    l_adj_lane = {1: 2, 2: 3, 3: 4, 6: 5, 7: 6}
    r_adj_lane = {2: 1, 3: 2, 4: 3, 5: 6, 6: 7}
    for i in [1,2,3,4,5,6,7]:
        type_dict = dict()
        type_dict["base"] = "lanelet"
        cursor.execute(insertLaneMetaSql, (i, json.dumps(type_dict), l_adj_lane.get(i), r_adj_lane.get(i)))


if __name__ == '__main__':
    conn, cursor = init_DB("Interaction_MergingZS_Test_Scenario_DB")
    CreateNodeInfoTable(cursor)
    CreateWayInfoTable(cursor)
    CreateNodeToWayTable(cursor)
    CreateLaneMetaTable(cursor)
    CreateAdditionalConditionTable(cursor)
    InsertMapTable(cursor)
    UpdateLaneWayConnection()

    FittingLaneCurve(1)
    FittingLaneCurve(2)
    FittingLaneCurve(3)
    FittingLaneCurve(4)
    FittingLaneCurve(5)
    FittingLaneCurve(6)
    FittingLaneCurve(7)

    cursor.close()
    conn.commit()
    conn.close()