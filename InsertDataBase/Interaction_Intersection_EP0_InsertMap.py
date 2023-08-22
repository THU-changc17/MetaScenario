import sys
sys.path.append("../")
import pymysql
import math
import pyproj
import pandas as pd
import numpy as np
import xml.etree.ElementTree as xml
import json
import pylab
import argparse
from InsertDataBase.CreateTables import *
from DBtools.init_db import init_DB


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


def update_lane_border(cursor, border_list, lane_left, lane_right):
    for way in border_list:
        if lane_left:
            UpdateLeftLaneSql = '''Update Way_Info set l_border_of = %s where way_id = %s'''
            cursor.execute(UpdateLeftLaneSql, (lane_left, way))
        if lane_right:
            UpdateRightLaneSql = '''Update Way_Info set r_border_of = %s where way_id = %s'''
            cursor.execute(UpdateRightLaneSql, (lane_right, way))


def UpdateLaneWayConnection(cursor):
    border_1_2 = [10011]
    update_lane_border(cursor, border_1_2, None , '1')
    border_1_1 =[10068]
    update_lane_border(cursor, border_1_1, '1', None)

    border_2_2 = [10068]
    update_lane_border(cursor, border_2_2, None, '2')
    border_2_1 = [10079]
    update_lane_border(cursor, border_2_1, '2', None)

    border_CA1_5_2 = [10044]
    update_lane_border(cursor, border_CA1_5_2, None, 'CA1_5')
    border_CA1_5_1 = [10014]
    update_lane_border(cursor, border_CA1_5_1, 'CA1_5', None)

    border_5_1 = [10065, 10022, 10062, 10049, 10047, 10046]
    update_lane_border(cursor, border_5_1, '5', None)
    border_5_2 = [10055, 10039, 10053, 10059, 10005, 10066, 10041]
    update_lane_border(cursor, border_5_2, None, '5')

    border_3_2 = [10020, 10026]
    update_lane_border(cursor, border_3_2, None, '3')
    border_3_1 = [10055]
    update_lane_border(cursor, border_3_1, '3', None)

    border_CA2_4_2 = [10033]
    update_lane_border(cursor, border_CA2_4_2, None, 'CA2_4')
    border_CA2_4_1 = [10015]
    update_lane_border(cursor, border_CA2_4_1, 'CA2_4', None)

    border_4_2 = [10081, 10077, 10075, 10073, 10043]
    update_lane_border(cursor, border_4_2, None, '4')
    border_4_1 = [10053, 10059, 10005, 10066, 10041]
    update_lane_border(cursor, border_4_1, '4', None)

    border_CA2_5_2 = [10015]
    update_lane_border(cursor, border_CA2_5_2, None, 'CA2_5')
    border_CA2_5_1 = [10012]
    update_lane_border(cursor, border_CA2_5_1, 'CA2_5', None)

    border_6_2 = [10065, 10022, 10062, 10049, 10047, 10040]
    border_6_1 = [10027, 10025, 10038, 10054, 10042, 10061, 10057]
    update_lane_border(cursor, border_6_2, None, '6')
    update_lane_border(cursor, border_6_1, '6', None)

    border_CA1_6_2 = [10012]
    border_CA1_6_1 = [10083]
    update_lane_border(cursor, border_CA1_6_1, 'CA1_6', None)
    update_lane_border(cursor, border_CA1_6_2, None, 'CA1_6')

    border_CA2_6_2 = [10014]
    border_CA2_6_1 = [10013]
    update_lane_border(cursor, border_CA2_6_1, 'CA2_6', None)
    update_lane_border(cursor, border_CA2_6_2, None, 'CA2_6')

    border_7_1 = [10104]
    update_lane_border(cursor, border_7_1, '7', None)
    border_7_2 = [10101]
    update_lane_border(cursor, border_7_2, None, '7')

    border_CA5_7_1 = [10003]
    update_lane_border(cursor, border_CA5_7_1, 'CA5_7', None)
    border_CA5_7_2 = [10002]
    update_lane_border(cursor, border_CA5_7_2, None, 'CA5_7')

    border_8_2 = [10104, 10099]
    border_8_1 = [10102, 10098]
    update_lane_border(cursor, border_8_1, '8', None)
    update_lane_border(cursor, border_8_2, None, '8')

    border_CA6_7_1 = [10035]
    update_lane_border(cursor, border_CA6_7_1, 'CA6_7', None)
    border_CA6_7_2 = [10056]
    update_lane_border(cursor, border_CA6_7_2, None, 'CA6_7')

    border_CA4_8_2 = [10016]
    update_lane_border(cursor, border_CA4_8_2, None, 'CA4_8')
    border_CA4_8_1 = [10017]
    update_lane_border(cursor, border_CA4_8_1, 'CA4_8', None)

    border_CA5_8_1 = [10018]
    border_CA5_8_2 = [10035]
    update_lane_border(cursor, border_CA5_8_2, None, 'CA5_8')
    update_lane_border(cursor, border_CA5_8_1, 'CA5_8', None)

    border_CA6_8_1 = [10019]
    border_CA6_8_2 = [10100]
    update_lane_border(cursor, border_CA6_8_2, None, 'CA6_8')
    update_lane_border(cursor, border_CA6_8_1, 'CA6_8', None)

    border_9_1 = [10106]
    update_lane_border(cursor, border_9_1, '9', None)
    border_9_2 = [10103]
    update_lane_border(cursor, border_9_2, None, '9')

    border_10_1 = [10089]
    border_10_2 = [10106]
    update_lane_border(cursor, border_10_1, '10', None)
    update_lane_border(cursor, border_10_2, None, '10')

    border_CA5_9_1 = [10010]
    update_lane_border(cursor, border_CA5_9_1, 'CA5_9', None)
    border_CA5_9_2 = [10007]
    update_lane_border(cursor, border_CA5_9_2, None, 'CA5_9')

    border_CA6_10_1 = [10078]
    update_lane_border(cursor, border_CA6_10_1, 'CA6_10', None)
    border_CA6_10_2 = [10010]
    update_lane_border(cursor, border_CA6_10_2, None, 'CA6_10')

    border_11_1 = [10032]
    update_lane_border(cursor, border_11_1, '11', None)
    border_11_2 = [10057]
    update_lane_border(cursor, border_11_2, None, '11')

    border_CA10_11_1 = [10087]
    update_lane_border(cursor, border_CA10_11_1, 'CA10_11', None)
    border_CA10_11_2 = [10050]
    update_lane_border(cursor, border_CA10_11_2, None, 'CA10_11')


def InsertMapTable(cursor, file):
    lat_origin = 0
    lon_origin = 0
    projector = LL2XYProjector(lat_origin, lon_origin)

    e = xml.parse(file).getroot()

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
            cursor.execute(insertNodeToWaySql,(int(way.get('id')),node_id))
        print(int(way.get('id')))

    insertLaneMetaSql = "insert into Lane_Meta(lane_id,lane_property,l_adj_lane,r_adj_lane) " \
                        "values(%s,%s,%s,%s)"
    l_adj_lane = {}
    r_adj_lane = {}
    for i in ['1','2','3','4','5','6','7','8','9','10','11', 'CA1_5','CA1_6','CA2_4','CA2_5','CA2_6','CA4_8','CA5_7','CA5_8','CA5_9','CA6_7', 'CA6_8', 'CA6_10','CA10_11']:
        type_dict = dict()
        type_dict["base"] = "lanelet"
        cursor.execute(insertLaneMetaSql, (i, json.dumps(type_dict), l_adj_lane.get(i), r_adj_lane.get(i)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # "Interaction_Intersection_EP0_Scenario_DB"
    parser.add_argument('--DB', type=str, default=None)
    # '../DataSet/INTERACTION-Dataset-DR-v1_1/maps/DR_USA_Intersection_EP0.osm'
    parser.add_argument('--File', type=str, default=None)
    args = parser.parse_args()
    conn, cursor = init_DB(args.DB)
    CreateNodeInfoTable(cursor)
    CreateWayInfoTable(cursor)
    CreateNodeToWayTable(cursor)
    CreateLaneMetaTable(cursor)
    CreateAdditionalConditionTable(cursor)
    InsertMapTable(cursor, args.File)
    UpdateLaneWayConnection(cursor)

    conn.commit()
    cursor.close()
    conn.close()