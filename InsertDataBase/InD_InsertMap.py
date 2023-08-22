import sys
sys.path.append("../")
import pymysql
import math
import pyproj
from pyproj import Transformer
import xml.etree.ElementTree as xml
import json
from InsertDataBase.CreateTables import *
from DBtools.init_db import init_DB
import argparse

class Point:
    def __init__(self):
        self.x = None
        self.y = None


# class LL2XYProjector:
#     def __init__(self, lat_origin, lon_origin):
#         self.lat_origin = lat_origin
#         self.lon_origin = lon_origin
#         # works for most tiles, and for all in the dataset
#         # self.zone = math.floor((lon_origin + 180.) / 6) + 1
#         self.p = pyproj.Proj(
#             proj='utm',
#             ellps='WGS84',
#             zone=32,
#             datum='WGS84')
#         [self.x_origin, self.y_origin] = self.p(lon_origin, lat_origin)
#
#     def latlon2xy(self, lat, lon):
#         [x, y] = self.p(lon, lat)
#         return [x - self.x_origin, y - self.y_origin]



def get_type(element):
    type_dict = dict()
    type_list = ["line_thin", "curbstone", "virtual", "wall", "fence"]
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


def InsertMapTable(cursor, file):
    # lat_origin = 50.78207
    # lon_origin = 6.07116
    # projector = LL2XYProjector(lat_origin, lon_origin)

    e = xml.parse(file).getroot()

    point_dict = dict()
    for node in e.findall("node"):
        point = Point()
        # https://developers.arcgis.com/javascript/3/jshelp/gcs.htm 4326: GCS_WGS_1984
        # https://developers.arcgis.com/javascript/3/jshelp/pcs.htm 32632: WGS_1984_UTM_Zone_32N
        tf = Transformer.from_crs(4326, 32632)
        point.x, point.y = tf.transform(float(node.get('lat')), float(node.get('lon')))
        point.x = point.x - 293487.12240
        point.y = point.y - 5629711.58163
        # point.x, point.y = projector.latlon2xy(float(node.get('lat')), float(node.get('lon')))
        point_dict[int(node.get('id'))] = point

    # insert node information
    insertNodeInfoSql = "insert into Node_Info(node_id,local_x,local_y) " \
                      "values(%s,%s,%s)"
    for node_id,point in point_dict.items():
        cursor.execute(insertNodeInfoSql,(node_id,round(point.x,3),round(point.y,3)))
        print(node_id)

    # insert way information
    insertWayInfoSql = "insert into Way_Info(way_id,way_type,road_channelization,dynamic_facility,static_facility,l_neighbor_id,r_neighbor_id,predecessor,successor) " \
                        "values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    insertNodeToWaySql = "insert into Node_To_Way(way_id,node_id) " \
                        "values(%s,%s)"
    insertLaneSql = "insert into Lane_Meta(lane_id,lane_property,l_adj_lane,r_adj_lane) values(%s,%s,%s,%s)"
    for way in e.findall('way'):
        way_type_dict = get_type(way)
        road_channelization_dict = get_road_channelization(way)
        dynamic_facility_dict = get_dynamic_facility(way)
        static_facility_dict = get_static_facility(way)
        adjacent_way_dict = get_adjacent_way(way)
        cursor.execute(insertWayInfoSql,(int(way.get('id')),json.dumps(way_type_dict),json.dumps(road_channelization_dict),
                                         json.dumps(dynamic_facility_dict),json.dumps(static_facility_dict),adjacent_way_dict.get("l_neighbor_id"),
                                         adjacent_way_dict.get("r_neighbor_id"),adjacent_way_dict.get("predecessor"),adjacent_way_dict.get("successor")))

        # insert way corresponding to nodes
        node_list = get_node_lists(way,point_dict)
        for node_id in node_list:
            cursor.execute(insertNodeToWaySql,(int(way.get('id')),node_id))
        # cursor.execute(insertLaneSql,(str(way.get('lane_id')), json.dumps(way_type_dict), adjacent_way_dict.get("l_neighbor_id"), adjacent_way_dict.get("r_neighbor_id")))
        print(int(way.get('id')))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # "InD_I_Scenario_DB"
    parser.add_argument('--DB', type=str, default=None)
    # '../DataSet/inD-dataset/lanelets/location1.osm'
    parser.add_argument('--File', type=str, default=None)
    args = parser.parse_args()
    conn, cursor = init_DB(args.DB)
    CreateNodeInfoTable(cursor)
    CreateWayInfoTable(cursor)
    CreateNodeToWayTable(cursor)
    CreateLaneMetaTable(cursor)
    CreateAdditionalConditionTable(cursor)
    InsertMapTable(cursor, args.File)

    cursor.close()
    conn.commit()
    conn.close()