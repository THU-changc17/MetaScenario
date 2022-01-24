import pymysql
import math
import pandas as pd
import numpy as np
from shapely import geometry
import functools
import matplotlib.pyplot as plt
from InsertDataBase.CreateTables import *
from DBtools.init_db import init_DB


VehicleInfo = list()
table = "_7"
file = "../DataSet/INTERACTION-Dataset-DR-v1_1/recorded_trackfiles/DR_USA_Intersection_EP0/vehicle_tracks_007.csv"
VehicleInfo = pd.read_csv(file, decimal=",", low_memory=False)
VehicleInfo = np.array(VehicleInfo)
VehicleInfo = np.hstack((VehicleInfo[:, 0:4], VehicleInfo[:, 4:].astype(np.float)))
print(type(VehicleInfo))


def plot_polygon(pylogon):
    pylogon_x = list(pylogon[:, 1])
    pylogon_y = list(pylogon[:, 2])
    pylogon_x.append(pylogon_x[0])
    pylogon_y.append(pylogon_y[0])
    plt.plot(pylogon_x, pylogon_y, '-')
    for i in range(len(pylogon)):
        plt.annotate(pylogon[i][0], xy = (pylogon[i][1], pylogon[i][2]), xytext = (pylogon[i][1] + 0.1, pylogon[i][2] + 0.1))
    plt.show()


def inPoly(polygon, Points):
    line = geometry.LineString(polygon)
    point = geometry.Point(Points)
    polygon = geometry.Polygon(line)
    return polygon.contains(point)


def AngleCompare(pointA, pointB):
    angleA = math.atan2(pointA[2], pointA[1])
    angleB = math.atan2(pointB[2], pointB[1])
    if angleA<=angleB:
        return 1
    else:
        return -1


def ClockwiseSort(left_node, right_node):
    sorted_left_node = sorted(left_node, key=lambda x: (x[1], x[2]), reverse=True)
    sorted_right_node = sorted(right_node, key=lambda x: (x[1], x[2]))
    polygon_point_list = np.array(list(sorted_left_node) + list(sorted_right_node))
    return polygon_point_list


def RearrangeNodePolygon(lane_id):
    SearchLeftLaneNodesql = '''select Node_Info.node_id,local_x,local_y from (Node_Info join Node_To_Way on Node_Info.node_id = Node_To_Way.node_id)
        join Way_Info on Node_To_Way.way_id = Way_Info.way_id
        where l_border_of = '%s' group by Node_Info.node_id,local_x,local_y'''%(lane_id)
    cursor.execute(SearchLeftLaneNodesql)
    left_node = list()
    for i in cursor.fetchall():
        left_node.append([int(i[0]), float(i[1]), float(i[2])])

    SearchRightLaneNodesql = '''select Node_Info.node_id,local_x,local_y from (Node_Info join Node_To_Way on Node_Info.node_id = Node_To_Way.node_id)
            join Way_Info on Node_To_Way.way_id = Way_Info.way_id
            where r_border_of = '%s' group by Node_Info.node_id,local_x,local_y''' % (lane_id)
    cursor.execute(SearchRightLaneNodesql)
    right_node = list()
    for i in cursor.fetchall():
        right_node.append([int(i[0]), float(i[1]), float(i[2])])

    left_node = np.array(left_node)
    right_node = np.array(right_node)
    rearrange_node_list = ClockwiseSort(left_node, right_node)
    return rearrange_node_list


polygon_1 = RearrangeNodePolygon("1")
polygon_2 = RearrangeNodePolygon("2")
polygon_3 = RearrangeNodePolygon("3")
polygon_4 = RearrangeNodePolygon("4")
polygon_5 = RearrangeNodePolygon("5")
polygon_6 = RearrangeNodePolygon("6")
polygon_7 = RearrangeNodePolygon("7")
polygon_8 = RearrangeNodePolygon("8")
polygon_9 = RearrangeNodePolygon("9")
polygon_10 = RearrangeNodePolygon("10")
polygon_11 = RearrangeNodePolygon("11")
polygon_CA1_5 = RearrangeNodePolygon("CA1_5")
polygon_CA1_6 = RearrangeNodePolygon("CA1_6")
polygon_CA2_4 = RearrangeNodePolygon("CA2_4")
polygon_CA2_5 = RearrangeNodePolygon("CA2_5")
polygon_CA2_6 = RearrangeNodePolygon("CA2_6")
polygon_CA5_7 = RearrangeNodePolygon("CA5_7")
polygon_CA5_8 = RearrangeNodePolygon("CA5_8")
polygon_CA5_9 = RearrangeNodePolygon("CA5_9")
polygon_CA6_7 = RearrangeNodePolygon("CA6_7")
polygon_CA10_11 = RearrangeNodePolygon("CA10_11")


polygon_list = [polygon_1, polygon_2, polygon_3, polygon_4, polygon_5, polygon_6, polygon_7, polygon_8, polygon_9, polygon_10,
                polygon_11, polygon_CA1_5, polygon_CA1_6, polygon_CA2_4, polygon_CA2_5, polygon_CA2_6, polygon_CA5_7,
                polygon_CA5_8, polygon_CA5_9, polygon_CA6_7, polygon_CA10_11]

polygon_dict = {0: '1', 1: '2', 2: '3', 3: '4', 4: '5', 5: '6', 6: '7',
                7: '8', 8: '9', 9: '10', 10: '11', 11: 'CA1_5', 12: 'CA1_6',
                13: 'CA2_4', 14: 'CA2_5', 15: 'CA2_6', 16: 'CA5_7', 17: 'CA5_8',
                18: 'CA5_9', 19: 'CA6_7', 20: 'CA10_11'}


def InsertTable(cursor, table):
    insertTimingSql = "insert into Traffic_timing_state" + table + "(time_stamp,vehicle_id,local_x,local_y,velocity_x,velocity_y,orientation,lane_id) " \
                "values(%s,%s,%s,%s,%s,%s,%s,%s)"
    for i in range(len(VehicleInfo)):
        time_stamp = VehicleInfo[i][2]
        vehicle_id = VehicleInfo[i][0]
        local_x = VehicleInfo[i][4]
        local_y = VehicleInfo[i][5]
        lane_id = None
        for j in range(len(polygon_list)):
            if(inPoly(polygon_list[j][:, 1:], [local_x, local_y])):
                lane_id = polygon_dict.get(j)
        velocity_x = round(VehicleInfo[i][6], 3)
        velocity_y = round(VehicleInfo[i][7], 3)
        orientation = VehicleInfo[i][8]
        cursor.execute(insertTimingSql,(time_stamp,vehicle_id,local_x,local_y,velocity_x,velocity_y,orientation,lane_id))
        print(i)

    insertPropertySql = "insert into Traffic_participant_property" + table + "(vehicle_id,vehicle_class,vehicle_length,vehicle_width) " \
                "values(%s,%s,%s,%s) ON DUPLICATE KEY UPDATE vehicle_class = vehicle_class,vehicle_length = vehicle_length,vehicle_width = vehicle_width"
    for i in range(len(VehicleInfo)):
        vehicle_id = VehicleInfo[i][0]
        vehicle_type = VehicleInfo[i][3]
        if(vehicle_type == "car"):
            vehicle_class = 1
        else:
            vehicle_class = 0
        vehicle_length = VehicleInfo[i][9]
        vehicle_width = VehicleInfo[i][10]
        cursor.execute(insertPropertySql,(vehicle_id,vehicle_class,vehicle_length,vehicle_width))
        print(i)


if __name__ == '__main__':
    conn, cursor = init_DB("Interaction_Intersection_EP0_Scenario_DB")
    CreateTrafficParticipantPropertyTable(cursor, table)
    CreateTrafficTimingStateTable(cursor, table)
    InsertTable(cursor, table)

    cursor.close()
    conn.commit()
    conn.close()