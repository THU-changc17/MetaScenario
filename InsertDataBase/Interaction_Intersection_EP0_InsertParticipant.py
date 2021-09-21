import pymysql
import math
import pandas as pd
import numpy as np
from shapely import geometry
import functools
import matplotlib.pyplot as plt

conn = pymysql.connect(
    host='localhost',
    user="root",
    passwd="123456",
    db="Interaction_Intersection_EP0_Scenario_DB")
# 获取游标
cursor = conn.cursor()

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
    # polygon_point_list = np.array(list(left_node) + list(right_node))
    # left_index = np.where(left_node[:, 1] == np.min(left_node[:, 1]))
    # right_index = np.where(right_node[:, 2] == np.max(right_node[:, 2]))
    # print(left_index)
    # print(right_index)
    # center_x = 0.5 * (left_node[left_index[0][0]][1] + right_node[right_index[0][0]][1])
    # center_y = 0.5 * (left_node[left_index[0][0]][2] + right_node[right_index[0][0]][2])
    # center_x = 0.5 * (left_node[len(left_node) - 1][1] + right_node[0][1])
    # center_y = 0.5 * (left_node[len(left_node) - 1][2] + right_node[0][2])
    # center_x = 0.5 * (center_x1 + center_x2)
    # center_y = 0.5 * (center_y1 + center_y2)
    # center_x = np.mean(polygon_point_list[:, 1])
    # center_y = np.mean(polygon_point_list[:, 2])
    # polygon_point_list[:, 1] = polygon_point_list[:, 1] - center_x
    # polygon_point_list[:, 2] = polygon_point_list[:, 2] - center_y
    # polygon_point_list = np.array(sorted(polygon_point_list, key=functools.cmp_to_key(AngleCompare)))
    # polygon_point_list[:, 1] = polygon_point_list[:, 1] + center_x
    # polygon_point_list[:, 2] = polygon_point_list[:, 2] + center_y
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


def CreateTrafficTimingStateTable(table):
    cursor.execute('drop table if exists Traffic_timing_state' + table)
    TrafficTimingStateTable = """CREATE TABLE IF NOT EXISTS `Traffic_timing_state""" + table + """` (
    	  `data_id` bigint NOT NULL AUTO_INCREMENT,
    	  `time_stamp` bigint NOT NULL,
    	  `vehicle_id` bigint NOT NULL,
    	  `local_x` decimal(11,3),
    	  `local_y` decimal(11,3),
    	  `velocity_x` decimal(11,3),
    	  `velocity_y` decimal(11,3),
    	  `acceleration` decimal(11,3),
    	  `orientation` decimal(11,3),
    	  `lane_id` varchar(32),
    	  `preced_vehicle` bigint,
    	  `follow_vehicle` bigint,
    	  PRIMARY KEY (`data_id`)
    	) ENGINE=InnoDB  DEFAULT CHARSET=utf8 AUTO_INCREMENT=0"""
    cursor.execute(TrafficTimingStateTable)


def CreateTrafficParticipantPropertyTable(table):
    cursor.execute('drop table if exists Traffic_participant_property' + table)
    TrafficParticipantPropertyTable = """CREATE TABLE IF NOT EXISTS `Traffic_participant_property""" + table + """` (
        	  `vehicle_id` bigint NOT NULL,
        	  `vehicle_class` bigint NOT NULL,
        	  `vehicle_length` decimal(11,3),
        	  `vehicle_width` decimal(11,3),
        	  `vehicle_height` decimal(11,3),
        	  PRIMARY KEY (`vehicle_id`)
        	) ENGINE=InnoDB  DEFAULT CHARSET=utf8"""
    cursor.execute(TrafficParticipantPropertyTable)


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

# plt.figure(figsize=(14, 10))
# for plt_index in range(1, 5):
#     plt.subplot(2, 2, plt_index)
#     if plt_index == 1:
#         pylogon = polygon_CA2_4
#         plt.title("Conflict Area 2_4 Extract")
#     elif plt_index == 2:
#         pylogon = polygon_CA2_6
#         plt.title("Conflict Area 2_6 Extract")
#     elif plt_index == 3:
#         pylogon = polygon_CA5_7
#         plt.title("Conflict Area 5_7 Extract")
#     elif plt_index == 4:
#         pylogon = polygon_CA5_9
#         plt.title("Conflict Area 5_9 Extract")
#     pylogon_x = list(pylogon[:, 1])
#     pylogon_y = list(pylogon[:, 2])
#     pylogon_x.append(pylogon_x[0])
#     pylogon_y.append(pylogon_y[0])
#     plt.plot(pylogon_x, pylogon_y, '-')
#     plt.xlabel("localx")
#     plt.ylabel("localy")
#     for i in range(len(pylogon)):
#         plt.annotate(int(pylogon[i][0]), xy=(pylogon[i][1], pylogon[i][2]),
#                      xytext=(pylogon[i][1] + 0.1, pylogon[i][2] + 0.1))
#
# plt.savefig("../Inter_polygon.eps")
# plt.show()
# plot_polygon(polygon_1)
# plot_polygon(polygon_2)
# plot_polygon(polygon_3)
# plot_polygon(polygon_4)
# plot_polygon(polygon_5)
# plot_polygon(polygon_6)
# plot_polygon(polygon_7)
# plot_polygon(polygon_CA1_5)
# plot_polygon(polygon_CA1_6)
# plot_polygon(polygon_CA2_4)
# plot_polygon(polygon_CA2_5)
# plot_polygon(polygon_CA2_6)
# plot_polygon(polygon_CA5_7)
# plot_polygon(polygon_CA5_8)
# plot_polygon(polygon_CA5_9)
# plot_polygon(polygon_CA6_7)
# plot_polygon(polygon_CA10_11)

polygon_list = [polygon_1, polygon_2, polygon_3, polygon_4, polygon_5, polygon_6, polygon_7, polygon_8, polygon_9, polygon_10,
                polygon_11, polygon_CA1_5, polygon_CA1_6, polygon_CA2_4, polygon_CA2_5, polygon_CA2_6, polygon_CA5_7,
                polygon_CA5_8, polygon_CA5_9, polygon_CA6_7, polygon_CA10_11]

polygon_dict = {0: '1', 1: '2', 2: '3', 3: '4', 4: '5', 5: '6', 6: '7',
                7: '8', 8: '9', 9: '10', 10: '11', 11: 'CA1_5', 12: 'CA1_6',
                13: 'CA2_4', 14: 'CA2_5', 15: 'CA2_6', 16: 'CA5_7', 17: 'CA5_8',
                18: 'CA5_9', 19: 'CA6_7', 20: 'CA10_11'}


def InsertTable(table):
    # 取前n条数据做样例
    insertTimingSql = "insert into Traffic_timing_state" + table + "(time_stamp,vehicle_id,local_x,local_y,velocity_x,velocity_y,orientation,lane_id) " \
                "values(%s,%s,%s,%s,%s,%s,%s,%s)"
    for i in range(len(VehicleInfo)):
        time_stamp = VehicleInfo[i][2]
        # print(type(time_stamp))
        vehicle_id = VehicleInfo[i][0]
        # print(type(vehicle_id))
        local_x = VehicleInfo[i][4]
        local_y = VehicleInfo[i][5]
        lane_id = None
        for j in range(len(polygon_list)):
            if(inPoly(polygon_list[j][:, 1:], [local_x, local_y])):
                lane_id = polygon_dict.get(j)
        # print(lane_id)
        velocity_x = round(VehicleInfo[i][6], 3)
        velocity_y = round(VehicleInfo[i][7], 3)
        orientation = VehicleInfo[i][8]
        cursor.execute(insertTimingSql,(time_stamp,vehicle_id,local_x,local_y,velocity_x,velocity_y,orientation,lane_id))
        print(i)

    insertPropertySql = "insert into Traffic_participant_property" + table + "(vehicle_id,vehicle_class,vehicle_length,vehicle_width) " \
                "values(%s,%s,%s,%s) ON DUPLICATE KEY UPDATE vehicle_class = vehicle_class,vehicle_length = vehicle_length,vehicle_width = vehicle_width"
    for i in range(len(VehicleInfo)):
        # print(type(time_stamp))
        vehicle_id = VehicleInfo[i][0]
        vehicle_type = VehicleInfo[i][3]
        # print(type(vehicle_type))
        if(vehicle_type == "car"):
            vehicle_class = 1
        else:
            vehicle_class = 0
        vehicle_length = VehicleInfo[i][9]
        vehicle_width = VehicleInfo[i][10]
        cursor.execute(insertPropertySql,(vehicle_id,vehicle_class,vehicle_length,vehicle_width))
        print(i)


if __name__ == '__main__':
    CreateTrafficParticipantPropertyTable(table)
    CreateTrafficTimingStateTable(table)
    InsertTable(table)

    cursor.close()
    conn.commit()
    conn.close()