import json
import numpy as np
from scipy.spatial import distance
from shapely import geometry
from init_db import init_DB
from collections import Counter


def rotate_around_center(pts, center, yaw):
    return np.dot(pts - center, np.array([[np.cos(yaw), np.sin(yaw)], [-np.sin(yaw), np.cos(yaw)]])) + center


def polygon_xy_from_motionstate(x, y, psi_rad, width, length):
    lowleft = (x - length / 2., y - width / 2.)
    lowright = (x + length / 2., y - width / 2.)
    upright = (x + length / 2., y + width / 2.)
    upleft = (x - length / 2., y + width / 2.)
    return rotate_around_center(np.array([lowleft, lowright, upright, upleft]), np.array([x, y]), yaw=psi_rad)


def if_inPoly(polygon, Points):
    line = geometry.LineString(polygon)
    point = geometry.Point(Points)
    polygon = geometry.Polygon(line)
    return polygon.contains(point)


def cal_polymindist(ego_rotate, other_rotate):
    point_list_ego = list()
    for i in range(10):
        inter01 = (ego_rotate[1] - ego_rotate[0]) / 10 * i + ego_rotate[0]
        inter12 = (ego_rotate[2] - ego_rotate[1]) / 10 * i + ego_rotate[1]
        inter23 = (ego_rotate[3] - ego_rotate[2]) / 10 * i + ego_rotate[2]
        inter30 = (ego_rotate[0] - ego_rotate[3]) / 10 * i + ego_rotate[3]
        point_list_ego.append(inter01)
        point_list_ego.append(inter12)
        point_list_ego.append(inter23)
        point_list_ego.append(inter30)
    point_list_other = list()
    for i in range(10):
        inter01 = (other_rotate[1] - other_rotate[0]) / 10 * i + other_rotate[0]
        inter12 = (other_rotate[2] - other_rotate[1]) / 10 * i + other_rotate[1]
        inter23 = (other_rotate[3] - other_rotate[2]) / 10 * i + other_rotate[2]
        inter30 = (other_rotate[0] - other_rotate[3]) / 10 * i + other_rotate[3]
        point_list_other.append(inter01)
        point_list_other.append(inter12)
        point_list_other.append(inter23)
        point_list_other.append(inter30)
    point_list_ego = np.array(point_list_ego)
    point_list_other = np.array(point_list_other)
    # print(point_list_ego.shape)
    #     # print(point_list_other.shape)
    return distance.cdist(point_list_ego, point_list_other).min(axis=1).min(axis=0)


def cal_poly_point_mindist(ego_rotate, other):
    point_list_ego = list()
    for i in range(10):
        inter01 = (ego_rotate[1] - ego_rotate[0]) / 10 * i + ego_rotate[0]
        inter12 = (ego_rotate[2] - ego_rotate[1]) / 10 * i + ego_rotate[1]
        inter23 = (ego_rotate[3] - ego_rotate[2]) / 10 * i + ego_rotate[2]
        inter30 = (ego_rotate[0] - ego_rotate[3]) / 10 * i + ego_rotate[3]
        point_list_ego.append(inter01)
        point_list_ego.append(inter12)
        point_list_ego.append(inter23)
        point_list_ego.append(inter30)
    point_list_ego = np.array(point_list_ego)
    return distance.cdist(point_list_ego, other).min(axis=1).min(axis=0)


def min_distance_between_vehicles(ego_location, ego_property, other_location, other_property):
    ego_rotate = polygon_xy_from_motionstate(x = ego_location[0],
                                             y = ego_location[1],
                                             psi_rad = ego_location[2],
                                             width = ego_property["vehicle_width"],
                                             length = ego_property["vehicle_length"])

    other_rotate = polygon_xy_from_motionstate(x = other_location[0],
                                               y = other_location[1],
                                               psi_rad = other_location[2],
                                               width = other_property["vehicle_width"],
                                               length = other_property["vehicle_length"])

    minDist = cal_polymindist(ego_rotate, other_rotate)
    # cal_polymindist(ego_rotate,other_rotate)
    same_direction = True
    # 指示ego车辆的前进方向
    # if(abs(ego_location[2]) < math.pi / 2):
    ego_vector = ego_rotate[1] - ego_rotate[0]
    # 指示ego车辆的右侧方向
    ego_side_vector = ego_rotate[1] - ego_rotate[2]
    # else:
    #     ego_vector = ego_rotate[0] - ego_rotate[1]
    object_vector = other_rotate[1] - other_rotate[0]
    adjacent_vector = np.array([other_location[0] - ego_location[0], other_location[1] - ego_location[1]])
    Lx = np.sqrt(ego_vector.dot(ego_vector))
    Ly = np.sqrt(adjacent_vector.dot(adjacent_vector))
    Cobb = int((np.arccos(ego_vector.dot(adjacent_vector) / (float(Lx * Ly))) * 180 / np.pi))
    if(Cobb < 90):
        direction = "front"
        if(Cobb >= 5):
            if(ego_side_vector.dot(adjacent_vector) > 0):
                direction = direction + "_right"
            else:
                direction = direction + "_left"
    else:
        direction = "rear"
        if(180 - Cobb >= 5):
            if(ego_side_vector.dot(adjacent_vector) > 0):
                direction = direction + "_right"
            else:
                direction = direction + "_left"
    if int(ego_vector.dot(object_vector) < 0):
        same_direction = False
    return direction, minDist,same_direction


def min_distance_between_vehicles_no_shape(ego_location, other_location):
    dist = np.sqrt(pow(ego_location[0] - other_location[0], 2) + pow(ego_location[1] - other_location[1], 2))
    ego_vector = np.array([np.cos(ego_location[2]), np.sin(ego_location[2])])
    ego_side_vector = np.array([np.sin(ego_location[2]), -np.cos(ego_location[2])])
    object_vector = np.array([np.cos(other_location[2]), np.sin(other_location[2])])
    adjacent_vector = np.array([other_location[0] - ego_location[0], other_location[1] - ego_location[1]])
    same_direction = True
    Lx = np.sqrt(ego_vector.dot(ego_vector))
    Ly = np.sqrt(adjacent_vector.dot(adjacent_vector))
    Cobb = int((np.arccos(ego_vector.dot(adjacent_vector) / (float(Lx * Ly))) * 180 / np.pi))
    if (Cobb < 90):
        direction = "front"
        if (Cobb >= 10):
            if (ego_side_vector.dot(adjacent_vector) > 0):
                direction = direction + "_right"
            else:
                direction = direction + "_left"
    else:
        direction = "rear"
        if (180 - Cobb >= 10):
            if (ego_side_vector.dot(adjacent_vector) > 0):
                direction = direction + "_right"
            else:
                direction = direction + "_left"
    if int(ego_vector.dot(object_vector) < 0):
        same_direction = False
    return direction, dist, same_direction


def min_distance_direction_between_car_node(ego_location, ego_property, node_x, node_y):
    ego_rotate = polygon_xy_from_motionstate(x=ego_location[0],
                                             y=ego_location[1],
                                             psi_rad=ego_location[2],
                                             width=ego_property["vehicle_width"],
                                             length=ego_property["vehicle_length"])

    other = np.array([node_x, node_y])

    # 判断车道控制点是否被车覆盖
    # if(if_inPoly(ego_rotate, other)):
    #     return None, None
    other = other.reshape(1,-1)
    # minDist = cal_poly_point_mindist(ego_rotate, other)
    # 指示ego车辆的前进方向
    ego_vector = ego_rotate[1] - ego_rotate[0]
    # 指示ego车辆的右侧方向
    ego_side_vector = ego_rotate[1] - ego_rotate[2]
    adjacent_vector = np.array([node_x - ego_location[0], node_y - ego_location[1]])
    Lx = np.sqrt(ego_vector.dot(ego_vector))
    Ly = np.sqrt(adjacent_vector.dot(adjacent_vector))
    Cobb = int((np.arccos(ego_vector.dot(adjacent_vector) / (float(Lx * Ly))) * 180 / np.pi))
    if (Cobb < 90):
        direction = "front"
        if (Cobb > 3):
            if (ego_side_vector.dot(adjacent_vector) > 0):
                direction = direction + "_right"
            else:
                direction = direction + "_left"
    else:
        direction = "rear"
        if (180 - Cobb > 3):
            if (ego_side_vector.dot(adjacent_vector) > 0):
                direction = direction + "_right"
            else:
                direction = direction + "_left"

    Dist = Ly
    return direction, Dist


def min_distance_direction_between_car_node_no_shape(ego_location, node_x, node_y):
    dist = np.sqrt(pow(ego_location[0] - node_x, 2) + pow(ego_location[1] - node_y, 2))
    ego_vector = np.array([np.cos(ego_location[2]), np.sin(ego_location[2])])
    ego_side_vector = np.array([np.sin(ego_location[2]), -np.cos(ego_location[2])])
    adjacent_vector = np.array([node_x - ego_location[0], node_y - ego_location[1]])
    Lx = np.sqrt(ego_vector.dot(ego_vector))
    Ly = np.sqrt(adjacent_vector.dot(adjacent_vector))
    Cobb = int((np.arccos(ego_vector.dot(adjacent_vector) / (float(Lx * Ly))) * 180 / np.pi))
    if (Cobb < 90):
        direction = "front"
        if (Cobb > 3):
            if (ego_side_vector.dot(adjacent_vector) > 0):
                direction = direction + "_right"
            else:
                direction = direction + "_left"
    else:
        direction = "rear"
        if (180 - Cobb > 3):
            if (ego_side_vector.dot(adjacent_vector) > 0):
                direction = direction + "_right"
            else:
                direction = direction + "_left"

    return direction, dist


def SearchDBTest(cursor):
    # sql = "select local_x,local_y from Traffic_timing_state"
    # cursor.execute(sql)  # 执行sql
    # for i in cursor.fetchall():
    #     print(i)
    #     print(len(i))
    #     print(type(i[0]))
    # print('共查询到：', cursor.rowcount, '条数据')
    # deleteSql = "delete from Traffic_timing_state where vehicle_id = 4"
    # cursor.execute(deleteSql)
    # searchSql = 'select vehicle_length from Traffic_participant_property where vehicle_id in (select vehicle_id from Traffic_timing_state where data_id < 100)'
    # cursor.execute(searchSql)
    # for i in cursor.fetchall():
    #     print(i)
    sql = "select way_id from Way_Info where l_border_of=3"
    cursor.execute(sql)
    way_list = list()
    node_list = list()
    x_list = list()
    y_list = list()
    for i in cursor.fetchall():
        way_list.append(i[0])
    for way in way_list:
        _,temp_x,temp_y = SearchNodeIDOnWayFromDB(cursor, way)
        x_list = x_list + temp_x
        y_list = y_list + temp_y
    print(len(x_list))
    print(y_list)


def SearchTrafficParticipantByTime(cursor, timestamp, table):
    sql = "select vehicle_id from Traffic_timing_state" + table + " where time_stamp = %s" %(timestamp)
    cursor.execute(sql)
    vehicle_list = list()
    for i in cursor.fetchall():
        vehicle_list.append(i[0])
    # print(vehicle_list)
    return vehicle_list


def SearchTrafficParticipantTiming(cursor, vehicleid, timestamp, table):
    sql = "select local_x,local_y,orientation from Traffic_timing_state" + table + " where vehicle_id = %s and time_stamp = %s" %(vehicleid,timestamp)
    cursor.execute(sql)
    x = None
    y = None
    orien = None
    for i in cursor.fetchall():
        if (i[0] != None):
            x = float(i[0])
        else:
            x = i[0]
        if (i[1] != None):
            y = float(i[1])
        else:
            y = i[1]
        if (i[2] != None):
            orien = float(i[2])
        else:
            orien = 0
    # print(x,y,orien)
    return x, y,orien


def SearchEgoSameTimeVehicle(cursor, ego_vehicle, timestamp, table):
    vehicle_list = SearchTrafficParticipantByTime(cursor, timestamp, table)
    if ego_vehicle not in vehicle_list:
        vehicle_list = list()
    else:
        vehicle_list.remove(ego_vehicle)
    return vehicle_list


def SearchTrafficParticipantProperty(cursor, vehicleid, table):
    sql = "select vehicle_class,vehicle_length,vehicle_width from Traffic_participant_property" + table + " where vehicle_id = %s" % (vehicleid)
    cursor.execute(sql)
    vehicle_property = dict()
    for i in cursor.fetchall():
        vehicle_property["vehicle_class"] = i[0]
        if (i[1] != None):
            vehicle_property["vehicle_length"] = float(i[1])
        else:
            vehicle_property["vehicle_length"] = None
        if (i[2] != None):
            vehicle_property["vehicle_width"] = float(i[2])
        else:
            vehicle_property["vehicle_width"] = None
    # print(vehicle_property)
    return vehicle_property


def SearchMap(cursor):
    sql = """select node_id from Node_To_Way where way_id in (select way_id from Way_Info where way_type -> '$.type' = "line_thin")"""
    cursor.execute(sql)  # 执行sql
    for i in cursor.fetchall():
        print(i)
    print('共查询到：', cursor.rowcount, '条数据')


def SearchNodeIDOnWayFromDB(cursor, way_id, x_range=None, y_range=None):
    '''
    从数据库中查询特定道路上所有节点的位置信息
    :return: node_list, x_list, y_list
    '''
    if(x_range==None):
        # sql = "select local_x,local_y from Node_Info where node_id in (select node_id from Node_To_Way where way_id = %s)" %(way_id)
        sql = "select Node_Info.node_id, local_x, local_y " \
              "from Node_Info " \
              "join Node_To_Way on Node_Info.node_id = Node_To_Way.node_id " \
              "where way_id = %s"%(way_id)
    else:
        # sql = "select local_x,local_y from Node_Info where node_id in (select node_id from Node_To_Way where way_id = %s) and local_x BETWEEN %s and %s and local_y BETWEEN %s and %s" \
        #       %(way_id, x_range[0], x_range[1], y_range[0], y_range[1])
        sql = "select Node_Info.node_id, local_x, local_y " \
              "from Node_Info " \
              "join Node_To_Way on Node_Info.node_id = Node_To_Way.node_id " \
              "where way_id = %s and local_x BETWEEN %s and %s and local_y BETWEEN %s and %s" \
              % (way_id, x_range[0], x_range[1], y_range[0], y_range[1])
    cursor.execute(sql)
    node_list = list()
    x_list = list()
    y_list = list()
    for i in cursor.fetchall():
        # print(i)
        node_list.append(i[0])
        x_list.append(i[1])
        y_list.append(i[2])
    # print('共查询到：', cursor.rowcount, '条数据')
    # print(x_list, y_list)
    return node_list, x_list, y_list


def SearchLineThinDashedNodeFromDB(cursor):
    '''
    从数据库中查询所有交互的节点位置信息
    return: node_list, x_list, y_list
    '''

    # sql = '''select node_id,local_x,local_y from Node_Info where node_id in
    # (select distinct Node_info.node_id from (Node_Info join Node_To_Way on Node_Info.node_id = Node_To_Way.node_id)
    # join Way_Info on Node_To_Way.way_id = Way_info.way_id
    # where way_type -> '$.type' = "line_thin")'''
    sql = '''select Node_Info.node_id,local_x,local_y from (Node_Info join Node_To_Way on Node_Info.node_id = Node_To_Way.node_id)
        join Way_Info on Node_To_Way.way_id = Way_Info.way_id
        where way_type -> '$.type' = "line_thin" and way_type -> '$.subtype' = "dashed" group by Node_Info.node_id,local_x,local_y'''
    cursor.execute(sql)
    node_list = list()
    x_list = list()
    y_list = list()
    for i in cursor.fetchall():
        node_list.append(i[0])
        x_list.append(float(i[1]))
        y_list.append(float(i[2]))
    # print(len(node_list))
    # print(x_list)
    # print(y_list)
    # print('共查询到：', cursor.rowcount, '条数据')
    return node_list, x_list, y_list


def SearchVirtualNodeFromDB(cursor):
    sql = '''select Node_Info.node_id,local_x,local_y from (Node_Info join Node_To_Way on Node_Info.node_id = Node_To_Way.node_id)
        join Way_Info on Node_To_Way.way_id = Way_Info.way_id
        where way_type -> '$.type' = "virtual" group by Node_Info.node_id,local_x,local_y'''
    cursor.execute(sql)
    node_list = list()
    x_list = list()
    y_list = list()
    for i in cursor.fetchall():
        node_list.append(i[0])
        x_list.append(float(i[1]))
        y_list.append(float(i[2]))
    # print(len(node_list))
    # print(x_list)
    # print(y_list)
    # print('共查询到：', cursor.rowcount, '条数据')
    return node_list, x_list, y_list


def SearchNodeIDFromDB(cursor):
    '''
    从数据库中查询所有的节点位置信息
    :return: x_list, y_list
    '''
    sql = "select node_id,local_x,local_y from Node_Info"
    cursor.execute(sql)
    node_list = list()
    x_list = list()
    y_list = list()
    for i in cursor.fetchall():
        node_list.append(i[0])
        x_list.append(float(i[1]))
        y_list.append(float(i[2]))
    # print(node_list)
    # print(x_list)
    # print(y_list)
    # print('共查询到：', cursor.rowcount, '条数据')
    return node_list, x_list, y_list


def SearchLinkWayFromDB(cursor):
    sql = "select node_id from Node_To_Way"
    cursor.execute(sql)
    node_list = list()
    for i in cursor.fetchall():
        node_list.append(i[0])
    temp = {}
    res = {}
    for key in node_list:
        temp[key] = temp.get(key, 0) + 1
    for key,value in temp.items():
        if value > 1:
            res[key] = value
    print(res)
    connect_way = []
    for key,value in res.items():
        temp_connect = list()
        sql = "select way_id from Node_To_Way where node_id = %s"%(key)
        cursor.execute(sql)
        for i in cursor.fetchall():
            temp_connect.append(i[0])
        connect_way.append(temp_connect)
    print(connect_way)
    num = len(connect_way)
    for i in range(num):
        for j in range(num):
            x = list(set(connect_way[i] + connect_way[j]))
            y = len(connect_way[j]) + len(connect_way[i])
            if i == j or connect_way[i] == 0 or connect_way[j] == 0:
                break
            elif len(x) < y:
                connect_way[i] = x
                connect_way[j] = [0]
    print([i for i in connect_way if i != [0]])


def SearchNodeLocationFromDB(cursor, node_id):
    '''
    从数据库中查询指定节点位置信息
    :return: local_x, local_y
    '''

    sql = "select local_x,local_y from Node_Info where node_id = %s" %(node_id)
    cursor.execute(sql)
    local_x = None
    local_y = None
    for i in cursor.fetchall():
        local_x = float(i[0])
        local_y = float(i[1])
    # print('共查询到：', cursor.rowcount, '条数据')
    return local_x, local_y


def SearchOnWhichLaneFromDB(cursor, vehicle, timestamp, table):
    sql = "select lane_id from Traffic_timing_state" + table + " where vehicle_id = %s and time_stamp = %s" % (vehicle, timestamp)
    cursor.execute(sql)
    laneID = None
    for i in cursor.fetchall():
        laneID = i[0]
    # print(laneID)
    return laneID


def SearchAdjacentLaneFromDB(cursor, lane_id):
    sql = "select l_adj_lane,r_adj_lane from Lane_Meta where lane_id = '%s'" % (lane_id)
    cursor.execute(sql)
    l_adj_lane = None
    r_adj_lane = None
    for i in cursor.fetchall():
        l_adj_lane = i[0]
        r_adj_lane = i[1]
    # print(l_adj_lane, r_adj_lane)
    return l_adj_lane,r_adj_lane


def SearchFrontAdjacentLaneFromDB(cursor, way_id):
    sql = "select predecessor from Way_Info where way_id = %s" % (way_id)
    cursor.execute(sql)
    predecessor = cursor.fetchall()[0][0]
    l_adj_lane = None
    r_adj_lane = None
    if predecessor:
        adjsql = "select l_adj_lane,r_adj_lane from Lane_Meta where lane_id = '%s'" % (str(predecessor))
        cursor.execute(adjsql)
        for i in cursor.fetchall():
            l_adj_lane = i[0]
            r_adj_lane = i[1]
    # print(l_adj_lane, r_adj_lane)
    return l_adj_lane,r_adj_lane


def SearchLocationRangeFromDB(cursor, table):
    '''
    从数据库中查询节点位置的边界信息
    :return: local_x, local_y
    '''

    sql = "select min(local_x),max(local_x),min(local_y),max(local_y) from Traffic_timing_state" + table
    cursor.execute(sql)
    min_local_x = None
    max_local_x = None
    min_local_y = None
    max_local_y = None
    for i in cursor.fetchall():
        # print(i)
        min_local_x = float(i[0])
        max_local_x = float(i[1])
        min_local_y = float(i[2])
        max_local_y = float(i[3])
    # print('共查询到：', cursor.rowcount, '条数据')
    return min_local_x, max_local_x, min_local_y, max_local_y


def SearchVehicleLocationRangeFromDB(cursor, table, vehicle):
    sql = "select min(local_x),max(local_x),min(local_y),max(local_y) from Traffic_timing_state" + table + " where vehicle_id = %s"%(vehicle)
    cursor.execute(sql)
    min_local_x = None
    max_local_x = None
    min_local_y = None
    max_local_y = None
    for i in cursor.fetchall():
        # print(i)
        min_local_x = float(i[0])
        max_local_x = float(i[1])
        min_local_y = float(i[2])
        max_local_y = float(i[3])
    # print('共查询到：', cursor.rowcount, '条数据')
    return min_local_x, max_local_x, min_local_y, max_local_y


def SearchWayTypeFromDB(cursor, way_id):
    sql = """select way_type ->> '$.type',way_type ->> '$.subtype' from Way_Info where way_id = %s"""%(way_id)
    cursor.execute(sql)
    way_type = None
    way_subtype = None
    for i in cursor.fetchall():
        way_type = i[0]
        way_subtype = i[1]
    # print(way_type, way_subtype)
    return way_type, way_subtype


def SearchWayFromDB(cursor):
    '''
    从数据库中查询所有的道路信息
    :return: x_list, y_list
    '''

    sql = "select way_id from Way_Info"
    cursor.execute(sql)
    way_list = list()
    for i in cursor.fetchall():
        way_list.append(i[0])
    # print(way_list)
    # print('共查询到：', cursor.rowcount, '条数据')
    return way_list


def Merge(dict1, dict2):
    res = {**dict1, **dict2}
    return res


def SearchChannelizationOnWayFromDB(cursor, way_id):
    sql = """select road_channelization from Way_Info where way_id = %s""" % (way_id)
    cursor.execute(sql)
    Channelization = dict()
    for i in cursor.fetchall():
        for j in range(len(i)):
            dic = json.loads(i[j])
            Channelization = Merge(Channelization, dic)
    # print(Channelization)
    return Channelization


def SearchVehicleTimeRangeFromDB(cursor, vehicle_id, table):
    '''
    从数据库中查询节点位置的边界信息
    :return: local_x, local_y
    '''

    sql = "select min(time_stamp),max(time_stamp) from Traffic_timing_state" + table + " where vehicle_id = %s"%(vehicle_id)
    cursor.execute(sql)
    min_timestamp = None
    max_timestamp = None
    for i in cursor.fetchall():
        # print(i)
        min_timestamp = int(i[0])
        max_timestamp = int(i[1])
    # print('共查询到：', cursor.rowcount, '条数据')
    return min_timestamp, max_timestamp


def SearchAllVehicleIDFromDB(cursor, table):
    sql = "select vehicle_id from Traffic_participant_property" + table
    cursor.execute(sql)
    vehicle_list = list()
    for i in cursor.fetchall():
        # print(i)
        vehicle_list.append(i[0])
    # print('共查询到：', cursor.rowcount, '条数据')
    return vehicle_list


def SearchVelocityOnTime(cursor, vehicle_id, timestamp, table):
    sql = "select velocity_x,velocity_y from Traffic_timing_state" + table + " where vehicle_id = %s and time_stamp = %s"%(vehicle_id,timestamp)
    cursor.execute(sql)
    velocity_x = None
    velocity_y = None
    velocity = None
    for i in cursor.fetchall():
        velocity_x = i[0]
        velocity_y = i[1]
        if velocity_x!=None and velocity_y!=None:
            velocity = np.sqrt(velocity_x * velocity_x + velocity_y * velocity_y)
    # print('共查询到：', cursor.rowcount, '条数据')
    # print(velocity_x, velocity_y, velocity)
    return velocity_x, velocity_y, velocity


def SearchOrientationOnTime(cursor, vehicle_id, timestamp, table):
    sql = "select orientation from Traffic_timing_state" + table + " where vehicle_id = %s and time_stamp = %s" % (vehicle_id, timestamp)
    cursor.execute(sql)
    orientation = None
    for i in cursor.fetchall():
        orientation = i[0]
    # print(velocity_x, velocity_y, velocity)
    return orientation


def SearchVehicleTotalTime(cursor, vehicle_id, table):
    sql = "select max(time_stamp),min(time_stamp) from Traffic_timing_state" + table + " where vehicle_id = %s" %(vehicle_id)
    max_time = 0
    min_time = 0
    cursor.execute(sql)
    for i in cursor.fetchall():
        # print(i)
        max_time = i[0]
        min_time = i[1]
    # print('共查询到：', cursor.rowcount, '条数据')
    # print(max_time, min_time)
    return max_time - min_time


if __name__ == '__main__':
    cursor = init_DB("Argoverse_Scenario_DB")
    channel = SearchChannelizationOnWayFromDB(cursor, 9605253)
    print(channel.get('turn_direction')==None)
    # SearchDB(cursor)
    # SearchNodeIDOnWayFromDB(cursor, 10000, x_range=[1030,1032],y_range=[960,962])
    # SearchTrafficParticipantByTime(cursor, 9000)
    # SearchTrafficParticipantTiming(cursor, 41, 2600)
    # SearchTrafficParticipantProperty(cursor,1)
    # SearchMap(cursor)
    # SearchNodeIDFromDB(cursor)
    # SearchLinkWayFromDB(cursor)
    # SearchNodeIDOnWayFromDB(cursor, 10001)
    # SearchWayTypeFromDB(cursor, 10001)
    # SearchChannelizationOnWayFromDB(cursor, 10000)
    # SearchWayFromDB(cursor)
    # SearchLineThinDashedNodeFromDB(cursor)
    # SearchNodeIDFromDB(cursor)
    # SearchOnWhichLaneFromDB(cursor, 261, 230640)
    # SearchAdjacentLaneFromDB(cursor, 3)
    # SearchAdjacentLaneFromDB(cursor, "2")
    # SearchFrontAdjacentLaneFromDB(cursor, 9605252)
    cursor.close()