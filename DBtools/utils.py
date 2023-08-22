import json
import numpy as np
from scipy.spatial import distance
from shapely import geometry


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
    same_direction = True
    # indicate ego vehicle forward direction
    ego_vector = ego_rotate[1] - ego_rotate[0]
    # indicate ego vehicle right direction
    ego_side_vector = ego_rotate[1] - ego_rotate[2]
    object_vector = other_rotate[1] - other_rotate[0]
    adjacent_vector = np.array([other_location[0] - ego_location[0], other_location[1] - ego_location[1]])
    Lx = np.sqrt(ego_vector.dot(ego_vector))
    Ly = np.sqrt(adjacent_vector.dot(adjacent_vector))
    if ego_vector.dot(adjacent_vector) / (float(Lx * Ly)) > 1:
        Cobb = 0
    elif ego_vector.dot(adjacent_vector) / (float(Lx * Ly)) < -1:
        Cobb = 180
    else:
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
    return direction, minDist, same_direction


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


    other = other.reshape(1,-1)
    ego_vector = ego_rotate[1] - ego_rotate[0]
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


def SearchTrafficParticipantByTime(cursor, timestamp, table):
    sql = "select vehicle_id from Traffic_timing_state" + table + " where time_stamp = %s" %(timestamp)
    cursor.execute(sql)
    vehicle_list = list()
    for i in cursor.fetchall():
        vehicle_list.append(i[0])
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
    return vehicle_property


def SearchMap(cursor):
    sql = """select node_id from Node_To_Way where way_id in (select way_id from Way_Info where way_type -> '$.type' = "line_thin")"""
    cursor.execute(sql)
    for i in cursor.fetchall():
        print(i)
    print(cursor.rowcount)


def SearchNodeIDOnWayFromDB(cursor, way_id, x_range=None, y_range=None):
    '''
     Query the location information of all nodes on a specific way from the database
    :return: node_list, x_list, y_list
    '''
    if(x_range==None):
        sql = "select Node_Info.node_id, local_x, local_y " \
              "from Node_Info " \
              "join Node_To_Way on Node_Info.node_id = Node_To_Way.node_id " \
              "where way_id = %s"%(way_id)
    else:
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
        node_list.append(i[0])
        x_list.append(i[1])
        y_list.append(i[2])
    return node_list, x_list, y_list


def SearchLineThinDashedNodeFromDB(cursor):
    '''
    Query the possible interaction information of all nodes from the database
    return: node_list, x_list, y_list
    '''
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
    return node_list, x_list, y_list


def SearchNodeIDFromDB(cursor):
    '''
     Query the information of all nodes from the database
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
     Query the location information of specific nodes from the database
    :return: local_x, local_y
    '''

    sql = "select local_x,local_y from Node_Info where node_id = %s" %(node_id)
    cursor.execute(sql)
    local_x = None
    local_y = None
    for i in cursor.fetchall():
        local_x = float(i[0])
        local_y = float(i[1])
    return local_x, local_y


def SearchOnWhichLaneFromDB(cursor, vehicle, timestamp, table):
    sql = "select lane_id from Traffic_timing_state" + table + " where vehicle_id = %s and time_stamp = %s" % (vehicle, timestamp)
    cursor.execute(sql)
    laneID = None
    for i in cursor.fetchall():
        laneID = i[0]
    return laneID


def SearchAdjacentLaneFromDB(cursor, lane_id):
    sql = "select l_adj_lane,r_adj_lane from Lane_Meta where lane_id = '%s'" % (lane_id)
    cursor.execute(sql)
    l_adj_lane = None
    r_adj_lane = None
    for i in cursor.fetchall():
        l_adj_lane = i[0]
        r_adj_lane = i[1]
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
    return l_adj_lane,r_adj_lane


def SearchLocationRangeFromDB(cursor, table):
    '''
     Query the location information of all nodes boundary from the database
    :return: local_x, local_y
    '''

    sql = "select min(local_x),max(local_x),min(local_y),max(local_y) from Traffic_timing_state" + table
    cursor.execute(sql)
    min_local_x = None
    max_local_x = None
    min_local_y = None
    max_local_y = None
    for i in cursor.fetchall():
        min_local_x = float(i[0])
        max_local_x = float(i[1])
        min_local_y = float(i[2])
        max_local_y = float(i[3])
    return min_local_x, max_local_x, min_local_y, max_local_y


def SearchVehicleLocationRangeFromDB(cursor, table, vehicle):
    sql = "select min(local_x),max(local_x),min(local_y),max(local_y) from Traffic_timing_state" + table + " where vehicle_id = %s"%(vehicle)
    cursor.execute(sql)
    min_local_x = None
    max_local_x = None
    min_local_y = None
    max_local_y = None
    for i in cursor.fetchall():
        min_local_x = float(i[0])
        max_local_x = float(i[1])
        min_local_y = float(i[2])
        max_local_y = float(i[3])
    return min_local_x, max_local_x, min_local_y, max_local_y


def SearchWayTypeFromDB(cursor, way_id):
    sql = """select way_type ->> '$.type',way_type ->> '$.subtype' from Way_Info where way_id = %s"""%(way_id)
    cursor.execute(sql)
    way_type = None
    way_subtype = None
    for i in cursor.fetchall():
        way_type = i[0]
        way_subtype = i[1]
    return way_type, way_subtype


def SearchWayFromDB(cursor):
    '''
     Query the information of ways from the database
    :return: x_list, y_list
    '''

    sql = "select way_id from Way_Info"
    cursor.execute(sql)
    way_list = list()
    for i in cursor.fetchall():
        way_list.append(i[0])
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
    return Channelization


def SearchVehicleTimeRangeFromDB(cursor, vehicle_id, table):
    '''
     Query the time range information of all vehicles from the database
    :return: local_x, local_y
    '''

    sql = "select min(time_stamp),max(time_stamp) from Traffic_timing_state" + table + " where vehicle_id = %s"%(vehicle_id)
    cursor.execute(sql)
    min_timestamp = None
    max_timestamp = None
    for i in cursor.fetchall():
        min_timestamp = int(i[0])
        max_timestamp = int(i[1])
    return min_timestamp, max_timestamp


def SearchAllVehicleIDFromDB(cursor, table):
    sql = "select vehicle_id from Traffic_participant_property" + table
    cursor.execute(sql)
    vehicle_list = list()
    for i in cursor.fetchall():
        vehicle_list.append(i[0])
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
    return velocity_x, velocity_y, velocity


def SearchOrientationOnTime(cursor, vehicle_id, timestamp, table):
    sql = "select orientation from Traffic_timing_state" + table + " where vehicle_id = %s and time_stamp = %s" % (vehicle_id, timestamp)
    cursor.execute(sql)
    orientation = None
    for i in cursor.fetchall():
        orientation = i[0]
    return orientation


def SearchVehicleTotalTime(cursor, vehicle_id, table):
    sql = "select max(time_stamp),min(time_stamp) from Traffic_timing_state" + table + " where vehicle_id = %s" %(vehicle_id)
    max_time = 0
    min_time = 0
    cursor.execute(sql)
    for i in cursor.fetchall():
        max_time = i[0]
        min_time = i[1]
    return max_time - min_time