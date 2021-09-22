import pymysql
import math
import pandas as pd
import numpy as np
from scipy.spatial import distance
import csv


conn = pymysql.connect(
    host='localhost',
    user="root",
    passwd="123456",
    db="Argoverse_MIA_Scenario_DB")
cursor = conn.cursor()


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


def GetMinDistanceLane(local_x, local_y):
    radius = 5
    node_list = list()
    way_list = list()
    while len(node_list) < 3:
        node_list = list()
        way_list = list()
        min_x = local_x - radius
        max_x = local_x + radius
        min_y = local_y - radius
        max_y = local_y + radius
        sql = "select Node_To_Way.node_id, way_id " \
              "from Node_To_Way " \
              "join Node_Info on Node_To_Way.node_id = Node_Info.node_id " \
              "where local_x BETWEEN %s and %s and local_y BETWEEN %s and %s group by Node_To_Way.node_id,way_id" \
              % (min_x, max_x, min_y, max_y)
        cursor.execute(sql)
        node_list = list()
        for i in cursor.fetchall():
            node_list.append(i[0])
            way_list.append(i[1])
        radius = 2 * radius
    
    way_list = list(set(way_list))
    vehicle_position_list = np.array([[local_x, local_y]])
    dist = np.inf
    lane = None
    for way_id in way_list:
        temp_position_list = list()
        sql = "select Node_Info.node_id, local_x, local_y " \
              "from Node_Info " \
              "join Node_To_Way on Node_Info.node_id = Node_To_Way.node_id " \
              "where way_id = %s" % (way_id)
        cursor.execute(sql)
        for i in cursor.fetchall():
            temp_position_list.append([float(i[1]), float(i[2])])
        temp_position_list = np.array(temp_position_list)
        new_dist = distance.cdist(vehicle_position_list, temp_position_list).min(axis=1).min(axis=0)
        if new_dist < dist:
            dist = new_dist
            lane = str(way_id)
    return lane


def InsertTable(VehicleInfo, table):
    insertTimingSql = "insert into Traffic_timing_state" + table + "(time_stamp,vehicle_id,local_x,local_y,orientation,lane_id) " \
                "values(%s,%s,%s,%s,%s,%s)"
    for i in range(len(VehicleInfo)):
        time_stamp = int(float(VehicleInfo[i][0]) * 1e+3)
        id_str = VehicleInfo[i][1].split('-')
        vehicle_id = int(id_str[-1])
        local_x = float(VehicleInfo[i][3])
        local_y = float(VehicleInfo[i][4])
        lane = GetMinDistanceLane(local_x, local_y)
        orien = 0.000
        cursor.execute(insertTimingSql,(time_stamp, vehicle_id, local_x, local_y, orien, lane))
        if (i % 100 == 0):
            print(i)

    insertPropertySql = "insert into Traffic_participant_property" + table + "(vehicle_id,vehicle_class) " \
                "values(%s,%s) ON DUPLICATE KEY UPDATE vehicle_class = vehicle_class"
    for i in range(len(VehicleInfo)):
        vehicle_type = VehicleInfo[i][2]
        id_str = VehicleInfo[i][1].split('-')
        vehicle_id = int(id_str[-1])
        vehicle_class = int(1)
        cursor.execute(insertPropertySql,(vehicle_id, vehicle_class))
        if (i % 100 == 0):
            print(i)

    UpdateParticipantSql = "Update Traffic_timing_state" + table + " set orientation = %s where time_stamp = %s and vehicle_id = %s"
    stampsql = "select time_stamp from Traffic_timing_state" + table
    vehiclesql = "select vehicle_id from Traffic_timing_state" + table + " where time_stamp = %s"
    positionsql = "select local_x, local_y from Traffic_timing_state" + table + " where time_stamp = %s and vehicle_id = %s"
    oriensql = "select orientation from Traffic_timing_state" + table + " where time_stamp = %s and vehicle_id = %s"
    cursor.execute(stampsql)
    timestampresult = cursor.fetchall()
    timestamp_set = set()
    for i in range(len(timestampresult)):
        timestamp_set.add(timestampresult[i][0])
    timestamp_list = list(timestamp_set)
    timestamp_list.sort()
    vehicle_zero_orien_list = list()
    for t in range(len(timestamp_list) - 1):
        vehicle_list = list()
        cursor.execute(vehiclesql, timestamp_list[t + 1])
        for vehicle in cursor.fetchall():
            vehicle_list.append(vehicle[0])
        if t == 0:
            cursor.execute(vehiclesql, timestamp_list[t])
            for vehicle_zero in cursor.fetchall():
                vehicle_zero_orien_list.append(vehicle_zero[0])
        for vehicle in vehicle_list:
            local_x = 0
            local_y = 0
            last_x = 0
            last_y = 0
            cursor.execute(positionsql, (timestamp_list[t + 1], vehicle))
            for i in cursor.fetchall():
                local_x = i[0]
                local_y = i[1]
            cursor.execute(positionsql, (timestamp_list[t], vehicle))
            for j in cursor.fetchall():
                last_x = j[0]
                last_y = j[1]
            if local_x != None and local_y != None and last_x != None and last_y != None:
                if last_x == 0 and last_y == 0:
                    vehicle_zero_orien_list.append(vehicle)
                    continue
                direction = [local_x - last_x, local_y - last_y]
                orien = math.atan2(direction[1], direction[0])
                if local_x == last_x and local_y == last_y:
                    cursor.execute(oriensql, (timestamp_list[t], vehicle))
                    last_orien = cursor.fetchall()[0][0]
                    cursor.execute(UpdateParticipantSql, (last_orien, timestamp_list[t + 1], vehicle))
                else:
                    cursor.execute(UpdateParticipantSql, (orien, timestamp_list[t + 1], vehicle))
                if vehicle in vehicle_zero_orien_list:
                    cursor.execute(UpdateParticipantSql, (orien, timestamp_list[t], vehicle))
                    vehicle_zero_orien_list.remove(vehicle)


if __name__ == '__main__':
    csv_reader = csv.reader(open("../Annotator/sample_record.csv", encoding='utf-8'))
    for i, rows in enumerate(csv_reader):
        table = str(rows[0])
        print("table: ", table)
        VehicleInfo = list()
        file = "../../argoverse_trajectory/forecasting_train_v1.1/train/data/" + table + ".csv"
        VehicleInfo = pd.read_csv(file, decimal=",", low_memory=False)
        VehicleInfo = np.array(VehicleInfo)
        table = "_" + table
        CreateTrafficParticipantPropertyTable(table)
        CreateTrafficTimingStateTable(table)
        InsertTable(VehicleInfo, table)

    cursor.close()
    conn.commit()
    conn.close()