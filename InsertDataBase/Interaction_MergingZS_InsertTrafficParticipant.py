import pymysql
import math
import pandas as pd
import numpy as np
from InsertDataBase.Interaction_MergingZS_InsertMap import LaneCurve

conn = pymysql.connect(
    host='localhost',
    user="root",
    passwd="123456",
    db="Interaction_MergingZS_Scenario_DB")
# 获取游标
cursor = conn.cursor()

VehicleInfo = list()
table = "_10"
file = "../DataSet/INTERACTION-Dataset-DR-v1_1/recorded_trackfiles/DR_CHN_Merging_ZS/vehicle_tracks_010.csv"
VehicleInfo = pd.read_csv(file, decimal=",", low_memory=False)
VehicleInfo = np.array(VehicleInfo)
VehicleInfo = np.hstack((VehicleInfo[:, 0:4], VehicleInfo[:, 4:].astype(np.float)))
print(type(VehicleInfo))


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
        for j in range(len(LaneCurve)):
            l2 = LaneCurve[j][0][0]
            l1 = LaneCurve[j][0][1]
            l0 = LaneCurve[j][0][2]
            r2 = LaneCurve[j][1][0]
            r1 = LaneCurve[j][1][1]
            r0 = LaneCurve[j][1][2]
            pred_l = l2 * local_x * local_x + l1 * local_x + l0
            pred_r = r2 * local_x * local_x + r1 * local_x + r0
            if pred_l <= local_y <= pred_r or pred_r <= local_y <= pred_l:
                lane_id = str(j + 1)
                break
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