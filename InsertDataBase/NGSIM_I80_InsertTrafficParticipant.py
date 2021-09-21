import pymysql
import math
import pandas as pd
import numpy as np

conn = pymysql.connect(
    host='localhost',
    user="root",
    passwd="123456",
    db="NGSIM_I80_Scenario_DB")
# 获取游标
cursor = conn.cursor()

table = "_3"
file = "../DataSet/NGSIM-Dataset/I-80-trajectory-data/trajectories-0515-0530.csv"
AllVehicleInfo = pd.read_csv(file, decimal=",", low_memory=False)
AllVehicleInfo = np.array(AllVehicleInfo)
VehicleInfo = np.hstack((AllVehicleInfo[:, 0:1].astype(np.int), AllVehicleInfo[:, 3:4].astype(np.int64), AllVehicleInfo[:, 4:6].astype(np.float), AllVehicleInfo[:, 11:13].astype(np.float), AllVehicleInfo[:, 13:16].astype(np.int)))
print(VehicleInfo[0])
VehicleMeta = np.hstack((AllVehicleInfo[:, 0:1].astype(np.int), AllVehicleInfo[:, 8:10].astype(np.float), AllVehicleInfo[:, 10:11].astype(np.int)))
print(VehicleMeta[0])
y_sign = -1
max_vehicle_num = 1757


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
    insertTimingSql = "insert into Traffic_timing_state" + table + "(time_stamp,vehicle_id,local_x,local_y,velocity_x,acceleration,orientation,lane_id,preced_vehicle,follow_vehicle) " \
                "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    for i in range(len(VehicleInfo)):
        # NGSIM数据集01采样频率10Hz
        time_stamp = int(VehicleInfo[i][1])
        # print(type(time_stamp))
        vehicle_id = VehicleInfo[i][0]
        # print(type(vehicle_id))
        local_x = VehicleInfo[i][3]
        # 将NGSIM坐标系是正常的沿x轴翻转
        local_y = y_sign * VehicleInfo[i][2]
        velocity_x = round(VehicleInfo[i][4], 3)
        acceleration = round(VehicleInfo[i][5], 3)
        orientation = 0.0
        if (VehicleInfo[i][7] == 0 or VehicleInfo[i][7] > max_vehicle_num):
            preced_vehicle = None
        else:
            preced_vehicle = int(VehicleInfo[i][7])
        if (VehicleInfo[i][8] == 0 or VehicleInfo[i][8] > max_vehicle_num):
            follow_vehicle = None
        else:
            follow_vehicle = int(VehicleInfo[i][8])
        lane_id = str(int(VehicleInfo[i][6]))
        cursor.execute(insertTimingSql,(time_stamp,vehicle_id,local_x,local_y,velocity_x,acceleration,orientation,lane_id,preced_vehicle,follow_vehicle))
        print(i)

    insertPropertySql = "insert into Traffic_participant_property" + table + "(vehicle_id,vehicle_class,vehicle_length,vehicle_width) " \
                "values(%s,%s,%s,%s) ON DUPLICATE KEY UPDATE vehicle_class = vehicle_class,vehicle_length = vehicle_length,vehicle_width = vehicle_width"
    for i in range(len(VehicleMeta)):
        # print(type(time_stamp))
        vehicle_id = VehicleMeta[i][0]
        # 1 - 摩托车, 2 - 汽车, 3 - 卡车
        vehicle_type = int(VehicleMeta[i][3])
        # print(type(vehicle_type))
        # if(vehicle_type == "Car"):
        #     vehicle_class = 1
        # elif(vehicle_type == "Truck"):
        #     vehicle_class = 2
        # else:
        #     vehicle_class = 0
        vehicle_length = VehicleMeta[i][1]
        vehicle_width = VehicleMeta[i][2]
        cursor.execute(insertPropertySql,(vehicle_id,vehicle_type,vehicle_length,vehicle_width))
        print(i)


if __name__ == '__main__':
    CreateTrafficParticipantPropertyTable(table)
    CreateTrafficTimingStateTable(table)
    InsertTable(table)
    # sql = "SELECT preced_vehicle FROM Traffic_timing_state WHERE preced_vehicle NOT IN (SELECT vehicle_id FROM Traffic_participant_property)"
    # cursor.execute(sql)
    # print(cursor.fetchall())

    cursor.close()
    conn.commit()
    conn.close()