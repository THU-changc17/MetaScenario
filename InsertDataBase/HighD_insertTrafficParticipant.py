import pymysql
import math
import pandas as pd
import numpy as np
from InsertDataBase.CreateTables import *

conn = pymysql.connect(
    host='localhost',
    user="root",
    passwd="123456",
    db="HighD_I_Scenario_DB")
cursor = conn.cursor()

VehicleInfo = list()
table = "_3"
file = "../DataSet/HighD-Dataset/03_tracks.csv"
VehicleInfo = pd.read_csv(file, decimal=",", low_memory=False)
VehicleInfo = np.array(VehicleInfo)
VehicleInfo = np.hstack((VehicleInfo[:, 0:2].astype(np.int), VehicleInfo[:, 2:10].astype(np.float), VehicleInfo[:, 16:18].astype(np.int),VehicleInfo[:, 24:25].astype(np.int)))
print(VehicleInfo[0])
meta_file = "../DataSet/HighD-Dataset/03_tracksMeta.csv"
VehicleMeta = pd.read_csv(meta_file, decimal=",", low_memory=False)
VehicleMeta = np.array(VehicleMeta)
VehicleMeta = np.hstack((VehicleMeta[:, 0:1].astype(np.int), VehicleMeta[:, 1:3].astype(np.float), VehicleMeta[:, 6:7]))
print(VehicleMeta[0])
y_sign = -1


def InsertTable(table):
    insertTimingSql = "insert into Traffic_timing_state" + table + "(time_stamp,vehicle_id,local_x,local_y,velocity_x,velocity_y,acceleration,orientation,lane_id,preced_vehicle,follow_vehicle) " \
                "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    for i in range(len(VehicleInfo)):
        # HighD record frequency 25Hz
        time_stamp = int(VehicleInfo[i][0] * 40)
        vehicle_id = VehicleInfo[i][1]
        local_x = VehicleInfo[i][2]
        # HighD Coordinate system reverse x-axis
        local_y = y_sign * VehicleInfo[i][3]
        velocity_x = round(VehicleInfo[i][6], 3)
        velocity_y = -1 * round(VehicleInfo[i][7], 3)
        acceleration = round(math.sqrt(VehicleInfo[i][8] * VehicleInfo[i][8] + VehicleInfo[i][9] * VehicleInfo[i][9]), 3)
        if (VehicleInfo[i][6] > 0):
            orientation = 0.0
        else:
            orientation = 3.14
        if (VehicleInfo[i][10] == 0):
            preced_vehicle = None
        else:
            preced_vehicle = VehicleInfo[i][10]
        if (VehicleInfo[i][11] == 0):
            follow_vehicle = None
        else:
            follow_vehicle = VehicleInfo[i][11]
        lane_id = int(VehicleInfo[i][12])
        cursor.execute(insertTimingSql,(time_stamp,vehicle_id,local_x,local_y,velocity_x,velocity_y,acceleration,orientation,lane_id,preced_vehicle,follow_vehicle))
        print(i)

    insertPropertySql = "insert into Traffic_participant_property" + table + "(vehicle_id,vehicle_class,vehicle_length,vehicle_width) " \
                "values(%s,%s,%s,%s) ON DUPLICATE KEY UPDATE vehicle_class = vehicle_class,vehicle_length = vehicle_length,vehicle_width = vehicle_width"
    for i in range(len(VehicleMeta)):
        vehicle_id = VehicleMeta[i][0]
        vehicle_type = VehicleMeta[i][3]
        if(vehicle_type == "Car"):
            vehicle_class = 1
        elif(vehicle_type == "Truck"):
            vehicle_class = 2
        else:
            vehicle_class = 0
        vehicle_length = VehicleMeta[i][1]
        vehicle_width = VehicleMeta[i][2]
        cursor.execute(insertPropertySql,(vehicle_id,vehicle_class,vehicle_length,vehicle_width))
        print(i)


if __name__ == '__main__':
    CreateTrafficParticipantPropertyTable(cursor, table)
    CreateTrafficTimingStateTable(cursor, table)
    InsertTable(table)

    cursor.close()
    conn.commit()
    conn.close()