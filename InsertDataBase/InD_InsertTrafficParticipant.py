import pymysql
import math
import pandas as pd
import numpy as np
from sympy import im
from InsertDataBase.CreateTables import *
from DBtools.init_db import init_DB

VehicleInfo = list()
table = "_1"
file = "../DataSet/inD-dataset/data/07_tracks.csv"
VehicleInfo = pd.read_csv(file, decimal=",", low_memory=False)
VehicleInfo = np.array(VehicleInfo)
VehicleInfo = np.hstack((VehicleInfo[:, 1:3].astype(np.int), VehicleInfo[:, 4:13].astype(np.float)))
meta_file = "../DataSet/inD-dataset/data/07_tracksMeta.csv"
VehicleMeta = pd.read_csv(meta_file, decimal=",", low_memory=False)
VehicleMeta = np.array(VehicleMeta)
VehicleMeta = np.hstack((VehicleMeta[:, 1:2].astype(np.int), VehicleMeta[:, 5:8]))
print(VehicleMeta[0])


def InsertTable(cursor, table):
    insertTimingSql = "insert into Traffic_timing_state" + table + "(time_stamp,vehicle_id,local_x,local_y,velocity_x,velocity_y,acceleration,orientation) " \
                "values(%s,%s,%s,%s,%s,%s,%s,%s)"
    for i in range(len(VehicleInfo)):
        if VehicleInfo[i][5] == 0 and VehicleInfo[i][6] == 0:
            continue
        # InD record frequency 25Hz
        time_stamp = int(VehicleInfo[i][1] * 40)
        vehicle_id = VehicleInfo[i][0]
        local_x = VehicleInfo[i][2]
        local_y = VehicleInfo[i][3]
        velocity_x = round(VehicleInfo[i][7], 3)
        velocity_y = round(VehicleInfo[i][8], 3)
        acceleration = round(math.sqrt(VehicleInfo[i][9] * VehicleInfo[i][9] + VehicleInfo[i][10] * VehicleInfo[i][10]), 3)
        if VehicleInfo[i][4] < 180:
            orientation = VehicleInfo[i][4] / 180 * math.pi
        else:
            orientation = (VehicleInfo[i][4] - 360) / 180 * math.pi
        cursor.execute(insertTimingSql, (time_stamp, vehicle_id, local_x, local_y, velocity_x, velocity_y, acceleration, orientation))
        print(i)

    insertPropertySql = "insert into Traffic_participant_property" + table + "(vehicle_id,vehicle_class,vehicle_length,vehicle_width) " \
                "values(%s,%s,%s,%s) ON DUPLICATE KEY UPDATE vehicle_class = vehicle_class,vehicle_length = vehicle_length,vehicle_width = vehicle_width"
    for i in range(len(VehicleMeta)):
        vehicle_id = VehicleMeta[i][0]
        vehicle_type = VehicleMeta[i][3]
        if(vehicle_type == "car"):
            vehicle_class = 1
        elif(vehicle_type == "truck_bus"):
            vehicle_class = 2
        else:
            continue
        vehicle_length = VehicleMeta[i][2]
        vehicle_width = VehicleMeta[i][1]
        cursor.execute(insertPropertySql,(vehicle_id, vehicle_class, vehicle_length, vehicle_width))
        print(i)


if __name__ == '__main__':
    conn, cursor = init_DB("InD_I_Scenario_DB")
    CreateTrafficParticipantPropertyTable(cursor, table)
    CreateTrafficTimingStateTable(cursor, table)
    InsertTable(cursor, table)

    cursor.close()
    conn.commit()
    conn.close()