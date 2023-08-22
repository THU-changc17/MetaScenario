import sys
sys.path.append("../")
import pymysql
import math
import pandas as pd
import numpy as np
from InsertDataBase.CreateTables import *
from DBtools.init_db import init_DB
import argparse

def InsertTable(cursor, table, file):
    AllVehicleInfo = pd.read_csv(file, decimal=",", low_memory=False)
    AllVehicleInfo = np.array(AllVehicleInfo)
    VehicleInfo = np.hstack((AllVehicleInfo[:, 0:1].astype(np.int), AllVehicleInfo[:, 3:4].astype(np.int64),
                             AllVehicleInfo[:, 4:6].astype(np.float), AllVehicleInfo[:, 11:13].astype(np.float),
                             AllVehicleInfo[:, 13:16].astype(np.int)))
    VehicleMeta = np.hstack((AllVehicleInfo[:, 0:1].astype(np.int), AllVehicleInfo[:, 8:10].astype(np.float),
                             AllVehicleInfo[:, 10:11].astype(np.int)))
    y_sign = -1
    max_vehicle_num = 1757  # 2911, 2077
    insertTimingSql = "insert into Traffic_timing_state" + table + "(time_stamp,vehicle_id,local_x,local_y,velocity_x,acceleration,orientation,lane_id,preced_vehicle,follow_vehicle) " \
                      "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    for i in range(len(VehicleInfo)):
        # NGSIM record frequency 10Hz
        time_stamp = int(VehicleInfo[i][1])
        vehicle_id = VehicleInfo[i][0]
        local_x = VehicleInfo[i][3] * 0.3048
        # NGSIM Coordinate system reverse x-axis
        local_y = y_sign * VehicleInfo[i][2] * 0.3048
        velocity_x = round(VehicleInfo[i][4], 3) * 0.3048
        acceleration = round(VehicleInfo[i][5], 3) * 0.3048
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
        vehicle_id = VehicleMeta[i][0]
        vehicle_type = int(VehicleMeta[i][3])
        vehicle_length = VehicleMeta[i][1] * 0.3048
        vehicle_width = VehicleMeta[i][2] * 0.3048
        cursor.execute(insertPropertySql,(vehicle_id,vehicle_type,vehicle_length,vehicle_width))
        print(i)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # 'NGSIM_I_80_Scenario_DB'
    parser.add_argument('--DB', type=str, default=None)
    parser.add_argument('--Table', type=str, default=None)
    # eg: "../DataSet/NGSIM-Dataset/I-80-trajectory-data/trajectories-0515-0530.csv"
    parser.add_argument('--File', type=str, default=None)
    args = parser.parse_args()
    conn, cursor = init_DB(args.DB)
    table = "_" + args.Table
    CreateTrafficParticipantPropertyTable(cursor, table)
    CreateTrafficTimingStateTable(cursor, table)
    InsertTable(cursor, table, args.File)

    cursor.close()
    conn.commit()
    conn.close()