import pymysql
import math
import pandas as pd
import numpy as np
from InsertDataBase.Interaction_MergingZS_InsertMap import LaneCurve
from InsertDataBase.CreateTables import *

conn = pymysql.connect(
    host='localhost',
    user="root",
    passwd="123456",
    db="Interaction_MergingZS_Scenario_DB")
cursor = conn.cursor()

VehicleInfo = list()
table = "_10"
file = "../DataSet/INTERACTION-Dataset-DR-v1_1/recorded_trackfiles/DR_CHN_Merging_ZS/vehicle_tracks_010.csv"
VehicleInfo = pd.read_csv(file, decimal=",", low_memory=False)
VehicleInfo = np.array(VehicleInfo)
VehicleInfo = np.hstack((VehicleInfo[:, 0:4], VehicleInfo[:, 4:].astype(np.float)))
print(type(VehicleInfo))


def InsertTable(table):
    insertTimingSql = "insert into Traffic_timing_state" + table + "(time_stamp,vehicle_id,local_x,local_y,velocity_x,velocity_y,orientation,lane_id) " \
                "values(%s,%s,%s,%s,%s,%s,%s,%s)"
    for i in range(len(VehicleInfo)):
        time_stamp = VehicleInfo[i][2]
        vehicle_id = VehicleInfo[i][0]
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
    CreateTrafficParticipantPropertyTable(cursor, table)
    CreateTrafficTimingStateTable(cursor, table)
    InsertTable(table)

    cursor.close()
    conn.commit()
    conn.close()