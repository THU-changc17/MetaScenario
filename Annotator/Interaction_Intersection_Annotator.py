from DBtools.init_db import init_DB
import DBtools.utils as utils
import math
from relation_extractor import RelationExtractor
from InsertDataBase.CreateTables import CreateScenarioBehaviorIndexTable
import numpy as np
import itertools
import json


def TrajectoryChunk(cursor, ChunkSize, vehicle_id, table):
    min_timestamp, max_timestamp = utils.SearchVehicleTimeRangeFromDB(cursor, vehicle_id, table)
    ChunkNum = math.ceil((max_timestamp - min_timestamp) / (ChunkSize * 1000))
    return ChunkNum, max_timestamp, min_timestamp


def BehaviorRecognition(cursor, ChunkSize, vehicle_id, table):
    ChunkNum, max_timestamp, min_timestamp = TrajectoryChunk(cursor, ChunkSize, vehicle_id, table)
    TimeSecList = list()
    for i in range(ChunkNum - 1):
        TimeSec = [min_timestamp + i * ChunkSize * 1000, min_timestamp + (i + 1) * ChunkSize * 1000]
        TimeSecList.append(TimeSec)
    TimeSec = [min_timestamp + (ChunkNum - 1) * ChunkSize * 1000, max_timestamp]
    TimeSecList.append(TimeSec)
    AllInteractionVehicle = list()
    AllOrientationList = list()
    for TimeSec in TimeSecList:
        InteractionVehicle = set()
        OrientationList = list()
        StartTime = TimeSec[0]
        EndTime = TimeSec[1]
        TimeInterval = 1000
        for NowTime in range(StartTime, EndTime, TimeInterval):
            r = RelationExtractor(cursor, vehicle_id, table)
            r.get_vehicle_relation(NowTime)
            InteractionVehicle.update(r.relation_vehicle)
            orientation = utils.SearchOrientationOnTime(cursor, vehicle_id, NowTime, table)
            OrientationList.append(orientation)
        AllInteractionVehicle.append(InteractionVehicle)
        AllOrientationList.append(OrientationList)
    return ChunkNum, TimeSecList, AllInteractionVehicle, AllOrientationList


def BehaviorStatisticAnnotate(cursor, ChunkSize, table):
    AllVehicleList = utils.SearchAllVehicleIDFromDB(cursor, table)
    print(len(AllVehicleList))
    behavior_interaction_arr = np.zeros((3,11)).astype(np.int)
    all_trajectory_num = 0
    insertIndexSql = "insert into Scenario_Behavior_Index" + table + "(ego_vehicle,time_stamp_begin,time_stamp_end,v2v_interaction_count,v2v_interaction_id,behavior) " \
                                                                     "values(%s,%s,%s,%s,%s,%s)"
    for vehicle in AllVehicleList:
        ChunkNum, TimeSecList, InteractionTemporalList, AllOrientationList = BehaviorRecognition(cursor, ChunkSize, vehicle, table)
        print(vehicle)
        behavior = ""
        all_trajectory_num = all_trajectory_num + len(InteractionTemporalList)
        for i in range(ChunkNum):
            flag = 0
            NowTimeSec = TimeSecList[i]
            y = len(InteractionTemporalList[i])
            if y>10:
                y=10
            for orien_combine_tuple in list(itertools.combinations(AllOrientationList[i], 2)):
                orien_1 = float(orien_combine_tuple[0])
                orien_2 = float(orien_combine_tuple[1])
                dorien = orien_2 - orien_1
                while dorien < -math.pi:
                    dorien = dorien + 2 * math.pi
                while dorien > math.pi:
                    dorien = dorien - 2 * math.pi
                if 0.65 * math.pi /2 < dorien:
                    behavior_interaction_arr[1][y] = behavior_interaction_arr[1][y] + 1
                    flag = 1
                    behavior = "turn left"
                    break
                elif dorien < -0.65 * math.pi /2:
                    behavior_interaction_arr[2][y] = behavior_interaction_arr[2][y] + 1
                    flag = 1
                    behavior = "turn right"
                    break
            if not flag:
                behavior = "go straight"
                behavior_interaction_arr[0][y] = behavior_interaction_arr[0][y] + 1

            cursor.execute(insertIndexSql,
                           (vehicle, NowTimeSec[0], NowTimeSec[1], y,
                            json.dumps(list(InteractionTemporalList[i])), behavior))
        print(behavior_interaction_arr)
    return behavior_interaction_arr


if __name__ == '__main__':
    conn, cursor = init_DB("Interaction_Intersection_EP0_Scenario_DB")
    ChunkSize = 8
    all_arr = np.zeros((3, 11)).astype(np.int)
    for i in range(8):
        print("table: ", i)
        table = "_" + str(i)
        CreateScenarioBehaviorIndexTable(cursor, table)
        arr = BehaviorStatisticAnnotate(cursor, ChunkSize, table)
        all_arr = all_arr + arr
        print(all_arr)

    cursor.close()
    conn.commit()
    conn.close()