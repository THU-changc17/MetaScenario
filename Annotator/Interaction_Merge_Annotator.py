import sys
sys.path.append("../")
import argparse
from DBtools.init_db import init_DB
import DBtools.utils as utils
import math
from collections import Counter
from relation_extractor import RelationExtractor
from InsertDataBase.CreateTables import CreateScenarioBehaviorIndexTable
import numpy as np
import json

def TrajectoryChunk(cursor, ChunkSize, vehicle_id, table):
    min_timestamp, max_timestamp = utils.SearchVehicleTimeRangeFromDB(cursor, vehicle_id, table)
    ChunkNum = math.ceil((max_timestamp - min_timestamp) / (ChunkSize * 1000))
    return ChunkNum, max_timestamp, min_timestamp


def TotalTimeStatistic(cursor, table):
    AllVehicleList = utils.SearchAllVehicleIDFromDB(cursor, table)
    TotalTimelist = list()
    for vehicle in AllVehicleList:
        TotalTime = utils.SearchVehicleTotalTime(cursor, vehicle, table)
        TotalTimeS = int(TotalTime / 1000)
        TotalTimelist.append(TotalTimeS)
    arr = dict(Counter(TotalTimelist))
    return arr


def BahaviorRecognition(cursor, ChunkSize, vehicle_id, table):
    ChunkNum, max_timestamp, min_timestamp = TrajectoryChunk(cursor, ChunkSize, vehicle_id, table)
    TimeSecList = list()
    for i in range(ChunkNum - 1):
        TimeSec = [min_timestamp + i * ChunkSize * 1000, min_timestamp + (i + 1) * ChunkSize * 1000]
        TimeSecList.append(TimeSec)
    TimeSec = [min_timestamp + (ChunkNum - 1) * ChunkSize * 1000, max_timestamp]
    TimeSecList.append(TimeSec)
    AllInteractionVehicle = list()
    AllBehaviorChangeCapture = list()
    AllVelocityList = list()
    AllBehaviorChangeTimeRecord = list()
    for TimeSec in TimeSecList:
        InteractionVehicle = set()
        BehaviorCapture = list()
        VelocityList = list()
        BehaviorTimeRecord = list()
        StartTime = TimeSec[0]
        EndTime = TimeSec[1]
        TimeInterval = 1000
        for NowTime in range(StartTime, EndTime, TimeInterval):
            r = RelationExtractor(cursor, vehicle_id, table)
            r.get_vehicle_relation(NowTime)
            r.get_vehicle_lane_relation(NowTime)
            r.get_lane_lane_relation()
            InteractionVehicle.update(r.relation_vehicle)
            _,_,velocity = utils.SearchVelocityOnTime(cursor, vehicle_id, NowTime, table)
            VelocityList.append(velocity)
            for V2L in r.relation_V2L_list:
                if V2L[0] == vehicle_id and (len(BehaviorCapture) == 0 or V2L[2] != BehaviorCapture[-1]):
                    BehaviorCapture.append(V2L[2])
                    BehaviorTimeRecord.append(NowTime)
        AllInteractionVehicle.append(InteractionVehicle)
        AllBehaviorChangeCapture.append(BehaviorCapture)
        AllBehaviorChangeTimeRecord.append(BehaviorTimeRecord)
        AllVelocityList.append(VelocityList)
    return ChunkNum, TimeSecList, AllInteractionVehicle, AllBehaviorChangeCapture, AllBehaviorChangeTimeRecord, AllVelocityList


def BehaviorStatisticAnnotate(cursor, ChunkSize, table):
    insertIndexSql = "insert into Scenario_Behavior_Index" + table + "(ego_vehicle,time_stamp_begin,time_stamp_end,v2v_interaction_count,v2v_interaction_id,behavior) " \
                        "values(%s,%s,%s,%s,%s,%s)"
    AllVehicleList = utils.SearchAllVehicleIDFromDB(cursor, table)
    print(len(AllVehicleList))
    behavior_interaction_arr = np.zeros((5,11)).astype(np.int)
    all_trajectory_num = 0
    for vehicle in AllVehicleList[:50]:
        ChunkNum, TimeSecList, InteractionTemporalList, LaneTemporalList, LaneChangeTimeRecord, AllVelocityList = BahaviorRecognition(cursor, ChunkSize, vehicle, table)
        print(vehicle)
        behavior = ""
        all_trajectory_num = all_trajectory_num + len(TimeSecList)
        for i in range(ChunkNum):
            x = len(LaneTemporalList[i]) - 1
            y = len(InteractionTemporalList[i])
            if y > 10:
                y = 10
            NowTimeSec = TimeSecList[i]
            if x!= 0:
                if(LaneChangeTimeRecord[i][0] > NowTimeSec[0]):
                    cursor.execute(insertIndexSql,
                                   (vehicle, NowTimeSec[0], LaneChangeTimeRecord[i][0], y,
                                    json.dumps(list(InteractionTemporalList[i])), "go straight"))
                for j in range(x):
                    past_lane = list(LaneTemporalList[i])[j]
                    now_lane = list(LaneTemporalList[i])[j + 1]
                    print(past_lane, now_lane)
                    l_adj, r_adj = utils.SearchAdjacentLaneFromDB(cursor, past_lane)
                    # fl_adj, fr_adj = utils.SearchFrontAdjacentLaneFromDB(cursor, past_lane)
                    if (past_lane == '7' and now_lane == '6') or (past_lane == '1' and now_lane == '2'):
                        behavior_interaction_arr[2][y] = behavior_interaction_arr[2][y] + 1
                        behavior = "merge"
                    elif l_adj == now_lane:
                        behavior_interaction_arr[3][y] = behavior_interaction_arr[3][y] + 1
                        behavior = "left_lane_change"
                    elif r_adj == now_lane:
                        behavior_interaction_arr[4][y] = behavior_interaction_arr[4][y] + 1
                        behavior = "right_lane_change"
                    cursor.execute(insertIndexSql,
                                   (vehicle, LaneChangeTimeRecord[i][j], LaneChangeTimeRecord[i][j + 1], y,
                                    json.dumps(list(InteractionTemporalList[i])), behavior))
                if (LaneChangeTimeRecord[i][-1] < NowTimeSec[1]):
                    cursor.execute(insertIndexSql,
                                   (vehicle, LaneChangeTimeRecord[i][-1], NowTimeSec[1], y,
                                    json.dumps(list(InteractionTemporalList[i])), "go straight"))
            elif x == 0 and all(j < 1e-3 for j in AllVelocityList[i]):
                behavior = "stop"
                behavior_interaction_arr[1][y] = behavior_interaction_arr[1][y] + 1
                cursor.execute(insertIndexSql, (vehicle, NowTimeSec[0], NowTimeSec[1], y, json.dumps(list(InteractionTemporalList[i])), behavior))
            else:
                behavior = "go straight"
                behavior_interaction_arr[0][y] = behavior_interaction_arr[0][y] + 1
                cursor.execute(insertIndexSql, (vehicle, NowTimeSec[0], NowTimeSec[1], y, json.dumps(list(InteractionTemporalList[i])), behavior))
        print(behavior_interaction_arr)
    print(all_trajectory_num)
    return behavior_interaction_arr



if __name__ == '__main__':
    # "Interaction_MergingZS_Scenario_DB"
    parser = argparse.ArgumentParser()
    parser.add_argument('--DB', type=str, default=None)
    parser.add_argument('--Table', type=str, default=None)
    parser.add_argument('--Chunksize', type=int, default=10)
    args = parser.parse_args()
    conn, cursor = init_DB(args.DB)
    table = "_" + args.Table
    CreateScenarioBehaviorIndexTable(cursor, table)
    arr = BehaviorStatisticAnnotate(cursor, args.Chunksize, table)

    cursor.close()
    conn.commit()
    conn.close()