from init_db import init_DB
import utils
import math
from collections import Counter
import matplotlib.pyplot as plt
from relation_extractor import RelationExtractor
from scene_graph_visualization import SceneGraph
import numpy as np
import pandas as pd
import csv
import seaborn as sns
import pymysql
import json


conn = pymysql.connect(
        host='localhost',
        user="root",
        passwd="123456",
        db="HighD_I_Scenario_DB")
cursor = conn.cursor()


def CreateScenarioBehaviorIndexTable(table):
    cursor.execute('drop table if exists Scenario_Behavior_Index' + table)
    ScenarioBehaviorIndexTable = """CREATE TABLE IF NOT EXISTS `Scenario_Behavior_Index""" + table + """` (
              `data_id` bigint NOT NULL AUTO_INCREMENT,
        	  `ego_vehicle` bigint NOT NULL,
        	  `time_stamp_begin` bigint NOT NULL,
        	  `time_stamp_end` bigint NOT NULL,
        	  `v2v_interaction_count` bigint,
        	  `v2v_interaction_id` json,
        	  `behavior` varchar(32),
        	  PRIMARY KEY (`data_id`)
        	) ENGINE=InnoDB  DEFAULT CHARSET=utf8 AUTO_INCREMENT=0"""
    cursor.execute(ScenarioBehaviorIndexTable)


def TrajectoryChunk(cursor, ChunkSize, vehicle_id, table):
    min_timestamp, max_timestamp = utils.SearchVehicleTimeRangeFromDB(cursor, vehicle_id, table)
    ChunkNum = math.ceil((max_timestamp - min_timestamp) / (ChunkSize * 1000))
    return ChunkNum, max_timestamp, min_timestamp


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
    AllBehaviorChangeTimeRecord = list()
    for TimeSec in TimeSecList:
        InteractionVehicle = set()
        BehaviorCapture = list()
        BehaviorTimeRecord = list()
        StartTime = TimeSec[0]
        EndTime = TimeSec[1]
        TimeInterval = 400
        for NowTime in range(StartTime, EndTime, TimeInterval):
            r = RelationExtractor(cursor, vehicle_id, table)
            r.get_vehicle_relation(NowTime)
            r.get_vehicle_lane_relation(NowTime)
            r.get_lane_lane_relation()
            InteractionVehicle.update(r.relation_vehicle)
            for V2L in r.relation_V2L_list:
                if V2L[0] == vehicle_id and (len(BehaviorCapture) == 0 or V2L[2] != BehaviorCapture[-1]):
                    BehaviorCapture.append(V2L[2])
                    BehaviorTimeRecord.append(NowTime)
        AllInteractionVehicle.append(InteractionVehicle)
        AllBehaviorChangeCapture.append(BehaviorCapture)
        AllBehaviorChangeTimeRecord.append(BehaviorTimeRecord)
    return ChunkNum, TimeSecList, AllInteractionVehicle, AllBehaviorChangeCapture, AllBehaviorChangeTimeRecord


def BehaviorStatisticAnnotate(cursor, ChunkSize, table):
    insertIndexSql = "insert into Scenario_Behavior_Index" + table + "(ego_vehicle,time_stamp_begin,time_stamp_end,v2v_interaction_count,v2v_interaction_id,behavior) " \
                                                                     "values(%s,%s,%s,%s,%s,%s)"
    AllVehicleList = utils.SearchAllVehicleIDFromDB(cursor, table)
    print(len(AllVehicleList))
    behavior_interaction_arr = np.zeros((3, 11)).astype(np.int)
    all_trajectory_num = 0
    for vehicle in AllVehicleList:
        ChunkNum, TimeSecList, InteractionTemporalList, LaneTemporalList, LaneChangeTimeRecord = BahaviorRecognition(cursor, ChunkSize, vehicle, table)
        print(vehicle)
        behavior = "go straight"
        all_trajectory_num = all_trajectory_num + len(TimeSecList)
        for i in range(ChunkNum):
            x = len(LaneTemporalList[i]) - 1
            y = len(InteractionTemporalList[i])
            if y > 10:
                y = 10
            NowTimeSec = TimeSecList[i]
            if x != 0:
                if (LaneChangeTimeRecord[i][0] > NowTimeSec[0]):
                    cursor.execute(insertIndexSql,
                                   (vehicle, NowTimeSec[0], LaneChangeTimeRecord[i][0], y,
                                    json.dumps(list(InteractionTemporalList[i])), "go straight"))
                for j in range(x):
                    past_lane = list(LaneTemporalList[i])[j]
                    now_lane = list(LaneTemporalList[i])[j + 1]
                    print(past_lane, now_lane)
                    l_adj, r_adj = utils.SearchAdjacentLaneFromDB(cursor, past_lane)
                    if l_adj == now_lane:
                        behavior_interaction_arr[1][y] = behavior_interaction_arr[1][y] + 1
                        behavior = "left_lane_change"
                    elif r_adj == now_lane:
                        behavior_interaction_arr[2][y] = behavior_interaction_arr[2][y] + 1
                        behavior = "right_lane_change"
                    cursor.execute(insertIndexSql,
                                   (vehicle, LaneChangeTimeRecord[i][j], LaneChangeTimeRecord[i][j + 1], y,
                                    json.dumps(list(InteractionTemporalList[i])), behavior))
                if (LaneChangeTimeRecord[i][-1] < NowTimeSec[1]):
                    cursor.execute(insertIndexSql,
                                   (vehicle, LaneChangeTimeRecord[i][-1], NowTimeSec[1], y,
                                    json.dumps(list(InteractionTemporalList[i])), "go straight"))
            else:
                behavior = "go straight"
                behavior_interaction_arr[0][y] = behavior_interaction_arr[0][y] + 1
                cursor.execute(insertIndexSql, (vehicle, NowTimeSec[0], NowTimeSec[1], y, json.dumps(list(InteractionTemporalList[i])), behavior))
        print(behavior_interaction_arr)
    print(all_trajectory_num)
    return behavior_interaction_arr


if __name__ == '__main__':
    ChunkSize = 6
    all_arr = np.zeros((3, 11)).astype(np.int)
    for i in range(1, 4):
        print("table: ", i)
        table = "_" + str(i)
        CreateScenarioBehaviorIndexTable(table)
        arr = BehaviorStatisticAnnotate(cursor, ChunkSize, table)
        all_arr = all_arr + arr
        print(all_arr)

    cursor.close()
    conn.commit()
    conn.close()