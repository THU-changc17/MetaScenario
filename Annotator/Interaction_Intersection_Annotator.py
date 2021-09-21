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
import itertools
import pymysql
import json

conn = pymysql.connect(
        host='localhost',
        user="root",
        passwd="123456",
        db="Interaction_Intersection_EP0_Scenario_DB")
# 获取游标
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


def TotalTimeStatistic(cursor, table):
    AllVehicleList = utils.SearchAllVehicleIDFromDB(cursor, table)
    # print(AllVehicleList)
    TotalTimelist = list()
    for vehicle in AllVehicleList:
        TotalTime = utils.SearchVehicleTotalTime(cursor, vehicle, table)
        # print(TotalTime)
        TotalTimeS = int(TotalTime / 1000)
        TotalTimelist.append(TotalTimeS)
    arr = dict(Counter(TotalTimelist))
    # num_type = list()
    # sum_statistic = list()
    # for key, value in arr.items():
    #     num_type.append(key)
    #     sum_statistic.append(value)
    # plt.bar(num_type, sum_statistic)
    # plt.show()
    return arr


def LaneChangeRecognition(cursor, ChunkSize, vehicle_id, table):
    ChunkNum, max_timestamp, min_timestamp = TrajectoryChunk(cursor, ChunkSize, vehicle_id, table)
    TimeSecList = list()
    for i in range(ChunkNum - 1):
        TimeSec = [min_timestamp + i * ChunkSize * 1000, min_timestamp + (i + 1) * ChunkSize * 1000]
        TimeSecList.append(TimeSec)
    TimeSec = [min_timestamp + (ChunkNum - 1) * ChunkSize * 1000, max_timestamp]
    TimeSecList.append(TimeSec)
    AllInteractionVehicle = list()
    # AllLaneChangeCapture = list()
    # AllVelocityList = list()
    AllOrientationList = list()
    for TimeSec in TimeSecList:
        InteractionVehicle = set()
        # LaneChangeCapture = list()
        # VelocityList = list()
        OrientationList = list()
        StartTime = TimeSec[0]
        EndTime = TimeSec[1]
        TimeInterval = 1000
        for NowTime in range(StartTime, EndTime, TimeInterval):
            r = RelationExtractor(cursor, vehicle_id, table)
            r.get_vehicle_relation(NowTime)
            # r.get_node_relation(NowTime)
            # r.get_vehicle_node_relation(NowTime)
            # r.get_vehicle_vehicle_relation(NowTime)
            # r.get_vehicle_lane_relation(NowTime)
            # r.get_lane_lane_relation()
            InteractionVehicle.update(r.relation_vehicle)
            orientation = utils.SearchOrientationOnTime(cursor, vehicle_id, NowTime, table)
            # _,_,velocity = utils.SearchVelocityOnTime(cursor, vehicle_id, NowTime)
            # VelocityList.append(velocity)
            OrientationList.append(orientation)
            # print(r.relation_V2V_list)
            # print(r.relation_V2N_list)
            # print(r.relation_V2L_list)
            # print(r.relation_L2L_list)
            # for V2L in r.relation_V2L_list:
            #     if V2L[0]==vehicle_id and V2L[2] not in LaneChangeCapture:
            #         LaneChangeCapture.append(V2L[2])
        AllInteractionVehicle.append(InteractionVehicle)
        # AllLaneChangeCapture.append(LaneChangeCapture)
        # AllVelocityList.append(VelocityList)
        AllOrientationList.append(OrientationList)
    return ChunkNum, TimeSecList, AllInteractionVehicle, AllOrientationList


def LaneChangeStatisticAnnotate(cursor, ChunkSize, table):
    AllVehicleList = utils.SearchAllVehicleIDFromDB(cursor, table)
    print(len(AllVehicleList))
    # print(AllVehicleList)
    lanechange_interaction_arr = np.zeros((3,11)).astype(np.int)
    all_trajectory_num = 0
    # f = open('Interaction_Intersection_Annotate' + table + '.csv', 'w', encoding='utf-8',newline='')
    # csv_writer = csv.writer(f)
    # csv_writer.writerow(["vehicle_id", "start_time(ms)", "end_time(ms)", "v2v_num", "vehicle_interaction", "behavior"])
    insertIndexSql = "insert into Scenario_Behavior_Index" + table + "(ego_vehicle,time_stamp_begin,time_stamp_end,v2v_interaction_count,v2v_interaction_id,behavior) " \
                                                                     "values(%s,%s,%s,%s,%s,%s)"
    for vehicle in AllVehicleList:
        ChunkNum, TimeSecList, InteractionTemporalList, AllOrientationList = LaneChangeRecognition(cursor, ChunkSize, vehicle, table)
        print(vehicle)
        behavior = ""
        # print(InteractionTemporalList)
        # print(LaneTemporalList)
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
                # print(dorien)
                if 0.65 * math.pi /2 < dorien:
                    lanechange_interaction_arr[1][y] = lanechange_interaction_arr[1][y] + 1
                    flag = 1
                    behavior = "turn left"
                    break
                elif dorien < -0.65 * math.pi /2:
                    lanechange_interaction_arr[2][y] = lanechange_interaction_arr[2][y] + 1
                    flag = 1
                    behavior = "turn right"
                    break
            if not flag:
                behavior = "go straight"
                lanechange_interaction_arr[0][y] = lanechange_interaction_arr[0][y] + 1

            cursor.execute(insertIndexSql,
                           (vehicle, NowTimeSec[0], NowTimeSec[1], y,
                            json.dumps(list(InteractionTemporalList[i])), behavior))
            # csv_writer.writerow([vehicle, NowTimeSec[0], NowTimeSec[1], y, list(InteractionTemporalList[i]), behavior])
        print(lanechange_interaction_arr)
    return lanechange_interaction_arr



if __name__ == '__main__':
    cursor = init_DB("Interaction_Intersection_EP0_Scenario_DB")
    ChunkSize = 8  # 8s
    # table = ""
    # # ChunkNumStatistic(cursor, ChunkSize)
    # TotalTimeStatistic(cursor, table)
    # # LaneChangeRecognition(cursor, ChunkSize, 395, table)
    # LaneChangeStatisticAnnotate(cursor, ChunkSize, table)

    all_time_dur = dict()
    for i in range(8):
        table = "_" + str(i)
        temp_dict = TotalTimeStatistic(cursor, table)
        for k, v in temp_dict.items():
            if k not in all_time_dur:
                all_time_dur[k] = v
            else:
                all_time_dur[k] += v
        print(all_time_dur)

    num_type = list()
    sum_statistic = list()
    for key, value in all_time_dur.items():
        num_type.append(key)
        sum_statistic.append(value)
    plt.bar(num_type, sum_statistic)
    plt.show()

    # all_arr = np.zeros((3,11)).astype(np.int)
    # for i in range(8):
    #     print("table: ", i)
    #     table = "_" + str(i)
    #     arr = LaneChangeStatisticAnnotate(cursor, ChunkSize, table)
    #     all_arr = all_arr + arr
    #     print(all_arr)