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
import itertools


conn = pymysql.connect(
        host='localhost',
        user="root",
        passwd="123456",
        db="Argoverse_MIA_Scenario_DB")
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


def TrajectoryChunk(cursor, vehicle_id, table):
    stampsql = "select time_stamp from Traffic_timing_state" + table + " where vehicle_id = %s" %(vehicle_id)
    cursor.execute(stampsql)
    timestampresult = cursor.fetchall()
    timestamp_set = set()
    for i in range(len(timestampresult)):
        timestamp_set.add(timestampresult[i][0])
    timestamp_list = list(timestamp_set)
    timestamp_list.sort()
    return timestamp_list


def TotalTimeStatistic(cursor, table):
    AllVehicleList = utils.SearchAllVehicleIDFromDB(cursor, table)
    # print(AllVehicleList)
    TotalTimelist = list()
    for vehicle in AllVehicleList:
        TotalTime = utils.SearchVehicleTotalTime(cursor, vehicle, table)
        # print(TotalTime)
        TotalTimeS = TotalTime / 1000
        TotalTimelist.append(TotalTimeS)
    arr = dict(Counter(TotalTimelist))
    num_type = list()
    sum_statistic = list()
    for key, value in arr.items():
        num_type.append(key)
        sum_statistic.append(value)
    plt.bar(num_type, sum_statistic)
    plt.show()


def LaneChangeRecognition(cursor, vehicle_id, table):
    timestamplist = TrajectoryChunk(cursor, vehicle_id, table)
    InteractionVehicle = set()
    LaneChangeCapture = list()
    LaneChangeTimeRecord = list()
    OrientationList = list()
    for i in range(len(timestamplist)):
        NowTime = timestamplist[i]
        r = RelationExtractor(cursor, vehicle_id, table)
        r.get_vehicle_relation(NowTime)
        # r.get_node_relation(NowTime)
        # r.get_vehicle_node_relation(NowTime)
        # r.get_vehicle_vehicle_relation(NowTime)
        r.get_vehicle_lane_relation(NowTime)
        r.get_lane_lane_relation()
        InteractionVehicle.update(r.relation_vehicle)
        orientation = utils.SearchOrientationOnTime(cursor, vehicle_id, NowTime, table)
        if orientation != None:
            OrientationList.append(orientation)
        # print(r.relation_V2V_list)
        # print(r.relation_V2N_list)
        # print(r.relation_V2L_list)
        # print(r.relation_L2L_list)
        for V2L in r.relation_V2L_list:
            if V2L[0]==vehicle_id and (len(LaneChangeCapture) == 0 or V2L[2] != LaneChangeCapture[-1]):
                LaneChangeCapture.append(V2L[2])
                LaneChangeTimeRecord.append(NowTime)
    return timestamplist, InteractionVehicle, LaneChangeCapture, LaneChangeTimeRecord, OrientationList


def LaneChangeStatisticAnnotate(cursor, table):
    insertIndexSql = "insert into Scenario_Behavior_Index" + table + "(ego_vehicle,time_stamp_begin,time_stamp_end,v2v_interaction_count,v2v_interaction_id,behavior) " \
                                                                     "values(%s,%s,%s,%s,%s,%s)"
    AllVehicleList = utils.SearchAllVehicleIDFromDB(cursor, table)
    print(len(AllVehicleList))
    # print(AllVehicleList)
    lanechange_interaction_arr = np.zeros((6,11)).astype(np.int)
    all_trajectory_num = 0
    # f = open('../../Argoverse_Annotation/Argoverse_Data_Annotate' + table + '.csv', 'w', encoding='utf-8', newline='')
    # csv_writer = csv.writer(f)
    # csv_writer.writerow(["vehicle_id", "start_time(ms)", "end_time(ms)", "v2v_num", "vehicle_interaction", "behavior"])
    for vehicle in AllVehicleList:
        timestamplist, InteractionTemporalList, LaneTemporalList, LaneChangeTimeList, AllOrientationList = LaneChangeRecognition(cursor,vehicle, table)
        print(vehicle)
        # print(AllOrientationList)
        # print(InteractionTemporalList)
        # print(LaneTemporalList)
        all_trajectory_num = all_trajectory_num + len(InteractionTemporalList)
        x = len(LaneTemporalList) - 1
        y = len(InteractionTemporalList)
        if y > 10:
            y = 10
        NowTimeSec = [timestamplist[0] ,timestamplist[-1]]
        behavior = "go straight"
        lane_change_flag = 0
        turn_flag = 0
        min_x, max_x, min_y ,max_y = utils.SearchVehicleLocationRangeFromDB(cursor, table, vehicle)
        if (max_x - min_x) < 10 and (max_y - min_y) < 10:
            lanechange_interaction_arr[5][y] = lanechange_interaction_arr[5][y] + 1
            behavior = "stop"
            cursor.execute(insertIndexSql,
                           (vehicle,float(NowTimeSec[0] / 1000), float(NowTimeSec[1] / 1000), y,
                            json.dumps(list(InteractionTemporalList)), behavior))
            # csv_writer.writerow([vehicle, float(NowTimeSec[0] / 1000), float(NowTimeSec[1] / 1000), y,list(InteractionTemporalList), behavior])
            print(lanechange_interaction_arr)
            continue
        for i in range(len(LaneTemporalList) - 1):
            past_lane = LaneTemporalList[i]
            now_lane = LaneTemporalList[i + 1]
            print(past_lane, now_lane)
            l_adj, r_adj = utils.SearchAdjacentLaneFromDB(cursor, past_lane)
            fl_adj, fr_adj = utils.SearchFrontAdjacentLaneFromDB(cursor, past_lane)
            if l_adj == now_lane or fl_adj == now_lane:
                lanechange_interaction_arr[1][y] = lanechange_interaction_arr[1][y] + 1
                behavior = "left_lane_change"
                cursor.execute(insertIndexSql,
                               (vehicle, float(NowTimeSec[0] / 1000), float(NowTimeSec[1] / 1000), y,
                                json.dumps(list(InteractionTemporalList)), behavior))
                lane_change_flag = 1
            elif r_adj == now_lane or fr_adj == now_lane:
                lanechange_interaction_arr[2][y] = lanechange_interaction_arr[2][y] + 1
                behavior = "right_lane_change"
                cursor.execute(insertIndexSql,
                               (vehicle, float(NowTimeSec[0] / 1000), float(NowTimeSec[1] / 1000), y,
                                json.dumps(list(InteractionTemporalList)), behavior))
                lane_change_flag = 1


        turn_direction_way_list = list()
        for j in range(len(LaneTemporalList)):
            now_lane = LaneTemporalList[j]
            Channelization = utils.SearchChannelizationOnWayFromDB(cursor, now_lane)
            if Channelization.get('turn_direction'):
                turn_direction_way_list.append(Channelization.get('turn_direction'))

        print(turn_direction_way_list)
        if turn_direction_way_list.count('RIGHT') > 2:
            for orien_combine_tuple in list(itertools.combinations(AllOrientationList, 2)):
                orien_1 = float(orien_combine_tuple[0])
                orien_2 = float(orien_combine_tuple[1])
                dorien = orien_2 - orien_1
                while dorien < -math.pi:
                    dorien = dorien + 2 * math.pi
                while dorien > math.pi:
                    dorien = dorien - 2 * math.pi
                if dorien < -0.65 * math.pi /2:
                    lanechange_interaction_arr[4][y] = lanechange_interaction_arr[4][y] + 1
                    turn_flag = 1
                    behavior = "turn_right"
                    cursor.execute(insertIndexSql,
                                   (vehicle, float(NowTimeSec[0] / 1000), float(NowTimeSec[1] / 1000), y,
                                    json.dumps(list(InteractionTemporalList)), behavior))
                    break

        if turn_direction_way_list.count('LEFT') > 2:
            for orien_combine_tuple in list(itertools.combinations(AllOrientationList, 2)):
                orien_1 = float(orien_combine_tuple[0])
                orien_2 = float(orien_combine_tuple[1])
                dorien = orien_2 - orien_1
                while dorien < -math.pi:
                    dorien = dorien + 2 * math.pi
                while dorien > math.pi:
                    dorien = dorien - 2 * math.pi
                # print(dorien)
                if 0.65 * math.pi / 2 < dorien:
                    lanechange_interaction_arr[3][y] = lanechange_interaction_arr[3][y] + 1
                    turn_flag = 1
                    behavior = "turn_left"
                    cursor.execute(insertIndexSql,
                                   (vehicle, float(NowTimeSec[0] / 1000), float(NowTimeSec[1] / 1000), y,
                                    json.dumps(list(InteractionTemporalList)), behavior))
                    break
        # for orien_combine_tuple in list(itertools.combinations(AllOrientationList, 2)):
        #     orien_1 = float(orien_combine_tuple[0])
        #     orien_2 = float(orien_combine_tuple[1])
        #     dorien = orien_2 - orien_1
        #     while dorien < -math.pi:
        #         dorien = dorien + 2 * math.pi
        #     while dorien > math.pi:
        #         dorien = dorien - 2 * math.pi
        #     # print(dorien)
        #     if 0.65 * math.pi /2 < dorien:
        #         lanechange_interaction_arr[3][y] = lanechange_interaction_arr[3][y] + 1
        #         turn_flag = 1
        #         behavior = "turn_left"
        #         csv_writer.writerow([vehicle, float(NowTimeSec[0] / 1000), float(NowTimeSec[1] / 1000), y, list(InteractionTemporalList), behavior])
        #         break
        #     elif dorien < -0.65 * math.pi /2:
        #         lanechange_interaction_arr[4][y] = lanechange_interaction_arr[4][y] + 1
        #         turn_flag = 1
        #         behavior = "turn_right"
        #         csv_writer.writerow([vehicle, float(NowTimeSec[0] / 1000), float(NowTimeSec[1] / 1000), y, list(InteractionTemporalList), behavior])
        #         break

        if lane_change_flag == 0 and turn_flag == 0:
            lanechange_interaction_arr[0][y] = lanechange_interaction_arr[0][y] + 1
            cursor.execute(insertIndexSql,
                           (vehicle, float(NowTimeSec[0] / 1000), float(NowTimeSec[1] / 1000), y,
                            json.dumps(list(InteractionTemporalList)), behavior))

        print(lanechange_interaction_arr)
    # f.close()
    return lanechange_interaction_arr


if __name__ == '__main__':
    cursor = init_DB("Argoverse_MIA_Scenario_DB")
    csv_reader = csv.reader(open("../Annotator/sample_record.csv", encoding='utf-8'))
    all_lanechange_interaction_arr = np.zeros((6, 11)).astype(np.int)
    for i, rows in enumerate(csv_reader):
        table = str(rows[0])
        print("table: ", table)
        table = "_" + table
        CreateScenarioBehaviorIndexTable(table)
    # TotalTimeStatistic(cursor, table)
    # ChunkNumStatistic(cursor, ChunkSize, table)
    # LaneChangeRecognition(cursor, ChunkSize, 395, table)
        arr = LaneChangeStatisticAnnotate(cursor, table)
        all_lanechange_interaction_arr = all_lanechange_interaction_arr + arr
        print(all_lanechange_interaction_arr)