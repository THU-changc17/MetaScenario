from DBtools.init_db import init_DB
import DBtools.utils as utils
import math
from collections import Counter
import matplotlib.pyplot as plt
from relation_extractor import RelationExtractor
from scene_graph_visualization import SceneGraph
from InsertDataBase.CreateTables import CreateScenarioBehaviorIndexTable
import numpy as np
import pandas as pd
import csv
import seaborn as sns
import pymysql
import json
import itertools


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


def BehaviorRecognition(cursor, vehicle_id, table):
    timestamplist = TrajectoryChunk(cursor, vehicle_id, table)
    InteractionVehicle = set()
    LaneChangeCapture = list()
    LaneChangeTimeRecord = list()
    OrientationList = list()
    for i in range(len(timestamplist)):
        NowTime = timestamplist[i]
        r = RelationExtractor(cursor, vehicle_id, table)
        r.get_vehicle_relation(NowTime)
        r.get_vehicle_lane_relation(NowTime)
        r.get_lane_lane_relation()
        InteractionVehicle.update(r.relation_vehicle)
        orientation = utils.SearchOrientationOnTime(cursor, vehicle_id, NowTime, table)
        if orientation != None:
            OrientationList.append(orientation)
        for V2L in r.relation_V2L_list:
            if V2L[0]==vehicle_id and (len(LaneChangeCapture) == 0 or V2L[2] != LaneChangeCapture[-1]):
                LaneChangeCapture.append(V2L[2])
                LaneChangeTimeRecord.append(NowTime)
    return timestamplist, InteractionVehicle, LaneChangeCapture, LaneChangeTimeRecord, OrientationList


def BehaviorStatisticAnnotate(cursor, table):
    insertIndexSql = "insert into Scenario_Behavior_Index_v2" + table + "(ego_vehicle,time_stamp_begin,time_stamp_end,v2v_interaction_count,v2v_interaction_id,behavior) " \
                                                                     "values(%s,%s,%s,%s,%s,%s)"
    AllVehicleList = utils.SearchAllVehicleIDFromDB(cursor, table)
    print(len(AllVehicleList))
    behavior_interaction_arr = np.zeros((6,11)).astype(np.int)
    all_trajectory_num = 0
    for vehicle in AllVehicleList:
        timestamplist, InteractionTemporalList, LaneTemporalList, LaneChangeTimeList, AllOrientationList = BehaviorRecognition(cursor,vehicle, table)
        print(vehicle)
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
            behavior_interaction_arr[5][y] = behavior_interaction_arr[5][y] + 1
            behavior = "stop"
            cursor.execute(insertIndexSql,
                           (vehicle,float(NowTimeSec[0] / 1000), float(NowTimeSec[1] / 1000), y,
                            json.dumps(list(InteractionTemporalList)), behavior))
            print(behavior_interaction_arr)
            continue
        for i in range(len(LaneTemporalList) - 1):
            past_lane = LaneTemporalList[i]
            now_lane = LaneTemporalList[i + 1]
            print(past_lane, now_lane)
            l_adj, r_adj = utils.SearchAdjacentLaneFromDB(cursor, past_lane)
            fl_adj, fr_adj = utils.SearchFrontAdjacentLaneFromDB(cursor, past_lane)
            if l_adj == now_lane or fl_adj == now_lane:
                behavior_interaction_arr[1][y] = behavior_interaction_arr[1][y] + 1
                behavior = "left_lane_change"
                cursor.execute(insertIndexSql,
                               (vehicle, float(NowTimeSec[0] / 1000), float(NowTimeSec[1] / 1000), y,
                                json.dumps(list(InteractionTemporalList)), behavior))
                lane_change_flag = 1
            elif r_adj == now_lane or fr_adj == now_lane:
                behavior_interaction_arr[2][y] = behavior_interaction_arr[2][y] + 1
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
                    behavior_interaction_arr[4][y] = behavior_interaction_arr[4][y] + 1
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
                if 0.65 * math.pi / 2 < dorien:
                    behavior_interaction_arr[3][y] = behavior_interaction_arr[3][y] + 1
                    turn_flag = 1
                    behavior = "turn_left"
                    cursor.execute(insertIndexSql,
                                   (vehicle, float(NowTimeSec[0] / 1000), float(NowTimeSec[1] / 1000), y,
                                    json.dumps(list(InteractionTemporalList)), behavior))
                    break

        if lane_change_flag == 0 and turn_flag == 0:
            behavior_interaction_arr[0][y] = behavior_interaction_arr[0][y] + 1
            cursor.execute(insertIndexSql,
                           (vehicle, float(NowTimeSec[0] / 1000), float(NowTimeSec[1] / 1000), y,
                            json.dumps(list(InteractionTemporalList)), behavior))

        print(behavior_interaction_arr)
    return behavior_interaction_arr


if __name__ == '__main__':
    conn, cursor = init_DB("Argoverse_MIA_Scenario_DB")
    csv_reader = csv.reader(open("../Annotator/sample_record.csv", encoding='utf-8'))
    all_lanechange_interaction_arr = np.zeros((6, 11)).astype(np.int)
    for i, rows in enumerate(csv_reader):
        table = str(rows[0])
        print("table: ", table)
        table = "_" + table
        CreateScenarioBehaviorIndexTable(cursor, table)
        arr = BehaviorStatisticAnnotate(cursor, table)
        all_lanechange_interaction_arr = all_lanechange_interaction_arr + arr
        print(all_lanechange_interaction_arr)

    cursor.close()
    conn.commit()
    conn.close()