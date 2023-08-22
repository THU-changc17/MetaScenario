from DBtools.init_db import init_DB
import DBtools.utils as utils
import math
from relation_extractor import RelationExtractor
from InsertDataBase.CreateTables import CreateScenarioBehaviorIndexTable
import numpy as np
import json


def TrajectoryChunk(ChunkSize, BehaviorTimeRecord, min_timestamp, max_timestamp):
    TimeSecList = list()
    print(len(BehaviorTimeRecord))
    if len(BehaviorTimeRecord) == 1:
        ChunkNum = math.ceil((max_timestamp - min_timestamp) / (ChunkSize * 1000))
        for i in range(ChunkNum - 1):
            TimeSec = [min_timestamp + i * ChunkSize * 1000, min_timestamp + (i + 1) * ChunkSize * 1000]
            TimeSecList.append(TimeSec)
        TimeSec = [min_timestamp + (ChunkNum - 1) * ChunkSize * 1000, max_timestamp]
        TimeSecList.append(TimeSec)
    else:
        if BehaviorTimeRecord[1] - BehaviorTimeRecord[0] > ChunkSize / 2 * 1000:
            ChunkNum = math.ceil((BehaviorTimeRecord[1] - BehaviorTimeRecord[0]
                                  - ChunkSize / 2 * 1000) / (ChunkSize * 1000))
            for i in range(ChunkNum - 1):
                TimeSec = [BehaviorTimeRecord[0] + i * ChunkSize * 1000,
                           BehaviorTimeRecord[0] + (i + 1) * ChunkSize * 1000]
                TimeSecList.append(TimeSec)
            TimeSec = [BehaviorTimeRecord[0] + (ChunkNum - 1) * ChunkSize * 1000,
                       BehaviorTimeRecord[1] - ChunkSize / 2 * 1000]
            TimeSecList.append(TimeSec)
            left_border = BehaviorTimeRecord[1] - ChunkSize / 2 * 1000
        else:
            left_border = BehaviorTimeRecord[0]

        for ind in range(2, len(BehaviorTimeRecord)):
            if BehaviorTimeRecord[ind] - BehaviorTimeRecord[ind - 1] > ChunkSize * 1000:
                ChunkNum = math.ceil((BehaviorTimeRecord[ind] - BehaviorTimeRecord[ind - 1]
                                      - ChunkSize * 1000) / (ChunkSize * 1000))
                right_border = BehaviorTimeRecord[ind - 1] + (ChunkSize / 2) * 1000
                TimeSecList.append([left_border, right_border])
                for i in range(ChunkNum - 1):
                    TimeSec = [BehaviorTimeRecord[ind - 1] + (ChunkSize / 2) * 1000 + i * ChunkSize * 1000,
                               BehaviorTimeRecord[ind - 1] + (ChunkSize / 2) * 1000 + (i + 1) * ChunkSize * 1000]
                    TimeSecList.append(TimeSec)
                TimeSec = [BehaviorTimeRecord[ind - 1] + (ChunkNum - 1) * ChunkSize * 1000,
                           BehaviorTimeRecord[ind] - ChunkSize / 2 * 1000]
                TimeSecList.append(TimeSec)
                left_border = BehaviorTimeRecord[ind] - ChunkSize / 2 * 1000
            else:
                right_border = (BehaviorTimeRecord[ind - 1] + BehaviorTimeRecord[ind]) / 2
                TimeSecList.append([left_border, right_border])
                left_border = right_border

        if max_timestamp - BehaviorTimeRecord[-1] > ChunkSize / 2 * 1000:
            TimeSecList.append([left_border, BehaviorTimeRecord[-1] + ChunkSize / 2 * 1000])
            ChunkNum = math.ceil((max_timestamp - BehaviorTimeRecord[-1]
                                  - ChunkSize / 2 * 1000) / (ChunkSize * 1000))
            for i in range(ChunkNum - 1):
                TimeSec = [BehaviorTimeRecord[-1] + ChunkSize / 2 * 1000 + i * ChunkSize * 1000,
                           BehaviorTimeRecord[-1] + ChunkSize / 2 * 1000 + (i + 1) * ChunkSize * 1000]
                TimeSecList.append(TimeSec)
            TimeSec = [BehaviorTimeRecord[-1] + ChunkSize / 2 * 1000 + (ChunkNum - 1) * ChunkSize * 1000,
                       max_timestamp]
            TimeSecList.append(TimeSec)
        else:
            TimeSecList.append([left_border, max_timestamp])
    ChunkNum = len(TimeSecList)
    return TimeSecList


def BahaviorRecognition(cursor, vehicle_id, table):
    min_timestamp, max_timestamp = utils.SearchVehicleTimeRangeFromDB(cursor, vehicle_id, table)
    BehaviorCapture = list()
    BehaviorTimeRecord = list()
    BehaviorChange = list()
    TimeInterval = 100
    r = RelationExtractor(cursor, vehicle_id, table)
    for NowTime in range(min_timestamp, max_timestamp, TimeInterval):
        # r.get_vehicle_relation(NowTime)
        # r.get_node_relation(NowTime)
        r.get_vehicle_lane_relation(NowTime)
        if len(BehaviorCapture) == 0:
            BehaviorCapture.append(r.relation_lane[0])
            BehaviorTimeRecord.append(NowTime)
        elif r.relation_lane[0] != BehaviorCapture[-1]:
            past_lane = BehaviorCapture[-1]
            now_lane = r.relation_lane[0]
            print(past_lane, now_lane)
            l_adj, r_adj = utils.SearchAdjacentLaneFromDB(cursor, past_lane)
            if past_lane == '7' and now_lane == '6':
                behavior = "merge"
                BehaviorChange.append(behavior)
            elif l_adj == now_lane:
                behavior = "left_lane_change"
                BehaviorChange.append(behavior)
            elif r_adj == now_lane:
                behavior = "right_lane_change"
                BehaviorChange.append(behavior)
            BehaviorCapture.append(r.relation_lane[0])
            BehaviorTimeRecord.append(NowTime)
    return BehaviorCapture, BehaviorTimeRecord, BehaviorChange, min_timestamp, max_timestamp


def BehaviorStatisticAnnotate(cursor, ChunkSize, table):
    insertIndexSql = "insert into Scenario_Behavior_Index" + table + "(ego_vehicle,time_stamp_begin,time_stamp_end,v2v_interaction_count,v2v_interaction_id,behavior) " \
                                                                     "values(%s,%s,%s,%s,%s,%s)"
    AllVehicleList = utils.SearchAllVehicleIDFromDB(cursor, table)
    print(len(AllVehicleList))
    behavior_interaction_arr = np.zeros((4, 11)).astype(np.int)
    all_trajectory_num = 0
    TimeInterval = 1000
    for vehicle in AllVehicleList:
        BehaviorCapture, BehaviorTimeRecord, BehaviorChange, min_timestamp, max_timestamp = BahaviorRecognition(cursor, vehicle, table)
        print(vehicle)
        TimeSecList = TrajectoryChunk(ChunkSize, BehaviorTimeRecord, min_timestamp, max_timestamp)
        all_trajectory_num = all_trajectory_num + len(TimeSecList)
        r = RelationExtractor(cursor, vehicle, table)
        for TimeSec in TimeSecList:
            behavior = "go straight"
            x = 0
            InteractionVehicle = set()
            StartTime = TimeSec[0]
            EndTime = TimeSec[1]
            NowTime = StartTime
            for ind in range(1, len(BehaviorTimeRecord)):
                if BehaviorTimeRecord[ind] > StartTime and BehaviorTimeRecord[ind] <= EndTime:
                    behavior = BehaviorChange[ind - 1]
            while NowTime < EndTime:
                r.get_vehicle_relation(NowTime)
                InteractionVehicle.update(r.relation_vehicle)
                NowTime = NowTime + TimeInterval
            if behavior == "go straight":
                x = 0
            elif behavior == "merge":
                x = 1
            elif behavior == "left_lane_change":
                x = 2
            elif behavior == "right_lane_change":
                x = 3
            y = len(list(InteractionVehicle))
            if y > 10:
                y = 10
            behavior_interaction_arr[x][y] = behavior_interaction_arr[x][y] + 1
            cursor.execute(insertIndexSql,
                           (vehicle, StartTime, EndTime, y,
                            json.dumps(list(InteractionVehicle)), behavior))

        print(behavior_interaction_arr)
    print(all_trajectory_num)
    return behavior_interaction_arr


if __name__ == '__main__':
    conn, cursor = init_DB("NGSIM_I_80_Scenario_DB")
    ChunkSize = 10
    all_arr = np.zeros((4, 11)).astype(np.int)
    for i in range(1, 4):
        print("table: ", i)
        table = "_" + str(i)
        CreateScenarioBehaviorIndexTable(cursor, table)
        arr = BehaviorStatisticAnnotate(cursor, ChunkSize, table)
        all_arr = all_arr + arr
        print(all_arr)

    cursor.close()
    conn.commit()
    conn.close()