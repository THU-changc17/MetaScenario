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


plt.figure(figsize=(14, 10))
for plt_index in range(1, 5):
    # 往画布上添加子图：按三行二列，添加到下标为plt_index的位置
    plt.subplot(2, 2, plt_index)
    if plt_index == 1:
        conn = pymysql.connect(
            host='localhost',
            user="root",
            passwd="123456",
            db="NGSIM_I80_Scenario_DB")
        # 获取游标
        cursor = conn.cursor()
        # 绘图
        all_time_dur = dict()
        for i in range(1, 4):
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
        plt.xlabel("Total Record Time")
        plt.ylabel("Participant Count")
        plt.title("NGSIM I80 DataSet")

    if plt_index == 2:
        conn = pymysql.connect(
            host='localhost',
            user="root",
            passwd="123456",
            db="HighD_I_Scenario_DB")
        # 获取游标
        cursor = conn.cursor()
        # 绘图
        all_time_dur = dict()
        for i in range(1, 4):
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
        plt.xlabel("Total Record Time")
        plt.ylabel("Participant Count")
        plt.title("HighD Section I DataSet")

    if plt_index == 3:
        conn = pymysql.connect(
            host='localhost',
            user="root",
            passwd="123456",
            db="Interaction_MergingZS_Scenario_DB")
        # 获取游标
        cursor = conn.cursor()
        # 绘图
        all_time_dur = dict()
        for i in range(0, 11):
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
        plt.xlabel("Total Record Time")
        plt.ylabel("Participant Count")
        plt.title("Interaction MergingZS DataSet")

    if plt_index == 4:
        conn = pymysql.connect(
            host='localhost',
            user="root",
            passwd="123456",
            db="Interaction_Intersection_EP0_Scenario_DB")
        # 获取游标
        cursor = conn.cursor()
        # 绘图
        all_time_dur = dict()
        for i in range(0, 8):
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
        plt.xlabel("Total Record Time")
        plt.ylabel("Participant Count")
        plt.title("Interaction IntersectionEP0 DataSet")

plt.savefig("../recordtime_num.eps")
plt.show()
