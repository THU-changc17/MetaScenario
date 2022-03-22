import matplotlib.patches
import matplotlib.pyplot as plt
import numpy as np
from map_visualization import draw_map,draw_map_part
import random
import DBtools.utils as utils
from DBtools.init_db import init_DB
import time

def rotate_around_center(pts, center, yaw):
    return np.dot(pts - center, np.array([[np.cos(yaw), np.sin(yaw)], [-np.sin(yaw), np.cos(yaw)]])) + center


def polygon_xy_from_motionstate(x, y, psi_rad, width, length):
    lowleft = (x - length / 2., y - width / 2.)
    lowright = (x + length / 2., y - width / 2.)
    upright = (x + length / 2., y + width / 2.)
    upleft = (x - length / 2., y + width / 2.)
    return rotate_around_center(np.array([lowleft, lowright, upright, upleft]), np.array([x, y]), yaw=psi_rad)


def vehicle_visualize(DB, TableID, begin=None, end=None):
    conn, cursor = init_DB(DB)
    table = TableID
    if(begin==None):
        stampsql = "select time_stamp from Traffic_timing_state" + table
    else:
        stampsql = "select time_stamp from Traffic_timing_state" + table + " where time_stamp BETWEEN %s and %s"%(begin,end)
    cursor.execute(stampsql)
    timestampresult = cursor.fetchall()
    timestamp_set = set()
    for i in range(len(timestampresult)):
        timestamp_set.add(timestampresult[i][0])
    timestamp_list = list(timestamp_set)
    timestamp_list.sort()
    min_x, max_x, min_y, max_y = utils.SearchLocationRangeFromDB(cursor, table)
    X_range = [min_x - 10, max_x + 10]
    Y_range = [min_y - 10, max_y + 10]

    fig, axes = plt.subplots(1, 1)
    fig.canvas.set_window_title("Dataset Visualization")
    # draw_map(cursor, axes)
    draw_map_part(cursor, axes, X_range, Y_range)
    plt.ion()
    patch_dict = dict()
    text_dict = dict()
    vehicle_in_graph = list()
    color_list = ["whitesmoke", "oldlace", "lightgray", "aliceblue"]

    for index in range(0, len(timestamp_list), 20):
        timestamp = timestamp_list[index]
        vehicle_list = utils.SearchTrafficParticipantByTime(cursor, timestamp, table)
        for vehicle in vehicle_list:
            property_dict = utils.SearchTrafficParticipantProperty(cursor, vehicle, table)
            vehicle_class = property_dict["vehicle_class"]
            vehicle_length = property_dict["vehicle_length"]
            vehicle_width = property_dict["vehicle_width"]
            x, y, orientation = utils.SearchTrafficParticipantTiming(cursor, vehicle, timestamp, table)
            if vehicle not in vehicle_in_graph:
                if (vehicle_width != None and vehicle_length != None):
                    rect = matplotlib.patches.Polygon(
                        polygon_xy_from_motionstate(x, y, orientation, vehicle_width, vehicle_length),
                        closed=True,
                        zorder=20,
                        facecolor=color_list[random.randint(0, 3)],
                        edgecolor="black")
                    axes.add_patch(rect)
                    patch_dict[vehicle] = rect
                    text_dict[vehicle] = axes.text(x + 1, y + 1, str(vehicle), horizontalalignment='center', zorder=25)
                    vehicle_in_graph.append(vehicle)
                else:
                    rect = matplotlib.patches.Polygon(
                        polygon_xy_from_motionstate(x, y, orientation, 1, 1),
                        closed=True,
                        zorder=20,
                        facecolor="red",
                        edgecolor="black")
                    axes.add_patch(rect)
                    patch_dict[vehicle] = rect
                    text_dict[vehicle] = axes.text(x + 1, y + 1, str(vehicle), horizontalalignment='center', zorder=25)
                    vehicle_in_graph.append(vehicle)
            else:
                if (vehicle_width != None and vehicle_length != None):
                    patch_dict[vehicle].set_xy(
                        polygon_xy_from_motionstate(x, y, orientation, vehicle_width, vehicle_length))
                    text_dict[vehicle].set_position((x + 1, y + 1))
                else:
                    patch_dict[vehicle].set_xy(
                        polygon_xy_from_motionstate(x, y, orientation, 1, 1))
                    text_dict[vehicle].set_position((x + 1, y + 1))

        remove_list = list()
        for vehicle_plot in vehicle_in_graph:
            if vehicle_plot not in vehicle_list:
                patch_dict[vehicle_plot].remove()
                text_dict[vehicle_plot].remove()
                remove_list.append(vehicle_plot)
        for remove_vehicle_plot in remove_list:
            vehicle_in_graph.remove(remove_vehicle_plot)

        if DB=="NGSIM_I80_Scenario_DB":
            timeStamp = float(timestamp / 1000)  # ms timestamp to s
            timeArray = time.localtime(timeStamp)  # float to timestamp
            ntCtime_str = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)  # Y-M-D str
            plt.title("Dataset Visualization TimeStamp: %s " %(ntCtime_str))
        elif DB=="Argoverse_MIA_Scenario_DB":
            plt.title("Dataset Visualization TimeStamp: %s " % float(int(timestamp) / 1e+3))
        else:
            plt.title("Dataset Visualization Time: %s s" % round(timestamp / 1000, 3))
        fig.set_size_inches(10.8, 9.2)
        plt.pause(0.001)

    plt.ioff()
    plt.show()


if __name__=='__main__':
    # vehicle_visualize("NGSIM_I_80_Scenario_DB", "_1") #, begin=1113433372100, end=1113433375100)
    vehicle_visualize("Interaction_MergingZS_Testing_Scenario_DB", "_0")
    # vehicle_visualize("HighD_I_Scenario_DB", "_1", begin=185240, end=185240)
    # vehicle_visualize("Interaction_Intersection_EP0_Scenario_DB", "_0")
    # vehicle_visualize("InD_I_Scenario_DB", "_1", begin=880000, end=920000)
    # vehicle_visualize("Argoverse_MIA_Scenario_DB", "_64987")