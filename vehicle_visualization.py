import matplotlib
import matplotlib.axes
import matplotlib.patches
import matplotlib.pyplot as plt
import pymysql
import numpy as np
from map_visualization import draw_map
import random
import utils
from init_db import init_DB


def rotate_around_center(pts, center, yaw):
    return np.dot(pts - center, np.array([[np.cos(yaw), np.sin(yaw)], [-np.sin(yaw), np.cos(yaw)]])) + center


def polygon_xy_from_motionstate(x, y, psi_rad, width, length):
    lowleft = (x - length / 2., y - width / 2.)
    lowright = (x + length / 2., y - width / 2.)
    upright = (x + length / 2., y + width / 2.)
    upleft = (x - length / 2., y + width / 2.)
    return rotate_around_center(np.array([lowleft, lowright, upright, upleft]), np.array([x, y]), yaw=psi_rad)


if __name__=='__main__':
    cursor = init_DB("Interaction_Scenario_DB")
    maxstampsql = "select max(time_stamp) from traffic_timing_state"
    cursor.execute(maxstampsql)
    maxtimestamp = cursor.fetchall()[0][0]
    print(maxtimestamp)
    maxstampsqlvehicle_1 = "select max(time_stamp) from traffic_timing_state where vehicle_id = 1"
    cursor.execute(maxstampsqlvehicle_1)
    maxstampvehicle_1 = cursor.fetchall()[0][0]
    print(maxstampvehicle_1)
    delta_time = 100
    plottime = 100

    fig, axes = plt.subplots(1, 1)
    fig.canvas.set_window_title("Dataset Visualization")
    draw_map(cursor, axes)
    plt.ion()
    patch_dict = dict()
    text_dict = dict()
    vehicle_in_graph = list()
    color_list = ["whitesmoke", "oldlace", "lightgray", "aliceblue"]

    while plottime < maxtimestamp:
        vehicle_list = utils.SearchTrafficParticipantByTime(cursor, plottime)
        for vehicle in vehicle_list:
            property_dict = utils.SearchTrafficParticipantProperty(cursor, vehicle)
            vehicle_class = property_dict["vehicle_class"]
            vehicle_length = property_dict["vehicle_length"]
            vehicle_width = property_dict["vehicle_width"]
            x, y, orientation = utils.SearchTrafficParticipantTiming(cursor, vehicle, plottime)
            if vehicle not in vehicle_in_graph:
                rect = matplotlib.patches.Polygon(polygon_xy_from_motionstate(x, y, orientation, vehicle_width, vehicle_length),
                                                  closed=True,
                                                  zorder=20,
                                                  facecolor=color_list[random.randint(0,3)],
                                                  edgecolor="black")
                axes.add_patch(rect)
                patch_dict[vehicle] = rect
                text_dict[vehicle] = axes.text(x + 1, y + 1, str(vehicle), horizontalalignment='center', zorder=5)
                vehicle_in_graph.append(vehicle)
            else:
                patch_dict[vehicle].set_xy(polygon_xy_from_motionstate(x, y, orientation, vehicle_width, vehicle_length))
                text_dict[vehicle].set_position((x + 1, y + 1))

        remove_list = list()
        for vehicle_plot in vehicle_in_graph:
            if vehicle_plot not in vehicle_list:
                patch_dict[vehicle_plot].remove()
                text_dict[vehicle_plot].remove()
                remove_list.append(vehicle_plot)
        for remove_vehicle_plot in remove_list:
            vehicle_in_graph.remove(remove_vehicle_plot)

        plt.title("Dataset Visualization Time:%s s" %round(plottime/1000,3))
        fig.set_size_inches(19.2, 10.8)
        # if(plottime % 1000 == 0):
        #     plt.savefig('gif/%d.jpg' %(plottime/1000))
        # if(plottime % 10000 == 0):
        #     print(plottime)
        plottime = plottime + delta_time
        plt.pause(0.001)

    plt.ioff()
    # plt.show()