import matplotlib
import matplotlib.axes
import matplotlib.pyplot as plt
import DBtools.utils as utils
from DBtools.init_db import init_DB
import numpy as np

def set_visible_area(x_list, y_list, axes):
    min_x = 10e9
    min_y = 10e9
    max_x = -10e9
    max_y = -10e9

    for i in range(len(x_list)):
        min_x = min(float(x_list[i]), min_x)
        max_x = max(float(x_list[i]), max_x)
        min_y = min(float(y_list[i]), min_y)
        max_y = max(float(y_list[i]), max_y)

    axes.set_xlim([min_x, max_x])
    axes.set_ylim([min_y, max_y])
    axes.set_aspect('equal', adjustable='box')
    axes.patch.set_facecolor('white')


def draw_map_part(cursor, axes, Xlist, Ylist):
    assert isinstance(axes, matplotlib.axes.Axes)

    axes.set_aspect('equal', adjustable='box')
    axes.patch.set_facecolor('white')
    type_dict = dict()
    set_visible_area(Xlist, Ylist, axes)

    way_list = utils.SearchWayFromDB(cursor)
    print(way_list)
    for way_id in way_list:
        node_list, x_list, y_list = utils.SearchNodeIDOnWayFromDB(cursor, way_id=way_id, x_range=Xlist, y_range=Ylist)
        way_type, way_subtype = utils.SearchWayTypeFromDB(cursor, way_id=way_id)

        if way_type == "curbstone":
            type_dict = dict(color=(1.0, 0.8, 0.32), linewidth=3)
        elif way_type == "line_thin":
            if way_subtype == "dashed":
                type_dict = dict(color='#808A87', linewidth=1, dashes=[10, 10])
            else:
                type_dict = dict(color='#808A87', linewidth=1)
        elif way_type == "virtual":
            type_dict = dict(color='#A020F0', linewidth=1, dashes=[2, 5])
        else:
            type_dict = dict(color=(1.0, 0.8, 0.32), linewidth=1, dashes=[10, 10])

        channelization = utils.SearchChannelizationOnWayFromDB(cursor, way_id=way_id)

        for key in channelization:
            if key == "pedestrian_marking":
                type_dict = dict(color="#3D59AB", linewidth=2, dashes=[5, 10])
            elif key == "stop_line":
                type_dict = dict(color=(1.0, 0.8, 0.32), linewidth=3)
            elif key == "road_border":
                type_dict = dict(color=(1.0, 0.8, 0.32), linewidth=2)
            elif key == "guard_rail":
                type_dict = dict(color='#EB8E55', linewidth=2)
            elif key == "turn_direction":
                type_dict = dict(color=(1.0, 0.8, 0.32), linewidth=2)
                break
            elif key == "is_intersection":
                type_dict = dict(color='#DA70D6', linewidth=2)
                break
        plt.plot(x_list, y_list, **type_dict)
        for idx in range(len(x_list)):
            plt.plot(x_list[idx], y_list[idx])
            # axes.text(x_list[idx], y_list[idx], node_list[idx], fontsize=5, color="b")


def rotate_around_center(pts, center, yaw):
    return np.dot(pts - center, np.array([[np.cos(yaw), np.sin(yaw)], [-np.sin(yaw), np.cos(yaw)]])) + center


def polygon_xy_from_motionstate(x, y, psi_rad, width, length):
    lowleft = (x - length / 2., y - width / 2.)
    lowright = (x + length / 2., y - width / 2.)
    upright = (x + length / 2., y + width / 2.)
    upleft = (x - length / 2., y + width / 2.)
    return rotate_around_center(np.array([lowleft, lowright, upright, upleft]), np.array([x, y]), yaw=psi_rad)


def draw_vehicle_and_trajectory(cursor, axes, ego, timestamp, interval, table):
    local_x, local_y, orien = utils.SearchTrafficParticipantTiming(cursor, ego, timestamp, table)
    vehicle_list = utils.SearchEgoSameTimeVehicle(cursor, ego, timestamp, table)
    ego_property = utils.SearchTrafficParticipantProperty(cursor, ego, table)
    ego_location = [local_x, local_y, orien]
    vehicle_length = ego_property["vehicle_length"]
    vehicle_width = ego_property["vehicle_width"]
    rect = matplotlib.patches.Polygon(
        polygon_xy_from_motionstate(local_x, local_y, orien, vehicle_width, vehicle_length),
        closed=True,
        # zorder=20,
        facecolor="green",
        zorder=3)
    axes.add_patch(rect)
    ego_x_list = list()
    ego_y_list = list()
    for recordtime in range(timestamp, timestamp + interval * 4, interval):
        record_x, record_y, _ = utils.SearchTrafficParticipantTiming(cursor, ego, recordtime, table)
        if record_y == None or record_x == None:
            break
        ego_x_list.append(record_x)
        ego_y_list.append(record_y)
    plt.plot(ego_x_list, ego_y_list, color = 'red', linewidth = 3.0,zorder=4)
    # plt.plot(ego_x_list, ego_y_list, color='red', marker = "o", markersize=5.0)

    for vehicle in vehicle_list:
        x, y, orientation = utils.SearchTrafficParticipantTiming(cursor, vehicle, timestamp, table)
        vehicle_location = [x, y, orientation]
        vehicle_property = utils.SearchTrafficParticipantProperty(cursor, vehicle, table)
        _, min_dist, _ = utils.min_distance_between_vehicles(ego_location, ego_property,
                                                                                  vehicle_location, vehicle_property)
        if min_dist < 10:
            vehicle_length = vehicle_property["vehicle_length"]
            vehicle_width = vehicle_property["vehicle_width"]
            rect = matplotlib.patches.Polygon(
                polygon_xy_from_motionstate(x, y, orientation, vehicle_width, vehicle_length),
                closed=True,
                # zorder=20,
                facecolor="blue",
                zorder=3,)
            axes.add_patch(rect)
            x_list = list()
            y_list = list()
            for recordtime in range(timestamp, timestamp + interval * 4, interval):
                record_x, record_y, _ = utils.SearchTrafficParticipantTiming(cursor, vehicle, recordtime, table)
                if record_y == None or record_x == None:
                    break
                x_list.append(record_x)
                y_list.append(record_y)
            plt.plot(x_list, y_list, color='pink', linewidth=3.0,zorder=4)
            # plt.plot(x_list, y_list, color='pink', marker = "o", markersize=5.0)


    return local_x ,local_y


if __name__ == '__main__':
    conn, cursor = init_DB("Interaction_Intersection_EP0_Scenario_DB")
    fig, axes = plt.subplots(1, 1)
    axes.set_xticks([])
    axes.set_yticks([])
    plt.gca().xaxis.set_major_locator(plt.NullLocator())
    plt.gca().yaxis.set_major_locator(plt.NullLocator())
    plt.subplots_adjust(top=1, bottom=0, right=1, left=0, hspace=0, wspace=0)
    plt.margins(0, 0)
    fig.canvas.set_window_title("DatasetMap Visualization")
    x, y = draw_vehicle_and_trajectory(cursor, axes, 10, 1000, 1000, "_5")
    draw_map_part(cursor, axes, Xlist=[x - 30, x + 20], Ylist=[y - 20, y + 20])
    fig.set_size_inches(5, 4)
    plt.show()
    fig.savefig('../save_name.eps')

