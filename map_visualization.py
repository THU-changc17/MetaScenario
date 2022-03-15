import matplotlib
import matplotlib.axes
import matplotlib.pyplot as plt
import DBtools.utils as utils
from DBtools.init_db import init_DB


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

    axes.set_aspect('equal', adjustable='box')
    axes.set_xlim([min_x - 10, max_x + 10])
    axes.set_ylim([min_y - 10, max_y + 10])


def draw_map(cursor, axes):
    assert isinstance(axes, matplotlib.axes.Axes)

    axes.set_aspect('equal', adjustable='box')
    axes.patch.set_facecolor('white')
    type_dict = dict()
    _, Xlist, Ylist = utils.SearchNodeIDFromDB(cursor)
    set_visible_area(Xlist, Ylist, axes)
    way_list = utils.SearchWayFromDB(cursor)
    for way_id in way_list:
        node_list, x_list, y_list = utils.SearchNodeIDOnWayFromDB(cursor, way_id = way_id)
        way_type, way_subtype = utils.SearchWayTypeFromDB(cursor, way_id = way_id)

        if way_type == "curbstone":
            type_dict = dict(color="black", linewidth=2) #zorder=10)
        elif way_type == "line_thin":
            if way_subtype == "dashed":
                type_dict = dict(color="darkgreen", linewidth=1,  dashes=[10, 10])
            else:
                type_dict = dict(color="darkgreen", linewidth=1)
        elif way_type == "virtual":
            type_dict = dict(color="black", linewidth=1, dashes=[2, 5])
        elif way_type == "wall" or way_type == "fence":
            continue
        else:
            type_dict = dict(color="darkgreen", linewidth=1, dashes=[10, 10])

        channelization = utils.SearchChannelizationOnWayFromDB(cursor, way_id = way_id)
        for key in channelization:
            if key == "pedestrian_marking":
                type_dict = dict(color="blue", linewidth=1, dashes=[5, 10])
            elif key == "stop_line":
                type_dict = dict(color="gray", linewidth=3)
            elif key == "road_border":
                type_dict = dict(color="black", linewidth=1)
            elif key == "lane_change":
                type_dict = dict(color="black", linewidth=1)
            elif key == "guard_rail":
                type_dict = dict(color="black", linewidth=2)
            elif key == "turn_direction":
                type_dict = dict(color="blue", linewidth=2)
                break
            elif key == "is_intersection":
                type_dict = dict(color="black", linewidth=2)
                break
        plt.plot(x_list, y_list, **type_dict)
        for idx in range(len(x_list)):
            plt.plot(x_list[idx], y_list[idx], '.y', markersize=1.)
            axes.text(x_list[idx], y_list[idx], way_id , fontsize=8, color="b")



def draw_map_part(cursor, axes, Xlist, Ylist):
    assert isinstance(axes, matplotlib.axes.Axes)

    axes.set_aspect('equal', adjustable='box')
    axes.patch.set_facecolor('white')
    type_dict = dict()
    set_visible_area(Xlist, Ylist, axes)

    way_list = utils.SearchWayFromDB(cursor)
    for way_id in way_list:
        node_list, x_list, y_list = utils.SearchNodeIDOnWayFromDB(cursor, way_id=way_id, x_range=Xlist, y_range=Ylist)
        way_type, way_subtype = utils.SearchWayTypeFromDB(cursor, way_id=way_id)

        if way_type == "curbstone":
            type_dict = dict(color="black", linewidth=2)
        elif way_type == "line_thin":
            if way_subtype == "dashed":
                type_dict = dict(color="darkgreen", linewidth=2, dashes=[10, 10])
            else:
                type_dict = dict(color="darkgreen", linewidth=2)
        elif way_type == "virtual":
            type_dict = dict(color="black", linewidth=1, dashes=[2, 5])
        elif way_type == "wall" or way_type == "fence":
            continue
        else:
            type_dict = dict(color="darkgreen", linewidth=1, dashes=[10, 10])

        channelization = utils.SearchChannelizationOnWayFromDB(cursor, way_id=way_id)

        for key in channelization:
            if key == "pedestrian_marking":
                type_dict = dict(color="blue", linewidth=1, dashes=[5, 10])
            elif key == "stop_line":
                type_dict = dict(color="gray", linewidth=2)
            elif key == "road_border":
                type_dict = dict(color="black", linewidth=2)
            elif key == "guard_rail":
                type_dict = dict(color="black", linewidth=2)
            elif key == "turn_direction":
                type_dict = dict(color="blue", linewidth=2)
                break
            elif key == "is_intersection":
                type_dict = dict(color="blue", linewidth=2)
                break
        plt.plot(x_list, y_list, **type_dict)
        for idx in range(len(x_list)):
            plt.plot(x_list[idx], y_list[idx], '.y', markersize=1.)
            # axes.text(x_list[idx], y_list[idx], node_list[idx], fontsize=5, color="b")


if __name__ == '__main__':
    conn, cursor = init_DB("Interaction_MergingZS_Test_Scenario_DB")
    fig, axes = plt.subplots(1, 1)
    fig.canvas.set_window_title("DatasetMap Visualization")
    draw_map(cursor, axes)
    # draw_map_part(cursor, axes, Xlist=[1000, 1040], Ylist=[950, 1000])
    # draw_map_part(cursor, axes, Xlist=[400,440],Ylist=[1010,1050])
    fig.set_size_inches(19.2, 10.8)
    # plt.savefig("Merging_Map.eps")
    plt.show()