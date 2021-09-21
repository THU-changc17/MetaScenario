set1 = {3,2}
list2 = list(set1)
print(list2)

from shapely import geometry
import math
import numpy as np
import functools


def if_inPoly(polygon, Points):
    line = geometry.LineString(polygon)
    point = geometry.Point(Points)
    polygon = geometry.Polygon(line)
    return polygon.contains(point)


polygon = np.array([[1,1,2], [3,3,1], [4,3,2], [2,1,1]])
polygon = np.hstack((polygon[:, 0:1].astype(np.int), polygon[:, 1:].astype(np.float)))
Points = [1.5,1.8]
print(if_inPoly(polygon, Points))


def AngleCompare(pointA, pointB):
    angleA = math.atan2(pointA[2], pointA[1])
    angleB = math.atan2(pointB[2], pointB[1])
    if angleA<=angleB:
        return 1
    else:
        return -1


def ClockwiseSort(polygon_point_list):
    center_x = np.mean(polygon_point_list[:, 1])
    center_y = np.mean(polygon_point_list[:, 2])
    polygon_point_list[:, 1] = polygon_point_list[:, 1] - center_x
    polygon_point_list[:, 2] = polygon_point_list[:, 2] - center_y
    polygon_point_list = np.array(sorted(polygon_point_list, key=functools.cmp_to_key(AngleCompare)))
    polygon_point_list[:, 1] = polygon_point_list[:, 1] + center_x
    polygon_point_list[:, 2] = polygon_point_list[:, 2] + center_y
    return polygon_point_list

print(ClockwiseSort(polygon))
