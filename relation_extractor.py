from enum import Enum
import utils
import itertools
from init_db import init_DB

# HighD HighWay Parameter
# THRESH_NEAR_COLL = 2
# THRESH_VERY_NEAR = 5
# THRESH_NEAR = 7
# THRESH_VISIBLE = 10
# THRESH_LANE_VISIBLE = 8
# THRESH_LANE_NEAR = 5
# THRESH_LANE_VERY_NEAR = 3

# Interaction Merging Parameter
# annotation: 5 4 3 2  modify1: 4 3 2 1
THRESH_NEAR_COLL = 2
THRESH_VERY_NEAR = 3
THRESH_NEAR = 4
THRESH_VISIBLE = 5
THRESH_LANE_VISIBLE = 8
THRESH_LANE_NEAR = 5
THRESH_LANE_VERY_NEAR = 3

# Interaction Intersection Parameter
# THRESH_NEAR_COLL = 2
# THRESH_VERY_NEAR = 4
# THRESH_NEAR =6
# THRESH_VISIBLE = 8
# THRESH_LANE_VISIBLE = 3
# THRESH_LANE_NEAR = 2
# THRESH_LANE_VERY_NEAR = 1

# NGSIM I80 HighWay Parameter
# THRESH_NEAR_COLL = 2
# THRESH_VERY_NEAR = 5
# THRESH_NEAR = 7
# THRESH_VISIBLE = 10
# THRESH_LANE_VISIBLE = 8
# THRESH_LANE_NEAR = 5
# THRESH_LANE_VERY_NEAR = 3

# Argoverse Parameter
# THRESH_NEAR_COLL = 2
# THRESH_VERY_NEAR = 4
# THRESH_NEAR = 6
# THRESH_VISIBLE = 8
# THRESH_LANE_VISIBLE = 3
# THRESH_LANE_NEAR = 2
# THRESH_LANE_VERY_NEAR = 1


class Relations(Enum):
    near_coll = 0
    very_near = 1
    near = 2
    visible = 3
    front = 4
    front_left = 5
    front_right = 6
    rear = 7
    rear_left = 8
    rear_right = 9
    is_on = 10
    adjacent_left = 11
    adjacent_right = 12


# RELATION_COLORS = ["black", "red", "orange", "yellow", "green", "purple", "blue",
#                 "sienna", "pink", "turquoise", "violet", "lightblue"]

RELATION_COLORS = ["black", "red", "orange", "yellow", "green", "purple", "blue",
                "sienna", "pink", "turquoise","violet","lightblue","chocolate"]

NODE_COLORS = ['red','blue','green',"yellow"]

NODE_NTYPE = ["ego","vehicle","node","lane"]


class RelationExtractor:
    def __init__(self, cursor, ego_node, table):
        self.ego_node = ego_node
        self.cursor = cursor
        self.table = table
        self.ego_property = dict()
        self.ego_location = list()
        self.relation_V2V_list = list()
        self.relation_V2N_list = list()
        self.relation_V2L_list = list()
        self.relation_L2L_list = list()
        self.relation_vehicle = list()
        self.relation_node = list()
        self.relation_lane = list()
        self.get_ego_property()


    def get_ego_property(self):
        self.ego_property = utils.SearchTrafficParticipantProperty(self.cursor, self.ego_node, self.table)
        return


    def clear_V2V_relation(self):
        self.relation_V2V_list.clear()

    def clear_V2N_relation(self):
        self.relation_V2N_list.clear()

    def clear_V2L_relation(self):
        self.relation_V2L_list.clear()

    def clear_L2L_relation(self):
        self.relation_L2L_list.clear()


    def get_vehicle_relation(self, timestamp):
        self.relation_vehicle.clear()
        local_x, local_y, orien = utils.SearchTrafficParticipantTiming(self.cursor, self.ego_node, timestamp, self.table)
        ego_lane = utils.SearchOnWhichLaneFromDB(self.cursor, self.ego_node, timestamp, self.table)
        self.ego_location = [local_x, local_y, orien]
        vehicle_list = utils.SearchEgoSameTimeVehicle(self.cursor, self.ego_node, timestamp, self.table)
        for vehicle in vehicle_list:
            # print(vehicle)
            x, y ,orientation = utils.SearchTrafficParticipantTiming(self.cursor, vehicle, timestamp, self.table)
            vehicle_location = [x, y, orientation]
            vehicle_property = utils.SearchTrafficParticipantProperty(self.cursor, vehicle, self.table)
            other_lane = utils.SearchOnWhichLaneFromDB(self.cursor, vehicle, timestamp, self.table)
            if vehicle_property["vehicle_length"] == None and vehicle_property["vehicle_width"] == None:
                direction, min_dist, same_direction = utils.min_distance_between_vehicles_no_shape(self.ego_location, vehicle_location)
            else:
                direction, min_dist, same_direction = utils.min_distance_between_vehicles(self.ego_location, self.ego_property, vehicle_location, vehicle_property)
            if direction[:5]=="front" and ego_lane == other_lane and ego_lane!=None and other_lane!=None:
                direction = "front"
            if direction[:4]=="rear" and ego_lane == other_lane and ego_lane!=None and other_lane!=None:
                direction = "rear"
            if self.judge_add_vehicle_relation(self.ego_node, vehicle, direction, min_dist, same_direction):
                self.relation_vehicle.append(vehicle)
        return


    def get_node_relation(self, timestamp):
        self.relation_node.clear()
        local_x, local_y, orien = utils.SearchTrafficParticipantTiming(self.cursor, self.ego_node, timestamp, self.table)
        if local_x==None and local_y==None:
            return
        self.ego_location = [local_x, local_y, orien]
        node_list_dashed, node_x_list_dashed, node_y_list_dashed = utils.SearchLineThinDashedNodeFromDB(self.cursor)
        node_list_virtual, node_x_list_virtual, node_y_list_virtual = utils.SearchVirtualNodeFromDB(self.cursor)
        node_list = node_list_dashed + node_list_virtual
        node_x_list = node_x_list_dashed + node_x_list_virtual
        node_y_list = node_y_list_dashed + node_y_list_virtual
        for idx in range(len(node_list)):
            node_id = node_list[idx]
            node_x = node_x_list[idx]
            node_y = node_y_list[idx]
            if self.ego_property["vehicle_length"] == None and self.ego_property["vehicle_width"] == None:
                direction, min_dist = utils.min_distance_direction_between_car_node_no_shape(self.ego_location, node_x, node_y)
            else:
                direction, min_dist = utils.min_distance_direction_between_car_node(self.ego_location, self.ego_property, node_x, node_y)
            if self.judge_add_node_relation(self.ego_node, node_id, direction, min_dist):
                self.relation_node.append(node_id)
        return


    def get_vehicle_lane_relation(self, timestamp):
        self.relation_lane.clear()
        ego_lane = utils.SearchOnWhichLaneFromDB(self.cursor, self.ego_node, timestamp, self.table)
        if ego_lane:
            self.add_car_lane_relation(self.ego_node, ego_lane)
            self.relation_lane.append(ego_lane)
        if len(self.relation_vehicle) != 0:
            for rel_vehicle in self.relation_vehicle:
                vehicle_lane = utils.SearchOnWhichLaneFromDB(self.cursor, rel_vehicle, timestamp, self.table)
                # print(vehicle_lane, rel_vehicle)
                if vehicle_lane!=None:
                    self.add_car_lane_relation(rel_vehicle, vehicle_lane)
                if vehicle_lane!=None and vehicle_lane not in self.relation_lane:
                    self.relation_lane.append(vehicle_lane)


    def get_lane_lane_relation(self):
        # print(self.relation_lane)
        if len(self.relation_lane)!=0:
            for rel_lane in self.relation_lane:
                l_adj, r_adj = utils.SearchAdjacentLaneFromDB(self.cursor, rel_lane)
                if(l_adj != None and l_adj in self.relation_lane):
                    self.add_lane_lane_relation(rel_lane, l_adj, Relations.adjacent_left)
                if(r_adj != None and r_adj in self.relation_lane):
                    self.add_lane_lane_relation(rel_lane, r_adj, Relations.adjacent_right)


    def get_vehicle_node_relation(self, timestamp):
        # print(self.relation_lane)
        if len(self.relation_vehicle)!=0 and len(self.relation_node)!=0:
            for rel_vehicle in self.relation_vehicle:
                local_x, local_y, orien = utils.SearchTrafficParticipantTiming(self.cursor, rel_vehicle, timestamp, self.table)
                rel_vehicle_location = [local_x, local_y, orien]
                rel_vehicle_property = utils.SearchTrafficParticipantProperty(self.cursor, rel_vehicle, self.table)
                for rel_lane in self.relation_node:
                    node_x, node_y = utils.SearchNodeLocationFromDB(self.cursor, rel_lane)
                    if self.ego_property["vehicle_length"] == None and self.ego_property["vehicle_width"] == None:
                        direction, min_dist = utils.min_distance_direction_between_car_node_no_shape(rel_vehicle_location, node_x, node_y)
                    else:
                        direction, min_dist = utils.min_distance_direction_between_car_node(rel_vehicle_location, rel_vehicle_property, node_x, node_y)
                    self.judge_add_node_relation(rel_vehicle, rel_lane, direction, min_dist)
        return


    def get_vehicle_vehicle_relation(self, timestamp):
        if len(self.relation_vehicle)!=0:
            for vehicle_conbine_tuple in list(itertools.combinations(self.relation_vehicle, 2)):
                subject_vehicle = vehicle_conbine_tuple[0]
                object_vehicle = vehicle_conbine_tuple[1]
                subject_lane = utils.SearchOnWhichLaneFromDB(self.cursor, subject_vehicle, timestamp, self.table)
                object_lane = utils.SearchOnWhichLaneFromDB(self.cursor, object_vehicle, timestamp, self.table)
                local_x1, local_y1, orien1 = utils.SearchTrafficParticipantTiming(self.cursor, subject_vehicle,
                                                                                  timestamp, self.table)
                subject_vehicle_location = [local_x1, local_y1, orien1]
                subject_vehicle_property = utils.SearchTrafficParticipantProperty(self.cursor, subject_vehicle, self.table)
                local_x2, local_y2, orien2 = utils.SearchTrafficParticipantTiming(self.cursor, object_vehicle,
                                                                                  timestamp, self.table)
                object_vehicle_location = [local_x2, local_y2, orien2]
                object_vehicle_property = utils.SearchTrafficParticipantProperty(self.cursor, object_vehicle, self.table)
                if self.ego_property["vehicle_length"] == None and self.ego_property["vehicle_width"] == None:
                    direction, min_dist, same_direction = utils.min_distance_between_vehicles_no_shape(
                        subject_vehicle_location, object_vehicle_location)
                else:
                    direction, min_dist, same_direction = utils.min_distance_between_vehicles(subject_vehicle_location, subject_vehicle_property, object_vehicle_location, object_vehicle_property)
                if direction[:5] == "front" and subject_lane != None and object_lane != None and subject_lane == object_lane:
                    direction = "front"
                if direction[:4] == "rear" and subject_lane != None and object_lane != None and subject_lane == object_lane:
                    direction = "rear"
                self.judge_add_vehicle_relation(subject_vehicle, object_vehicle, direction, min_dist, same_direction)
        return


    def judge_add_vehicle_relation(self, subject, object, direction, min_dist, same_direction):
        if direction==None:
            return False
        if min_dist > THRESH_VISIBLE:
            return False
        # if extract Interaction_IntersectionEP or Argoverse,need to consider not same direction as the right-of-way is not explicit
        if not same_direction:
            return False
        if min_dist <= THRESH_VISIBLE and min_dist > THRESH_NEAR:
            self.relation_V2V_list.append([subject, Relations.visible, object])
            self.relation_V2V_list.append([object, Relations.visible, subject])
        elif min_dist <= THRESH_NEAR and min_dist > THRESH_VERY_NEAR:
            self.relation_V2V_list.append([subject, Relations.near, object])
            self.relation_V2V_list.append([object, Relations.near, subject])
        elif min_dist <= THRESH_VERY_NEAR and min_dist > THRESH_NEAR_COLL:
            self.relation_V2V_list.append([subject, Relations.very_near, object])
            self.relation_V2V_list.append([object, Relations.very_near, subject])
        elif min_dist <= THRESH_NEAR_COLL:
            self.relation_V2V_list.append([subject, Relations.near_coll, object])
            self.relation_V2V_list.append([object, Relations.near_coll, subject])

        if direction == "front":
            self.relation_V2V_list.append([subject, Relations.front, object])
            if same_direction:
                self.relation_V2V_list.append([object, Relations.rear, subject])
            else:
                self.relation_V2V_list.append([object, Relations.front, subject])
        elif direction == "front_left":
            self.relation_V2V_list.append([subject, Relations.front_left, object])
            if same_direction:
                self.relation_V2V_list.append([object, Relations.rear_right, subject])
            else:
                self.relation_V2V_list.append([object, Relations.front_left, subject])
        elif direction == "front_right":
            self.relation_V2V_list.append([subject, Relations.front_right, object])
            if same_direction:
                self.relation_V2V_list.append([object, Relations.rear_left, subject])
            else:
                self.relation_V2V_list.append([object, Relations.front_right, subject])
        elif direction == "rear":
            self.relation_V2V_list.append([subject, Relations.rear, object])
            if same_direction:
                self.relation_V2V_list.append([object, Relations.front, subject])
            else:
                self.relation_V2V_list.append([object, Relations.rear, subject])
        elif direction == "rear_left":
            self.relation_V2V_list.append([subject, Relations.rear_left, object])
            if same_direction:
                self.relation_V2V_list.append([object, Relations.front_right, subject])
            else:
                self.relation_V2V_list.append([object, Relations.rear_left, subject])
        elif direction == "rear_right":
            self.relation_V2V_list.append([subject, Relations.rear_right, object])
            if same_direction:
                self.relation_V2V_list.append([object, Relations.front_left, subject])
            else:
                self.relation_V2V_list.append([object, Relations.rear_right, subject])
        return True


    def judge_add_node_relation(self, subject, object, direction, min_dist):
        if direction==None:
            return False
        if min_dist > THRESH_LANE_VISIBLE:
            return False
        if min_dist <= THRESH_LANE_VISIBLE and min_dist > THRESH_LANE_NEAR:
            self.relation_V2N_list.append([subject, Relations.visible, object])
            self.relation_V2N_list.append([object, Relations.visible, subject])
        elif min_dist <= THRESH_LANE_NEAR and min_dist > THRESH_LANE_VERY_NEAR:
            self.relation_V2N_list.append([subject, Relations.near, object])
            self.relation_V2N_list.append([object, Relations.near, subject])
        elif min_dist <= THRESH_LANE_VERY_NEAR:
            self.relation_V2N_list.append([subject, Relations.very_near, object])
            self.relation_V2N_list.append([object, Relations.very_near, subject])
        # elif min_dist <= THRESH_NEAR_COLL:
        #     self.relation_list.append([subject, Relations.near_coll, object])
        #     self.relation_list.append([object, Relations.near_coll, subject])

        if direction == "front":
            self.relation_V2N_list.append([subject, Relations.front, object])
            self.relation_V2N_list.append([object, Relations.rear, subject])
        elif direction == "front_left":
            self.relation_V2N_list.append([subject, Relations.front_left, object])
            self.relation_V2N_list.append([object, Relations.rear_right, subject])
        elif direction == "front_right":
            self.relation_V2N_list.append([subject, Relations.front_right, object])
            self.relation_V2N_list.append([object, Relations.rear_left, subject])
        elif direction == "rear":
            self.relation_V2N_list.append([subject, Relations.rear, object])
            self.relation_V2N_list.append([object, Relations.front, subject])
        elif direction == "rear_left":
            self.relation_V2N_list.append([subject, Relations.rear_left, object])
            self.relation_V2N_list.append([object, Relations.front_right, subject])
        elif direction == "rear_right":
            self.relation_V2N_list.append([subject, Relations.rear_right, object])
            self.relation_V2N_list.append([object, Relations.front_left, subject])
        return True


    def add_car_lane_relation(self, subject, object):
        self.relation_V2L_list.append([subject, Relations.is_on, object])


    def add_lane_lane_relation(self, subject, object, relation):
        self.relation_L2L_list.append([subject, relation, object])


if __name__=='__main__':
    cursor = init_DB("HighD_Scenario_DB")
    table = "_1"
    r = RelationExtractor(cursor, 1, table)
    # timestamp = 315972619604
    timestamp = 40
    r.get_vehicle_relation(timestamp)
    r.get_node_relation(timestamp)
    r.get_vehicle_node_relation(timestamp)
    r.get_vehicle_vehicle_relation(timestamp)
    r.get_vehicle_lane_relation(timestamp)
    r.get_lane_lane_relation()
    print(r.relation_V2V_list)
    print(r.relation_V2N_list)
    print(r.relation_V2L_list)
    print(r.relation_L2L_list)
    # print(len(r.relation_list))
    # r.clear_relation()
    # print(r.relation_list)