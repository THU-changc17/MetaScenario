import pymysql

def CreateWayInfoTable(cursor):
    cursor.execute('drop table if exists Way_Info')
    WayInfoTable = """CREATE TABLE IF NOT EXISTS `Way_Info` (
        	  `way_id` bigint NOT NULL,
        	  `way_type` json,
        	  `road_channelization` json,
        	  `dynamic_facility` json,
        	  `static_facility` json,
        	  `l_border_of` varchar(32),
        	  `r_border_of` varchar(32),
        	  `center_line_of` varchar(32),
        	  `l_neighbor_id` bigint,
        	  `r_neighbor_id` bigint,
        	  `predecessor` bigint,
        	  `successor` bigint,
        	  PRIMARY KEY (`way_id`)
        	) ENGINE=InnoDB  DEFAULT CHARSET=utf8"""
    cursor.execute(WayInfoTable)


def CreateAdditionalConditionTable(cursor):
    cursor.execute('drop table if exists Additional_Condition')
    WayInfoTable = """CREATE TABLE IF NOT EXISTS `Additional_Condition` (
        	  `weather` varchar(32),
        	  `lighting` varchar(32),
        	  `temperature` varchar(32),
        	  `humidity` varchar(32)
        	) ENGINE=InnoDB  DEFAULT CHARSET=utf8"""
    cursor.execute(WayInfoTable)


def CreateNodeInfoTable(cursor):
    cursor.execute('drop table if exists Node_Info')
    NodeInfoTable = """CREATE TABLE IF NOT EXISTS `Node_Info` (
            	  `node_id` bigint NOT NULL,
            	  `local_x` decimal(11,3),
    	          `local_y` decimal(11,3),
    	          `global_x` decimal(11,3),
    	          `global_y` decimal(11,3),
            	  PRIMARY KEY (`node_id`)
            	) ENGINE=InnoDB  DEFAULT CHARSET=utf8"""
    cursor.execute(NodeInfoTable)


def CreateNodeToWayTable(cursor):
    cursor.execute('drop table if exists Node_To_Way')
    NodeToWayTable = """CREATE TABLE IF NOT EXISTS `Node_To_Way` (
                      `id` bigint NOT NULL AUTO_INCREMENT,
                	  `way_id` bigint NOT NULL,
                	  `node_id` bigint NOT NULL,
                	  PRIMARY KEY (`id`)
                	) ENGINE=InnoDB  DEFAULT CHARSET=utf8 AUTO_INCREMENT=0"""
    cursor.execute(NodeToWayTable)


def CreateLaneMetaTable(cursor):
    cursor.execute('drop table if exists Lane_Meta')
    LaneMetaTable = """CREATE TABLE IF NOT EXISTS `Lane_Meta` (
                      `lane_id` varchar(32) NOT NULL,
                	  `lane_property` json,
                	  `l_adj_lane` varchar(32),
                	  `r_adj_lane` varchar(32),
                	  PRIMARY KEY (`lane_id`)
                	) ENGINE=InnoDB  DEFAULT CHARSET=utf8 AUTO_INCREMENT=0"""
    cursor.execute(LaneMetaTable)


def CreateTrafficTimingStateTable(cursor, table):
    cursor.execute('drop table if exists Traffic_timing_state' + table)
    TrafficTimingStateTable = """CREATE TABLE IF NOT EXISTS `Traffic_timing_state""" + table + """` (
    	  `data_id` bigint NOT NULL AUTO_INCREMENT,
    	  `time_stamp` bigint NOT NULL,
    	  `vehicle_id` bigint NOT NULL,
    	  `local_x` decimal(11,3),
    	  `local_y` decimal(11,3),
    	  `velocity_x` decimal(11,3),
    	  `velocity_y` decimal(11,3),
    	  `acceleration` decimal(11,3),
    	  `orientation` decimal(11,3),
    	  `lane_id` varchar(32),
    	  `preced_vehicle` bigint,
    	  `follow_vehicle` bigint,
    	  PRIMARY KEY (`data_id`)
    	) ENGINE=InnoDB  DEFAULT CHARSET=utf8 AUTO_INCREMENT=0"""
    cursor.execute(TrafficTimingStateTable)


def CreateTrafficParticipantPropertyTable(cursor, table):
    cursor.execute('drop table if exists Traffic_participant_property' + table)
    TrafficParticipantPropertyTable = """CREATE TABLE IF NOT EXISTS `Traffic_participant_property""" + table + """` (
        	  `vehicle_id` bigint NOT NULL,
        	  `vehicle_class` bigint NOT NULL,
        	  `vehicle_length` decimal(11,3),
        	  `vehicle_width` decimal(11,3),
        	  `vehicle_height` decimal(11,3),
        	  PRIMARY KEY (`vehicle_id`)
        	) ENGINE=InnoDB  DEFAULT CHARSET=utf8"""
    cursor.execute(TrafficParticipantPropertyTable)
