import pymysql
from DBtools.init_db import init_DB


def AlterMapForegnKey(cursor):
    AlterForeignKey = 'ALTER TABLE Node_To_Way ADD FOREIGN KEY (way_id) REFERENCES Way_Info(way_id)'
    cursor.execute(AlterForeignKey)
    AlterForeignKey = 'ALTER TABLE Node_To_Way ADD FOREIGN KEY (node_id) REFERENCES Node_Info(node_id)'
    cursor.execute(AlterForeignKey)

    AlterForeignKey = '''ALTER TABLE Way_Info ADD FOREIGN KEY (l_border_of) REFERENCES Lane_Meta(lane_id)'''
    cursor.execute(AlterForeignKey)
    AlterForeignKey = '''ALTER TABLE Way_Info ADD FOREIGN KEY (r_border_of) REFERENCES Lane_Meta(lane_id)'''
    cursor.execute(AlterForeignKey)
    AlterForeignKey = '''ALTER TABLE Way_Info ADD FOREIGN KEY (center_line_of) REFERENCES Lane_Meta(lane_id)'''
    cursor.execute(AlterForeignKey)


def AlterParticipantForegnKey(cursor, table):
    AlterForeignKey = 'ALTER TABLE Traffic_timing_state' + table + ' ADD FOREIGN KEY (vehicle_id) REFERENCES Traffic_participant_property' + table + '(vehicle_id)'
    cursor.execute(AlterForeignKey)
    AlterForeignKey = 'ALTER TABLE Traffic_timing_state' + table + ' ADD FOREIGN KEY (preced_vehicle) REFERENCES Traffic_participant_property' + table + '(vehicle_id)'
    cursor.execute(AlterForeignKey)
    AlterForeignKey = 'ALTER TABLE Traffic_timing_state' + table + ' ADD FOREIGN KEY (follow_vehicle) REFERENCES Traffic_participant_property' + table + '(vehicle_id)'
    cursor.execute(AlterForeignKey)

    AlterForeignKey = 'ALTER TABLE Traffic_timing_state' + table + ' ADD FOREIGN KEY (lane_id) REFERENCES Lane_Meta(lane_id)'
    cursor.execute(AlterForeignKey)


if __name__ == '__main__':
    conn, cursor = init_DB("NGSIM_I_80_Scenario_DB")
    table = "_3"
    print(table)
    # AlterMapForegnKey()
    # AlterParticipantForegnKey(table)
    cursor.close()
    conn.commit()
    conn.close()