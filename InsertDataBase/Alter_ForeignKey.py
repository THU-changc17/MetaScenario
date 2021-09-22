import pymysql

conn = pymysql.connect(
    host='localhost',
    user="root",
    passwd="123456",
    db="HighD_I_Scenario_DB")
cursor = conn.cursor()

table = "_3"

AlterForeignKey = 'ALTER TABLE Traffic_timing_state' + table + ' ADD FOREIGN KEY (vehicle_id) REFERENCES Traffic_participant_property' + table + '(vehicle_id)'
cursor.execute(AlterForeignKey)
AlterForeignKey = 'ALTER TABLE Traffic_timing_state' + table + ' ADD FOREIGN KEY (preced_vehicle) REFERENCES Traffic_participant_property' + table + '(vehicle_id)'
cursor.execute(AlterForeignKey)
AlterForeignKey = 'ALTER TABLE Traffic_timing_state' + table + ' ADD FOREIGN KEY (follow_vehicle) REFERENCES Traffic_participant_property' + table + '(vehicle_id)'
cursor.execute(AlterForeignKey)

AlterForeignKey = 'ALTER TABLE Traffic_timing_state' + table + ' ADD FOREIGN KEY (lane_id) REFERENCES Lane_Meta(lane_id)'
cursor.execute(AlterForeignKey)
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

cursor.close()
conn.commit()
conn.close()