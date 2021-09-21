import pymysql
import csv

conn = pymysql.connect(
    host='localhost',
    user="root",
    passwd="123456",
    db="Argoverse_MIA_Scenario_DB")
# 获取游标
cursor = conn.cursor()

csv_reader = csv.reader(open("../Annotator/sample_record.csv", encoding='utf-8'))
for i, rows in enumerate(csv_reader):
    table = str(rows[0])
    print("table: ", table)
    table = "_" + table
# table = "_3"
# 设置外键
    AlterForeignKey = 'ALTER TABLE Traffic_timing_state' + table + ' ADD FOREIGN KEY (vehicle_id) REFERENCES Traffic_participant_property' + table + '(vehicle_id)'
    cursor.execute(AlterForeignKey)
    AlterForeignKey = 'ALTER TABLE Traffic_timing_state' + table + ' ADD FOREIGN KEY (preced_vehicle) REFERENCES Traffic_participant_property' + table + '(vehicle_id)'
    cursor.execute(AlterForeignKey)
    AlterForeignKey = 'ALTER TABLE Traffic_timing_state' + table + ' ADD FOREIGN KEY (follow_vehicle) REFERENCES Traffic_participant_property' + table + '(vehicle_id)'
    cursor.execute(AlterForeignKey)

    # 设置外键
    AlterForeignKey = 'ALTER TABLE Traffic_timing_state' + table + ' ADD FOREIGN KEY (lane_id) REFERENCES Lane_Meta(lane_id)'
    cursor.execute(AlterForeignKey)
# AlterForeignKey = 'ALTER TABLE Node_To_Way ADD FOREIGN KEY (way_id) REFERENCES Way_Info(way_id)'
# cursor.execute(AlterForeignKey)
# AlterForeignKey = 'ALTER TABLE Node_To_Way ADD FOREIGN KEY (node_id) REFERENCES Node_Info(node_id)'
# cursor.execute(AlterForeignKey)
#
# AlterForeignKey = '''ALTER TABLE Way_Info ADD FOREIGN KEY (l_border_of) REFERENCES Lane_Meta(lane_id)'''
# cursor.execute(AlterForeignKey)
# AlterForeignKey = '''ALTER TABLE Way_Info ADD FOREIGN KEY (r_border_of) REFERENCES Lane_Meta(lane_id)'''
# cursor.execute(AlterForeignKey)
# AlterForeignKey = '''ALTER TABLE Way_Info ADD FOREIGN KEY (center_line_of) REFERENCES Lane_Meta(lane_id)'''
# cursor.execute(AlterForeignKey)

cursor.close()
conn.commit()
conn.close()