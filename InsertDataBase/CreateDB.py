import pymysql

def CreateDB(DB):
    conn = pymysql.connect(host='localhost', user="root", passwd="123456")
    cursor = conn.cursor()
    print(cursor)
    cursor.execute(
        'CREATE DATABASE IF NOT EXISTS %s DEFAULT CHARSET utf8 COLLATE utf8_general_ci;'%(DB))
    cursor.close()
    conn.close()
    print('Create TrafficScenarioDB successfully')

CreateDB("InD_I_Scenario_DB")