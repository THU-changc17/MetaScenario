import pymysql

def CreateDB(DB):
    conn = pymysql.connect(host='localhost', user="root", passwd="123456")
    # 获取游标
    cursor = conn.cursor()
    print(cursor)
    cursor.execute(
        'CREATE DATABASE IF NOT EXISTS %s DEFAULT CHARSET utf8 COLLATE utf8_general_ci;'%(DB))
    cursor.close()  # 先关闭游标
    conn.close()  # 再关闭数据库连接
    print('创建TrafficScenarioDB数据库成功')

CreateDB("HighD_I_Scenario_DB")