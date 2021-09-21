import pymysql

def init_DB(DB):
    conn = pymysql.connect(
        host='localhost',
        user="root",
        passwd="123456",
        db=DB)
    # 获取游标
    cursor = conn.cursor()
    return cursor