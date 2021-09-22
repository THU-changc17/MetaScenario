import pymysql

def init_DB(DB):
    conn = pymysql.connect(
        host='localhost',
        user="root",
        passwd="123456",
        db=DB)
    cursor = conn.cursor()
    return cursor