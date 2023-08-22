import pymysql
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--DB', type=str, default=None)
    args = parser.parse_args()
    conn = pymysql.connect(host='localhost', user="root", passwd="123456")
    cursor = conn.cursor()
    print(cursor)
    cursor.execute(
        'CREATE DATABASE IF NOT EXISTS %s DEFAULT CHARSET utf8 COLLATE utf8_general_ci;'%(args.DB))
    cursor.close()
    conn.close()
    print('Create TrafficScenarioDB successfully')