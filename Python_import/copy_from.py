import psycopg2
import time
import csv

"""
使用copy from导入，size是从文件中读数据的缓冲区的大小，size越大load越快，默认8192
"""


def count_row(path):
    cnt = 0
    with open(path, 'r') as f:
        reader = csv.reader(f)
        for _ in reader:
            cnt = cnt + 1
    return cnt


def copy_from(cur, path, table, size=8192):
    f = open(path, 'r')
    t1 = time.perf_counter()
    cur.copy_from(f, table, sep=',', null="", size=size)
    t2 = time.perf_counter()
    cnt = count_row(path)
    print(str(cnt) + " records successfully loaded")
    print(table + " loading speed : " + str(round(cnt / (t2 - t1))) + " records/s\n")


if __name__ == '__main__':
    conn = psycopg2.connect("host=localhost dbname=cs307_2 user=checker password=123456")
    cur = conn.cursor()
    cur.execute(
        """truncate table model_class,contract,"order",client,header,product_model,product_class,sales,supply cascade;""")
    path_pre = 'D:\SUSTech\课程\大二下\数据库原理\Project1\CS307Database\sustc_tables'
    size = 8192
    copy_from(cur, path_pre + '\header.csv', 'header', size)
    copy_from(cur, path_pre + '\sales.csv', 'sales', size)
    copy_from(cur, path_pre + '\product_model.csv', 'product_model', size)
    copy_from(cur, path_pre + '\product_class.csv', 'product_class', size)
    copy_from(cur, path_pre + '\supply.csv', 'supply', size)
    copy_from(cur, path_pre + '\client.csv', 'client', size)
    copy_from(cur, path_pre + '\order.csv', 'order', size)
    copy_from(cur, path_pre + '\model_class.csv', 'model_class', size)
    copy_from(cur, path_pre + '\contract.csv', 'contract', size)
    conn.commit()
