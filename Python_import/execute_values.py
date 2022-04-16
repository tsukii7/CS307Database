import csv
import psycopg2
import time
from psycopg2 import extras

"""
execute values实现了批量执行sql语句、预加载sql语句。page size越大，load越快，默认100
page_size – maximum number of argslist items to include in every statement. 
            If there are more items the function will execute more than one statement.
"""


def count_row(path):
    cnt = 0
    with open(path, 'r') as f:
        reader = csv.reader(f)
        for _ in reader:
            cnt = cnt + 1
    return cnt


def load_data(cur, path, table, page_size=100):
    list = []
    with open(path, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            one_record = []
            for item in row:
                if(item != ''):
                    one_record.append(item)
                else:
                    one_record.append(None)
            list.append(one_record)
    cnt = count_row(path)
    t1 = time.perf_counter()
    psycopg2.extras.execute_values(cur, "INSERT INTO " + table + " VALUES %s;", list, page_size=page_size)
    t2 = time.perf_counter()
    print(str(cnt) + " records successfully loaded")
    print(table + " loading speed : " + str(round(cnt / (t2 - t1))) + " records/s\n")


if __name__ == '__main__':
    conn = psycopg2.connect("host=localhost dbname=cs307_2 user=checker password=123456")
    cur = conn.cursor()
    cur.execute(
        """truncate table model_class,contract,"order",client,header,product_model,product_class,sales,supply cascade;""")
    path_pre = 'D:\SUSTech\课程\大二下\数据库原理\Project1\CS307Database\sustc_tables'
    page_size = 1000
    load_data(cur, path_pre + '\header.csv', 'header', page_size)
    load_data(cur, path_pre + '\sales.csv', 'sales', page_size)
    load_data(cur, path_pre + '\product_model.csv', 'product_model', page_size)
    load_data(cur, path_pre + '\product_class.csv', 'product_class', page_size)
    load_data(cur, path_pre + '\supply.csv', 'supply', page_size)
    load_data(cur, path_pre + '\client.csv', 'client', page_size)
    load_data(cur, path_pre + '\order.csv', '\"order\"', page_size)
    load_data(cur, path_pre + '\model_class.csv', 'model_class', page_size)
    load_data(cur, path_pre + '\contract.csv', 'contract', page_size)
    conn.commit()
