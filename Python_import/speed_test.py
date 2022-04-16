"""
execute
"""
import csv
import psycopg2
import time
from psycopg2 import extras


def count_execute(num, cur):
    cnt = 0
    with open('../header.csv', 'r') as f:
        reader = csv.reader(f)
        for _ in reader:
            cnt = cnt + 1
    total = 0.0
    for i in range(0, num):
        cur.execute("TRUNCATE TABLE header CASCADE;")
        t1 = time.perf_counter()
        with open('../header.csv', 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                cur.execute("INSERT INTO header VALUES (%s, %s)", row)
        t2 = time.perf_counter()
        total += cnt / (t2 - t1)
    print(str(cnt) + " records successfully loaded")
    print("execute() : " + str(round(total / num)) + " records/s")
    return


"""
executemany
"""


def count_executemany(num, cur):
    cnt = 0
    list = []
    with open('../header.csv', 'r') as f:
        reader = csv.reader(f)
        for _ in reader:
            cnt = cnt + 1
    with open('../header.csv', 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            list.append([row[0], row[1]])
    total = 0.0
    for i in range(0, num):
        cur.execute("TRUNCATE TABLE header CASCADE;")
        t1 = time.perf_counter()
        cur.executemany("INSERT INTO header VALUES (%s, %s)", list)
        t2 = time.perf_counter()
        total += cnt / (t2 - t1)
    print(str(cnt) + " records successfully loaded")
    print("executemany() : " + str(round(total / num)) + " records/s")
    return


"""
execute_batch
"""


def count_batch(num, cur, page_size=100):
    cnt = 0
    list = []
    with open('../header.csv', 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            list.append([row[0], row[1]])
            cnt = cnt + 1
    total = 0.0
    for i in range(0, num):
        cur.execute("TRUNCATE TABLE header CASCADE;")
        t1 = time.perf_counter()
        extras.execute_batch(cur, "INSERT INTO header (contract_number, contract_date) VALUES (%s,%s)", list,
                             page_size=page_size)
        t2 = time.perf_counter()
        total += cnt / (t2 - t1)
    print(str(cnt) + " records successfully loaded")
    print("execute_batch() with page_size = " + str(page_size) + " : " + str(round(total / num)) + " records/s")
    return


"""
batch + iterator
"""


def count_batch_iterator_abnormal(num, cur, page_size=100):
    cnt = 0
    list = []
    with open('../header.csv', 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            list.append([row[0], row[1]])
            cnt = cnt + 1
    iter_list = iter(list)
    total = 0.0
    for i in range(0, num):
        cur.execute("TRUNCATE TABLE header CASCADE;")
        t1 = time.perf_counter()
        extras.execute_batch(cur, "INSERT INTO header (contract_number, contract_date) VALUES (%s,%s)", iter_list,
                             page_size=page_size)
        t2 = time.perf_counter()
        total += cnt / (t2 - t1)
        # print(str(round(cnt / (t2 - t1))))
    print(str(cnt) + " records successfully loaded")
    print("execute_batch() with iterator and page_size = " + str(page_size) + " : " + str(round(total / num)) + " records/s")
    return


def count_batch_iterator_normal(num, cur, page_size=100):
    cnt = 0
    list = []
    with open('../header.csv', 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            list.append([row[0], row[1]])
            cnt = cnt + 1
    total = 0.0
    for i in range(0, num):
        iter_list = iter(list)
        cur.execute("TRUNCATE TABLE header CASCADE;")
        t1 = time.perf_counter()
        extras.execute_batch(cur, "INSERT INTO header (contract_number, contract_date) VALUES (%s,%s)", iter_list,
                             page_size=page_size)
        t2 = time.perf_counter()
        total += cnt / (t2 - t1)
        # print(str(round(cnt / (t2 - t1))))
    print(str(cnt) + " records successfully loaded")
    print("execute_batch() with iterator and page_size = " + str(page_size) + " : " + str(round(total / num)) + " records/s")
    return


"""
value，可以用迭代器，可以用page size
"""


def count_execute_values(num, cur, page_size=100):
    cnt = 0
    list = []
    with open('/header.csv', 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            list.append([row[0], row[1]])
            cnt = cnt + 1
    total = 0.0
    for i in range(0, num):
        cur.execute("TRUNCATE TABLE header CASCADE;")
        t1 = time.perf_counter()
        psycopg2.extras.execute_values(cur, """INSERT INTO header VALUES %s;""", list, page_size=page_size)
        t2 = time.perf_counter()
        total += cnt / (t2 - t1)
    print(str(cnt) + " records successfully loaded")
    print("execute_values() with page_size = " + str(page_size) + " : " + str(round(total / num)) + " records/s")
    return

def count_execute_values_iterator_abnormal(num, cur, page_size=100):
    cnt = 0
    list = []
    with open('/header.csv', 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            list.append([row[0], row[1]])
            cnt = cnt + 1
    total = 0.0
    iter_list = iter(list)
    for i in range(0, num):
        cur.execute("TRUNCATE TABLE header CASCADE;")
        t1 = time.perf_counter()
        psycopg2.extras.execute_values(cur, """INSERT INTO header VALUES %s;""", iter_list, page_size=page_size)
        t2 = time.perf_counter()
        total += cnt / (t2 - t1)
    print(str(cnt) + " records successfully loaded")
    print("execute_values() with iterator and page_size = " + str(page_size) + " : " + str(round(total / num)) + " records/s")
    return

def count_execute_values_iterator_normal(num, cur, page_size=100):
    cnt = 0
    list = []
    with open('/header.csv', 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            list.append([row[0], row[1]])
            cnt = cnt + 1
    total = 0.0
    for i in range(0, num):
        iter_list = iter(list)
        cur.execute("TRUNCATE TABLE header CASCADE;")
        t1 = time.perf_counter()
        psycopg2.extras.execute_values(cur, """INSERT INTO header VALUES %s;""", iter_list, page_size=page_size)
        t2 = time.perf_counter()
        total += cnt / (t2 - t1)
    print(str(cnt) + " records successfully loaded")
    print("execute_values() with iterator and page_size = " + str(page_size) + " : " + str(round(total / num)) + " records/s")
    return

"""
copy from
"""


def count_copy_from(num, cur, size=8192):
    cnt = 0
    with open('/header.csv', 'r') as f:
        reader = csv.reader(f)
        for _ in reader:
            cnt = cnt + 1
    total = 0.0
    for i in range(0, num):
        f = open('/header.csv', 'r')
        cur.execute("TRUNCATE TABLE header CASCADE;")
        t1 = time.perf_counter()
        cur.copy_from(f, 'header', sep=',', null="", size=size)
        t2 = time.perf_counter()
        total += cnt / (t2 - t1)
    print(str(cnt) + " records successfully loaded")
    print("copy_from() with size = " + str(size) + " : " + str(round(total / num)) + " records/s")
    return


def normal_copy(num, cur, size=8192):
    cnt = 0
    with open('/header.csv', 'r') as f:
        reader = csv.reader(f)
        for _ in reader:
            cnt = cnt + 1
    total = 0.0
    for i in range(0, num):
        f = open('/header.csv', 'r')
        cur.execute("TRUNCATE TABLE header CASCADE;")
        t1 = time.perf_counter()
        cur.copy_from(f, 'header', sep=',', null="", size=size)
        t2 = time.perf_counter()
        total += cnt / (t2 - t1)
        print("The " + str(i + 1) + "th speed: " + str(round(cnt / (t2 - t1))) + " records/s")
    print(str(cnt) + " records successfully loaded")
    print("Average speed with size = " + str(size) + " : " + str(round(total / num)) + " records/s")
    return


def abnormal_copy(num, cur, size=8192):
    cnt = 0
    with open('/header.csv', 'r') as f:
        reader = csv.reader(f)
        for _ in reader:
            cnt = cnt + 1
    total = 0.0
    f = open('/header.csv', 'r')
    for i in range(0, num):
        cur.execute("TRUNCATE TABLE header CASCADE;")
        t1 = time.perf_counter()
        cur.copy_from(f, 'header', sep=',', null="", size=size)
        t2 = time.perf_counter()
        total += cnt / (t2 - t1)
        print("The " + str(i + 1) + "th speed: " + str(round(cnt / (t2 - t1))) + " records/s")
    print(str(cnt) + " records successfully loaded")
    print("copy_from() with size = " + str(size) + " : " + str(round(total / num)) + " records/s")
    return


if __name__ == '__main__':
    conn = psycopg2.connect("host=localhost dbname=cs307_2 user=checker password=123456")
    cur = conn.cursor()
    count_execute_values_iterator_normal(100, cur)
    count_execute_values(100, cur)
    conn.commit()
