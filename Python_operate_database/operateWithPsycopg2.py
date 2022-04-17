import psycopg2
import time
from psycopg2 import extras


# insert
def insert_execute(cur):
    cnt = 0
    total_cnt = 1000
    total_time = 0

    for i in range(0, total_cnt):
        contract_number = "CSE" + str(i + 5000).zfill(7)
        cnt += 1
        t1 = time.perf_counter()
        cur.execute("insert into contract_info values ('" + contract_number + "', 'Studio Trigger', 'Asia', "
                                                                              "'Japan', 'NULL', 'Internet', 'BJS0822', 'Poster', 'PosterKIK', 50, 1000, '2013-10-3', "
                                                                              "'2013-11-10','2013-11-11','Steven Edwards','Theo White','12140327','Male',25,"
                                                                              "'13986643179')")
        t2 = time.perf_counter()
        total_time += (t2 - t1)
    print("Insert operation done successfully " + str(cnt) + " times.")
    print("execute() : " + str(format(1.0 * cnt / total_time, '.2f')) + " operations/s")
    print("time:" + str(format(total_time, '.2f')) + "s   cnt:" + str(cnt) + "\n")
    return


def insert_batch_iterator(cur):
    cnt = 0
    total_cnt = 1000
    total_time = 0
    page_size = 2000

    list = []
    for i in range(0, total_cnt):
        contract_number = "CSE" + str(i + 6000).zfill(7)
        list.append([contract_number])

    iter_list = iter(list)
    for i in range(0, total_cnt):
        cnt += 1
        t1 = time.perf_counter()
        extras.execute_batch(cur,
                             "insert into contract_info values (%s, 'Studio Trigger', 'Asia', "
                             "'Japan', 'NULL', 'Internet', 'BJS0822', 'Poster', 'PosterKIK', 50, 1000, '2013-10-3', "
                             "'2013-11-10','2013-11-11','Steven Edwards','Theo White','12140327','Male',25,"
                             "'13986643179');",
                             iter_list, page_size=page_size)
        t2 = time.perf_counter()
        total_time += (t2 - t1)
    print("Insert operation done successfully " + str(cnt) + " times.")
    print("execute_batch() with iterator and page_size = " + str(page_size) + " : " + str(
        format(1.0 * cnt / total_time, '.2f')) + " operations/s")
    print("time:" + str(format(total_time, '.2f')) + "s   cnt:" + str(cnt) + "\n")
    return


# select
def select_execute(cur):
    cnt = 0
    total_cnt = 1000
    total_time = 0
    t1 = time.perf_counter()

    for i in range(0, total_cnt):
        contract_number = "CSE" + str(i).zfill(7)
        cnt += 1
        cur.execute("SELECT * from contract_info where contract_number = '" + contract_number + "'")
        rows = cur.fetchall()
        # print(rows)
        # for row in rows:
        #     print(row)
    t2 = time.perf_counter()
    total_time += (t2 - t1)
    print("Select operation done successfully " + str(cnt) + " times.")
    print("execute() : " + str(format(1.0 * cnt / total_time, '.2f')) + " operations/s")
    print("time:" + str(format(total_time, '.2f')) + "s   cnt:" + str(cnt) + "\n")
    return


def select_batch_iterator(cur):
    cnt = 0
    total_cnt = 1000
    total_time = 0
    page_size = 2000
    t1 = time.perf_counter()
    list = []
    for i in range(0, total_cnt):
        contract_number = "CSE" + str(i).zfill(7)
        list.append([contract_number])

    iter_list = iter(list)
    for i in range(0, total_cnt):
        cnt += 1
        extras.execute_batch(cur,
                             "SELECT * from contract_info where contract_number = %s",
                             iter_list, page_size=page_size)
        # rows = cur.fetchall()
        # for row in rows:
        #     print(row)
        #     print("contract_number = " + row[0])
        #     print("client_enterprise = " + row[1])
        #     print("supply_center = " + row[2])
    t2 = time.perf_counter()
    total_time += (t2 - t1)
    print("Select operation done successfully " + str(cnt) + " times.")
    print("execute_batch() with iterator and page_size = " + str(page_size) + " : " + str(
        format(1.0 * cnt / total_time, '.2f')) + " operations/s")
    print("time:" + str(format(total_time, '.2f')) + "s   cnt:" + str(cnt) + "\n")
    return


# update
def update_execute(cur):
    cnt = 0
    total_cnt = 1000
    total_time = 0
    t1 = time.perf_counter()

    for i in range(0, total_cnt):
        contract_number = "CSE" + str(i + 5000).zfill(7)
        cnt += 1
        cur.execute(
            "update contract_info set product_model = 'PosterFRANXX' where contract_number = '" + contract_number + "';")
    t2 = time.perf_counter()
    total_time += (t2 - t1)
    print("Update operation done successfully " + str(cnt) + " times.")
    print("execute() : " + str(format(1.0 * cnt / total_time, '.2f')) + " operations/s")
    print("time:" + str(format(total_time, '.2f')) + "s   cnt:" + str(cnt) + "\n")
    return


def update_batch_iterator(cur):
    cnt = 0
    total_cnt = 1000
    total_time = 0
    page_size = 2000
    t1 = time.perf_counter()

    list = []
    for i in range(0, total_cnt):
        contract_number = "CSE" + str(i + 6000).zfill(7)
        list.append([contract_number])

    iter_list = iter(list)
    for i in range(0, total_cnt):
        cnt += 1
        extras.execute_batch(cur,
                             "update contract_info set product_model = 'PosterFRANXX' where contract_number = %s;",
                             iter_list, page_size=page_size)
    t2 = time.perf_counter()
    total_time += (t2 - t1)
    print("Update operation done successfully " + str(cnt) + " times.")
    print("execute_batch() with iterator and page_size = " + str(page_size) + " : " + str(
        format(1.0 * cnt / total_time, '.2f')) + " operations/s")
    print("time:" + str(format(total_time, '.2f')) + "s   cnt:" + str(cnt) + "\n")
    return


# delete
def delete_execute(cur):
    cnt = 0
    total_cnt = 1000
    total_time = 0
    t1 = time.perf_counter()

    for i in range(0, total_cnt):
        contract_number = "CSE" + str(i + 5000).zfill(7)
        cnt += 1
        cur.execute("delete from contract_info where contract_number = '" + contract_number + "'")
    t2 = time.perf_counter()
    total_time += (t2 - t1)
    print("Delete operation done successfully " + str(cnt) + " times.")
    print("execute() : " + str(format(1.0 * cnt / total_time, '.2f')) + " operations/s")
    print("time:" + str(format(total_time, '.2f')) + "s   cnt:" + str(cnt) + "\n")
    return


def delete_batch_iterator(cur):
    cnt = 0
    total_cnt = 1000
    total_time = 0
    page_size = 2000
    t1 = time.perf_counter()

    list = []
    for i in range(0, total_cnt):
        contract_number = "CSE" + str(i + 6000).zfill(7)
        list.append([contract_number])

    iter_list = iter(list)
    for i in range(0, total_cnt):
        cnt += 1
        extras.execute_batch(cur,
                             "delete from contract_info where contract_number = %s",
                             iter_list, page_size=page_size)
    t2 = time.perf_counter()
    total_time += (t2 - t1)
    print("Delete operation done successfully " + str(cnt) + " times.")
    print("execute_batch() with iterator and page_size = " + str(page_size) + " : " + str(
        format(1.0 * cnt / total_time, '.2f')) + " operations/s")
    print("time:" + str(format(total_time, '.2f')) + "s   cnt:" + str(cnt) + "\n")
    return


if __name__ == '__main__':
    conn = psycopg2.connect(database="cs307_2", user="checker", password="123456", host="localhost", port="5432")
    print("Opened database successfully")

    cur = conn.cursor()
    print("Test for delete_execute:")
    delete_execute(cur)
    print("Test for delete_batch_iterator:")
    delete_batch_iterator(cur)
    conn.commit()
    conn.close()
