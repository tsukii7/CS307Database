import time
import csv


def insert(userList, total_cnt=1000):
    cnt = 0
    total_time = 0
    t1 = time.perf_counter()
    for i in range(0, total_cnt):
        contract_number = "CSE" + str(i + 5000).zfill(7)
        userList.append([contract_number, 'Studio Trigger', 'Asia',
                         'Japan', 'NULL', 'Internet', 'BJS0822', 'Poster', 'PosterKIK', 50, 1000, '2013-10-3',
                         '2013-11-10', '2013-11-11', 'Steven Edwards', 'Theo White', '12140327', 'Male', 25,
                         '13986643179'])
        cnt += 1
    t2 = time.perf_counter()
    total_time += (t2 - t1)
    print("Insert operation done successfully " + str(cnt) + " times.")
    print("list() : " + str(format(1.0 * cnt / total_time, '.2f')) + " operations/s")
    print("time:" + str(format(total_time, '.2f')) + "s   cnt:" + str(cnt) + "\n")
    return


def select(userList, total_cnt=1000):
    cnt = 0
    total_time = 0
    list = []
    t1 = time.perf_counter()
    for i in range(0, total_cnt):
        contract_number = "CSE" + str(i).zfill(7)
        for record in userList:
            if record[0] == contract_number:
                list.append(record)
        cnt += 1
    t2 = time.perf_counter()
    total_time += (t2 - t1)
    # print(list)
    print("Select operation done successfully " + str(cnt) + " times.")
    print("list() : " + str(format(1.0 * cnt / total_time, '.2f')) + " operations/s")
    print("time:" + str(format(total_time, '.2f')) + "s   cnt:" + str(cnt) + "\n")
    return


def update(userList, total_cnt=1000):
    cnt = 0
    total_time = 0
    t1 = time.perf_counter()
    for i in range(0, total_cnt):
        contract_number = "CSE" + str(i + 5000).zfill(7)
        for record in userList:
            if record[0] == contract_number:
                record[8] = 'PosterFRANXX'
        cnt += 1
    t2 = time.perf_counter()
    total_time += (t2 - t1)
    print("Update operation done successfully " + str(cnt) + " times.")
    print("list() : " + str(format(1.0 * cnt / total_time, '.2f')) + " operations/s")
    print("time:" + str(format(total_time, '.2f')) + "s   cnt:" + str(cnt) + "\n")
    return


def delete(userList, total_cnt=1000):
    cnt = 0
    total_time = 0
    t1 = time.perf_counter()
    for i in range(0, total_cnt):
        contract_number = "CSE" + str(i + 5000).zfill(7)
        for index, record in enumerate(userList):
            if record[0] == contract_number:
                del userList[index]
        cnt += 1
    t2 = time.perf_counter()
    total_time += (t2 - t1)
    print("Delete operation done successfully " + str(cnt) + " times.")
    print("list() : " + str(format(1.0 * cnt / total_time, '.2f')) + " operations/s")
    print("time:" + str(format(total_time, '.2f')) + "s   cnt:" + str(cnt) + "\n")
    return


if __name__ == '__main__':
    userList = []
    f = open("C:\\Users\\21414\\Documents\\Curriculum\\database\\project01\\contract_info.csv")
    content = f.readlines()
    cnt = 10000000
    for line in content:
        line = line.replace('\n', '').split(',')
        userList.append(line)
    # print("Test for select_list:")
    insert(userList, cnt)
    # update(userList, cnt)
    # select(userList, cnt)
    # delete(userList, cnt)
    # out = open("C:\\Users\\21414\\Documents\\Curriculum\\database\\project01\\tempnew.csv", 'w', newline='')
    # write = csv.writer(out)
    # write.writerows(userList)
