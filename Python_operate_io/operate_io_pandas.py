import pandas as pd
import time


# insert
def insert(data):
    cnt = 0
    total_cnt = 1000
    total_time = 0
    t1 = time.perf_counter()
    for i in range(0, total_cnt):
        contract_number = "CSE" + str(i + 5000).zfill(7)
        data.loc[i + 50000] = [contract_number, 'Studio Trigger', 'Asia',
                               'Japan', 'NULL', 'Internet', 'BJS0822', 'Poster', 'PosterKIK', 50, 1000, '2013-10-3',
                               '2013-11-10', '2013-11-11', 'Steven Edwards', 'Theo White', '12140327', 'Male', 25,
                               '13986643179']
        cnt += 1
    t2 = time.perf_counter()
    total_time += (t2 - t1)
    print("Insert operation done successfully " + str(cnt) + " times.")
    print("pandas() : " + str(format(1.0 * cnt / total_time, '.2f')) + " operations/s")
    print("time:" + str(format(total_time, '.2f')) + "s   cnt:" + str(cnt) + "\n")
    return


def insert_append(data):
    cnt = 0
    total_cnt = 1000
    total_time = 0
    t1 = time.perf_counter()
    for i in range(0, total_cnt):
        contract_number = "CSE" + str(i + 5000).zfill(7)
        s = pd.Series({"contract number": contract_number, "client_enterprise": 'Studio Trigger',
                       "supply center": 'Asia', "country": 'Japan', "city": 'NULL',
                       "industry": 'Internet', "product code": 'BJS0822', ' product name': 'Poster',
                       'product model': 'PosterKIK', 'unit price': 50, 'quantity': 1000, 'contract date': '2013-10-3',
                       'estimated delivery date': '2013-11-10', 'lodgement date': '2013-11-11',
                       'director': 'Steven Edwards',
                       'salesman': 'Theo White', 'salesman number': 12140327, 'gender': 'Male', 'age': 25,
                       'mobile phone': 13986643179
                       })
        # 这里 Series 必须是 dict-like 类型
        data = data.append(s, ignore_index=True)
        # 这里必须选择ignore_index=True 或者给 Series 一个index值
        cnt += 1
    t2 = time.perf_counter()
    total_time += (t2 - t1)
    print("Insert operation done successfully " + str(cnt) + " times.")
    print("pandas_append() : " + str(format(1.0 * cnt / total_time, '.2f')) + " operations/s")
    print("time:" + str(format(total_time, '.2f')) + "s   cnt:" + str(cnt) + "\n")
    return


if __name__ == '__main__':
    path = 'C:\\Users\\21414\\Documents\\Curriculum\\database\\project01\\contract_info.csv'
    with open(path) as file:
        data = pd.read_csv(file)
    insert_append(data)
    # print(data)
    # print(data.iloc[:, :])
    # print(data.iloc[0,])
    data.to_csv('C:\\Users\\21414\\Documents\\Curriculum\\database\\project01\\temp.csv')
