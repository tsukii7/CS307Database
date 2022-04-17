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
        data.iloc[i + 50000] = [contract_number, 'Studio Trigger', 'Asia',
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


def insert_append(data,total_cnt = 1000):
    cnt = 0
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


'''
        >>> df = pd.DataFrame([[1, 2], [3, 4]], columns=list('AB'))
        >>> df
           A  B
        0  1  2
        1  3  4
        >>> df2 = pd.DataFrame([[5, 6], [7, 8]], columns=list('AB'))
        >>> df.append(df2)
           A  B
        0  1  2
        1  3  4
        0  5  6
        1  7  8
        With `ignore_index` set to True:
        >>> df.append(df2, ignore_index=True)
'''
def insert_append_dataframe(data, total_cnt = 1000):
    cnt = 0
    total_time = 0
    t1 = time.perf_counter()
    column_name = data.columns
    list = []
    for i in range(0, total_cnt):
        contract_number = "CSE" + str(i + 5000).zfill(7)
        list.append([contract_number, 'Studio Trigger', 'Asia',
                                'Japan', 'NULL', 'Internet', 'BJS0822', 'Poster', 'PosterKIK', 50, 1000, '2013-10-3',
                                '2013-11-10', '2013-11-11', 'Steven Edwards', 'Theo White', '12140327', 'Male', 25,
                                '13986643179'])
        cnt += 1
    newdata = pd.DataFrame(list,columns=column_name)
    data = data.append(newdata,ignore_index=True)
    t2 = time.perf_counter()
    total_time += (t2 - t1)
    data.to_csv('C:\\Users\\21414\\Documents\\Curriculum\\database\\project01\\insert.csv')
    print("Insert operation done successfully " + str(cnt) + " times.")
    print("insert_append_dataframe() : " + str(format(1.0 * cnt / total_time, '.2f')) + " operations/s")
    print("time:" + str(format(total_time, '.2f')) + "s   cnt:" + str(cnt) + "\n")
    return


# select
def select(data,total_cnt = 1000):
    cnt = 0
    total_time = 0
    t1 = time.perf_counter()
    numbers = []
    for i in range(0, total_cnt):
        # number = "CSE" + str(i + 5000).zfill(7)
        numbers.append("CSE" + str(i + 5000).zfill(7))
        cnt += 1
    res = data[data['contract number'].isin(numbers)]
    # cnt = 1000
    t2 = time.perf_counter()
    total_time += (t2 - t1)
    print(res)
    # res.to_csv('C:\\Users\\21414\\Documents\\Curriculum\\database\\project01\\select.csv')
    print("Select operation done successfully " + str(cnt) + " times.")
    print("pandas() : " + str(format(1.0 * cnt / total_time, '.2f')) + " operations/s")
    print("time:" + str(format(total_time, '.2f')) + "s   cnt:" + str(cnt) + "\n")
    return


# update
def update(data,total_cnt = 1000):
    cnt = 0
    total_time = 0
    t1 = time.perf_counter()
    numbers = []
    for i in range(0, total_cnt):
        # number = "CSE" + str(i + 5000).zfill(7)
        numbers.append("CSE" + str(i + 5000).zfill(7))
        cnt += 1
    data[data['contract number'].isin(numbers)].replace("PosterKIK", "PosterFRANXX")
    t2 = time.perf_counter()
    # print(res)
    total_time += (t2 - t1)
    # print(res)
    data.to_csv('C:\\Users\\21414\\Documents\\Curriculum\\database\\project01\\update.csv')
    print("Update operation done successfully " + str(cnt) + " times.")
    print("pandas() : " + str(format(1.0 * cnt / total_time, '.2f')) + " operations/s")
    print("time:" + str(format(total_time, '.2f')) + "s   cnt:" + str(cnt) + "\n")
    return

def delete(data, total_cnt = 1000):
    cnt = 0
    total_time = 0
    t1 = time.perf_counter()
    numbers = []
    for i in range(0, total_cnt):
        # number = "CSE" + str(i + 5000).zfill(7)
        numbers.append("CSE" + str(i + 5000).zfill(7))
        cnt += 1
    res = data[~data['contract number'].isin(numbers)]
    # cnt = 1000
    t2 = time.perf_counter()
    total_time += (t2 - t1)
    # print(res)
    # res.to_csv('C:\\Users\\21414\\Documents\\Curriculum\\database\\project01\\delete.csv')
    print("Dlete operation done successfully " + str(cnt) + " times.")
    print("pandas() : " + str(format(1.0 * cnt / total_time, '.2f')) + " operations/s")
    print("time:" + str(format(total_time, '.2f')) + "s   cnt:" + str(cnt) + "\n")
    return


if __name__ == '__main__':
    path = 'C:\\Users\\21414\\Documents\\Curriculum\\database\\project01\\contract_info.csv'
    # path = 'C:\\Users\\21414\\Documents\\Curriculum\\database\\project01\\insert.csv'
    with open(path) as file:
        data = pd.read_csv(file, low_memory=False)
    # data.sort_values(by='contract number', inplace=True)
    # data = data.set_index("index", inplace=True)
    # print(data.columns)
    # print(data)
    cnt = [1000,10000,100000,1000000]
    for i in cnt:
        insert_append_dataframe(data, i)    # insert_append(data)
    # update(data)
    # insert_append_dataframe(data,cnt)
    # delete(data,cnt)
    # update(data, cnt)
    # select(data,cnt)
    # data.to_csv('C:\\Users\\21414\\Documents\\Curriculum\\database\\project01\\insert.csv')
