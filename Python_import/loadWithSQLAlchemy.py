import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pandas as pd
import time


# 1.创建连接
# 数据库类型+数据库操作的包:// 用户名:密码@主机地址/你要操作的数据库
# mysql://scott:tiger@hostname/dbname
db = sqlalchemy.create_engine('postgresql+psycopg2://checker:123456@localhost:5432/cs307_2')

# 2.创建基类
base = declarative_base(db)


# 3.创建类 必须继承基类  创建模型
class Header(base):
    # 表名
    __tablename__ = 'header'
    contract_number = sqlalchemy.Column(sqlalchemy.String(150), primary_key=True)  # varchar()
    contract_date = sqlalchemy.Column(sqlalchemy.String(150))


if __name__ == '__main__':
    # 执行数据库迁移 创建表
    base.metadata.create_all(db)
    # 绑定一个实例
    s = sessionmaker(bind=db)
    # # 创建回话对象  类似于游标
    session = s()
    # 添加
    df = pd.read_csv("headerWithTitle.csv")
    lines = list()
    read_result = df.reset_index().T.to_dict()
    num = 100
    cnt = 0
    total = 0.0
    for i in range(0, num):
        session.execute("truncate table header")
        cnt = 0
        t1 = time.perf_counter()
        for _index in read_result:
            # 获取每行的数据
            cnt+=1
            header = Header(contract_number=read_result[_index]['contract number'],
                            contract_date=read_result[_index]['contract date'])
            session.add(header)
        t2 = time.perf_counter()
        total += cnt / (t2 - t1)
        session.commit()
    print(str(cnt) + " records successfully loaded")
    print("SQLAlchemy() : " + str(round(total / num)) + " records/s")
