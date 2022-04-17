import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pandas as pd

# 1.创建连接
# 数据库类型+数据库操作的包:// 用户名:密码@主机地址/你要操作的数据库
db = sqlalchemy.create_engine('postgresql+psycopg2://checker:123456@localhost:5432/cs307_2')

# 2.创建基类
base = declarative_base(db)


# 3.创建类 必须继承基类  创建模型
class Contract_info_orm(base):
    # 表名
    __tablename__ = 'contract_info_orm'
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    contract_number = sqlalchemy.Column(sqlalchemy.String(150))  # varchar()
    client_enterprise = sqlalchemy.Column(sqlalchemy.String(150))  # varchar()
    supply_center = sqlalchemy.Column(sqlalchemy.String(150))  # varchar()
    country = sqlalchemy.Column(sqlalchemy.String(150))  # varchar()
    city = sqlalchemy.Column(sqlalchemy.String(150))  # varchar()
    industry = sqlalchemy.Column(sqlalchemy.String(150))  # varchar()
    product_code = sqlalchemy.Column(sqlalchemy.String(150))  # varchar()
    product_name = sqlalchemy.Column(sqlalchemy.String(150))  # varchar()
    product_model = sqlalchemy.Column(sqlalchemy.String(150))  # varchar()
    unit_price = sqlalchemy.Column(sqlalchemy.Integer)
    quantity = sqlalchemy.Column(sqlalchemy.Integer)
    contract_date = sqlalchemy.Column(sqlalchemy.String(150))
    estimated_delivery_date = sqlalchemy.Column(sqlalchemy.String(150))
    lodgement_date = sqlalchemy.Column(sqlalchemy.String(150))
    director = sqlalchemy.Column(sqlalchemy.String(150))
    salesman = sqlalchemy.Column(sqlalchemy.String(150))
    salesman_number = sqlalchemy.Column(sqlalchemy.String(150))
    gender = sqlalchemy.Column(sqlalchemy.String(150))
    age = sqlalchemy.Column(sqlalchemy.Integer)
    mobile_phone = sqlalchemy.Column(sqlalchemy.String(150))


if __name__ == '__main__':
    base.metadata.create_all(db)
    s = sessionmaker(bind=db)
    session = s()
    # session.execute("truncate table contract_info_orm")
    df = pd.read_csv('C:\\Users\\21414\\Documents\\Curriculum\\database\\project01\\contract_info.csv',
                     encoding="UTF-8")
    lines = list()
    read_result = df.reset_index().T.to_dict()
    for _index in read_result:
        # 获取每行的数据
        contract_info_orm = Contract_info_orm(contract_number=read_result[_index]['contract number'],
                                              client_enterprise=read_result[_index]['client enterprise'],
                                              supply_center=read_result[_index]['supply center'],
                                              country=read_result[_index]['country'], city=read_result[_index]['city'],
                                              industry=read_result[_index]['industry'],
                                              product_code=read_result[_index]['product code'],
                                              product_name=read_result[_index]['product name'],
                                              product_model=read_result[_index]['product model'],
                                              unit_price=read_result[_index]['unit price'],
                                              quantity=read_result[_index]['quantity'],
                                              contract_date=read_result[_index]['contract date'],
                                              estimated_delivery_date=read_result[_index]['estimated delivery date'],
                                              lodgement_date=read_result[_index]['lodgement date'],
                                              director=read_result[_index]['director'],
                                              salesman=read_result[_index]['salesman'],
                                              salesman_number=read_result[_index]['salesman number'],
                                              gender=read_result[_index]['gender'], age=read_result[_index]['age'],
                                              mobile_phone=read_result[_index]['mobile phone'])
        session.add(contract_info_orm)
    session.commit()
