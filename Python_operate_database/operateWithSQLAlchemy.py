from InitializeSQLAlchemy import db, Contract_info_orm
from sqlalchemy.orm import sessionmaker
import time


# select
def select(session):
    cnt = 0
    total_cnt = 1000
    total_time = 0
    for i in range(0, total_cnt):
        number = "CSE" + str(i).zfill(7)
        cnt+=1
        t1 = time.perf_counter()
        session.query(Contract_info_orm).filter_by(contract_number=number).all()
        t2 = time.perf_counter()
        total_time += (t2 - t1)
    session.commit()
    print("Select operation done successfully " + str(cnt) + " times.")
    print("SQLAlchemy : " + str(format(1.0 * cnt / total_time, '.2f')) + " operations/s")
    print("time:" + str(format(total_time, '.2f')) + "s   cnt:" + str(cnt) + "\n")
    return
# insert
def insert(session):
    cnt = 0
    total_cnt = 1000
    total_time = 0
    for i in range(0, total_cnt):
        contract_number = "CSE" + str(i + 5000).zfill(7)
        cnt+=1
        contract_info_orm = Contract_info_orm(contract_number=contract_number, client_enterprise='Studio Trigger',
                                              supply_center='Asia', country='Japan', city='NULL',
                                              industry='Internet',
                                              product_code='BJS0822', product_name='Poster',
                                              product_model='PosterKIK',
                                              unit_price=50,
                                              quantity=1000, contract_date='2013-10-3',
                                              estimated_delivery_date='2013-11-10',
                                              lodgement_date='2013-11-11', director='Steven Edwards',
                                              salesman='Theo White',
                                              salesman_number=12140327,
                                              gender='Male', age=25, mobile_phone='13986643179')
        t1 = time.perf_counter()
        session.add(contract_info_orm)
        t2 = time.perf_counter()
        total_time += (t2 - t1)
    session.commit()
    print("Insert operation done successfully " + str(cnt) + " times.")
    print("SQLAlchemy : " + str(format(1.0 * cnt / total_time, '.2f')) + " operations/s")
    print("time:" + str(format(total_time, '.2f')) + "s   cnt:" + str(cnt) + "\n")
    return

# update
def update(session):
    cnt = 0
    total_cnt = 1000
    total_time = 0
    for i in range(0, total_cnt):
        number = "CSE" + str(i+5000).zfill(7)
        cnt+=1
        t1 = time.perf_counter()
        for data in session.query(Contract_info_orm).filter_by(contract_number=number).first():
            data.product_model = 'PosterKIK'
        t2 = time.perf_counter()
        total_time += (t2 - t1)
    session.commit()
    print("Update operation done successfully " + str(cnt) + " times.")
    print("SQLAlchemy : " + str(format(1.0 * cnt / total_time, '.2f')) + " operations/s")
    print("time:" + str(format(total_time, '.2f')) + "s   cnt:" + str(cnt) + "\n")
    return
# delete
def delete(session):
    cnt = 0
    total_cnt = 1000
    total_time = 0
    for i in range(0, total_cnt):
        number = "CSE" + str(i+5000).zfill(7)
        cnt+=1
        t1 = time.perf_counter()
        session.query(Contract_info_orm).filter_by(contract_number=number).delete()
        t2 = time.perf_counter()
        total_time += (t2 - t1)
    session.commit()
    print("Delete operation done successfully " + str(cnt) + " times.")
    print("SQLAlchemy : " + str(format(1.0 * cnt / total_time, '.2f')) + " operations/s")
    print("time:" + str(format(total_time, '.2f')) + "s   cnt:" + str(cnt) + "\n")
    return

if __name__ == '__main__':
    s = sessionmaker(bind=db)
    session = s()
    print("Operate with SQLAlchemy test for delete():")
    delete(session)
