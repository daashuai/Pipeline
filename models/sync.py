from models import Tank, Customer, CustomerOrder, Pipeline_info, Pipeline_detail, Oil, Base
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import uuid
from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer, Boolean, ForeignKey, JSON
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
import json

# Base = declarative_base()

# === ORM Session 工具 ===
def init_db(db_url: str = 'sqlite:///pipeline_batch.db'):
    engine = create_engine(db_url, echo=False, future=True)
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)
    return SessionLocal


def load_data_from_tank_json(session, json_file_path: str = "data.json"):
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    # 添加 Tank 数据
    tanks_data = data
    for tank_info in tanks_data:
        tank = Tank(**tank_info)
        session.add(tank)
    
    session.commit()
    print("tank数据导入成功！")


# def load_data_from_tank_detail_json(session, json_file_path: str = "data.json"):
#     with open(json_file_path, 'r', encoding='utf-8') as file:
#         data = json.load(file)
    
#     # 添加 Tank 数据
#     tanks_data = data
#     for tank_detail in tanks_data:
#         tank = TankDetail(**tank_detail)
#         session.add(tank)
    
#     session.commit()
#     print("tank_detail数据导入成功！")


def load_data_from_pipeline_info_json(session, json_file_path: str = "data.json"):
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    # 添加 pipeline_info 数据
    pipelines_data = data
    for pipeline_info in pipelines_data:
        pipeline = Pipeline_info(**pipeline_info)
        session.add(pipeline)
    
    session.commit()
    print("pipeline_info数据导入成功！")



def load_data_from_pipeline_detail_json(session, json_file_path: str = "data.json"):
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    # 添加 pipeline_detail 数据
    pipelines_data = data
    for pipeline_detail in pipelines_data:
        pipeline = Pipeline_detail(**pipeline_detail)
        session.add(pipeline)
    
    session.commit()
    print("pipeline_detail数据导入成功！")


def load_data_from_oil_json(session, json_file_path: str = "data.json"):
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    # 添加 oil 数据
    oils_data = data
    for oil_data in oils_data:
        oil = Oil(**oil_data)
        session.add(oil)
    
    session.commit()
    print("oils数据导入成功！")
   

def load_data_from_customer_order_json(session, json_file_path: str = "data.json"):
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    time_fields = [
        'start_time',
        'end_time', 
        'finish_storage_tank_time',
        'branch_start_time',
        'branch_end_time'
    ]

    # 添加 order 数据
    orders_data = data
    for order_data in orders_data:
        for i in time_fields:
            if order_data[i] is not None:
                order_data[i] = datetime.strptime(order_data[i], "%Y-%m-%d %H:%M:%S")

        order = CustomerOrder(**order_data)
        session.add(order)
    
    session.commit()
    print("customer_orders数据导入成功！")


def load_data_from_customer_json(session, json_file_path: str = "data.json"):
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    # 添加 customer 数据
    customers_data = data
    for customer_data in customers_data:
        customer = Customer(**customer_data)
        session.add(customer)
    
    session.commit()
    print("customers数据导入成功！")



if __name__ == "__main__":
    SessionLocal = init_db()
    session = SessionLocal()

    tank_path = '../data/json/tank.json'
    pipeline_info_path = '../data/json/pipeline_info.json'
    pipeline_detail_path = '../data/json/pipeline_detail.json'
    customer_path = '../data/json/customer.json'
    oil_path = '../data/json/oil.json'
    customer_order_path = '../data/json/customer_order.json'

    load_data_from_tank_json(session, tank_path)
    # load_data_from_tank_detail_json(session, tank_detail_path)
    load_data_from_pipeline_info_json(session, pipeline_info_path)
    load_data_from_pipeline_detail_json(session, pipeline_detail_path)
    load_data_from_oil_json(session, oil_path)
    load_data_from_customer_order_json(session, customer_order_path)
    load_data_from_customer_json(session, customer_path) 

