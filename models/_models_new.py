# -*- coding: utf-8 -*-
"""
批次管输调度系统 - 核心数据模型 + 数据持久化接口
适用于每周批次调度 + 动态调整模式。
提供基础 ORM 持久化接口（SQLite）。
"""
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import uuid
from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer, Boolean, ForeignKey, JSON
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
import json

Base = declarative_base()

# === ORM 实体定义 ===
class TankInfo(Base):
    __tablename__ = 'tank_info'
    tank_id = Column(String(50), primary_key=True)  # ✅ 指定长度，比如 50
    tank_name = Column(String(50)) 
    tank_area = Column(String(50)) 
    oil_id = Column(String(50))  
    status = Column(String(20)) 
    safe_tank_capacity = Column(Float)
    maximum_tank_capacity = Column(Float)
    tank_capacity_per_meter = Column(Float)


class TankDetail(Base):
    __tablename__ = 'tank_detail'
    id = Column(Integer, primary_key=True, autoincrement=True)
    tank_id = Column(String(50))  # ✅ 指定长度，比如 50
    tank_capacity = Column(Float) 
    oil_height = Column(Float) 


class Order(Base):
    __tablename__ = 'orders'
    tank_id = Column(String(50)) 
    order_id = Column(String(50), primary_key=True)
    customer_name = Column(String(100))
    oil_id = Column(String(50))
    required_volume = Column(Float)
    required_height = Column(Float)
    earliest_start_time = Column(DateTime)
    deadline = Column(DateTime)
    priority = Column(Integer)
    origin = Column(String(50))
    destination =  Column(String(50))


class Oil(Base):
    __tablename__ = 'oils'

    id = Column(Integer, primary_key=True, autoincrement=True)
    oil_name = Column(String(50))
    oil_id = Column(String(50))
    p20 = Column(Float) 
    freezing_point = Column(Float)
    h2s = Column(String(50))
    kinematic_viscosity = Column(String(50))
    place_of_origin = Column(String(50))
    transfer_way = Column(String(50))


class Pipeline_info(Base):
    __tablename__ = 'pipeline_info' 

    pipe_id = Column(String(50), primary_key=True)
    pipe_name = Column(String(50))
    pipe_capacity_per_meter = Column(Float)
    pipe_shutdown_start_time = Column(DateTime)
    pipe_shutdown_end_time = Column(DateTime)
    pipe_shutdown_reason = Column(String(50))


class Pipeline_detail(Base):
    __tablename__ = 'pipeline_detail' 

    id = Column(Integer, primary_key=True, autoincrement=True)
    pipe_id = Column(String(50))
    pipe_name = Column(String(50))
    station_name = Column(String(50))
    pipe_mileage = Column(Float)
    pipe_elevation = Column(Float)
    is_begin = Column(String(10))
    is_end = Column(String(10))
    is_middle = Column(String(10))


class Customer(Base):
    __tablename__ = 'customers' 

    customer_id = Column(String(50), primary_key=True)
    customer_name = Column(String(50))


# === ORM Session 工具 ===
def init_db(db_url: str = 'sqlite:///pipeline_batch.db'):
    engine = create_engine(db_url, echo=False, future=True)
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)
    return SessionLocal


def load_data_from_tank_info_json(session, json_file_path: str = "data.json"):
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    # 添加 Tank 数据
    tanks_data = data
    for tank_info in tanks_data:
        tank = TankInfo(**tank_info)
        session.add(tank)
    
    session.commit()
    print("tank_info数据导入成功！")


def load_data_from_tank_detail_json(session, json_file_path: str = "data.json"):
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    # 添加 Tank 数据
    tanks_data = data
    for tank_detail in tanks_data:
        tank = TankDetail(**tank_detail)
        session.add(tank)
    
    session.commit()
    print("tank_detail数据导入成功！")


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
   

def load_data_from_order_json(session, json_file_path: str = "data.json"):
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    # 添加 order 数据
    orders_data = data
    for order_data in orders_data:
        order = Order(**order_data)
        session.add(order)
    
    session.commit()
    print("orders数据导入成功！")


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

    tank_info_path = '/home/lijiehui/project/pipeline/Pipeline/data/json/tank_info.json'
    tank_detail_path = '/home/lijiehui/project/pipeline/Pipeline/data/json/tank_detail.json'
    pipeline_info_path = '/home/lijiehui/project/pipeline/Pipeline/data/json/pipeline_info.json'
    pipeline_detail_path = '/home/lijiehui/project/pipeline/Pipeline/data/json/pipeline_detail.json'
    customer_path = '/home/lijiehui/project/pipeline/Pipeline/data/json/customer.json'
    oil_path = '/home/lijiehui/project/pipeline/Pipeline/data/json/oil.json'
    order_path = '/home/lijiehui/project/pipeline/Pipeline/data/json/order.json'

    load_data_from_tank_info_json(session, tank_info_path)
    load_data_from_tank_detail_json(session, tank_detail_path)
    load_data_from_pipeline_info_json(session, pipeline_info_path)
    load_data_from_pipeline_detail_json(session, pipeline_detail_path)
    load_data_from_oil_json(session, oil_path)
    load_data_from_order_json(session, order_path)
    load_data_from_customer_json(session, customer_path) 

