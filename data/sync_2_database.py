from models.models import Tank, Customer, CustomerOrder, Pipeline, Branch, Oil, Base
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import uuid
from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer, Boolean, ForeignKey, JSON
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
import json
from utils.database import load_config, get_database_url

def init_db(db_url: str = 'sqlite:///pipeline_batch.db'):
    config = load_config()
    db_cfg = config['database']
    db_url = get_database_url(db_cfg, include_db=True)
    engine = create_engine(db_url, echo=False, future=True)
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)
    return SessionLocal


def clear_table(session, model_class):
    """清空指定表的所有数据"""
    session.query(model_class).delete()
    session.commit()


def load_data_from_json(session, model_class, json_file_path: str, time_fields: List[str] = None):
    """
    通用的数据加载函数
    
    Args:
        session: 数据库会话
        model_class: ORM模型类
        json_file_path: JSON文件路径
        time_fields: 需要转换为datetime的时间字段列表
    """
    # 清空表
    clear_table(session, model_class)
    
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    # 如果指定了时间字段，则转换时间格式
    if time_fields:
        for item in data:
            for field in time_fields:
                if field in item and item[field] is not None:
                    item[field] = datetime.strptime(item[field], "%Y-%m-%d %H:%M:%S")
    
    # 批量添加数据
    for item_info in data:
        item = model_class(**item_info)
        session.add(item)
    
    session.commit()
    
    # 获取模型类名并打印成功信息
    class_name = model_class.__name__
    print(f"{class_name.lower()}数据导入成功！")


if __name__ == "__main__":
    SessionLocal = init_db()
    session = SessionLocal()

    # 定义数据加载配置
    data_configs = [
        {
            'model': Tank,
            'path': './data/json/tank.json',
            'time_fields': []
        },
        {
            'model': Pipeline,
            'path': './data/json/pipeline.json',
            'time_fields': []
        },
        {
            'model': Branch,
            'path': './data/json/branch.json',
            'time_fields': []
        },
        {
            'model': Customer,
            'path': './data/json/customer.json',
            'time_fields': []
        },
        {
            'model': Oil,
            'path': './data/json/oil.json',
            'time_fields': []
        },
        {
            'model': CustomerOrder,
            'path': './data/json/customer_order.json',
            'time_fields': [
                'start_time',
                'end_time', 
                'finish_storage_tank_time',
                'branch_start_time',
                'branch_end_time'
            ]
        }
    ]

    # 批量加载数据
    for config in data_configs:
        load_data_from_json(
            session=session,
            model_class=config['model'],
            json_file_path=config['path'],
            time_fields=config['time_fields']
        )


