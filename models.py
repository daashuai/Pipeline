# -*- coding: utf-8 -*-
"""
批次管输调度系统 - 数据层模型
SQLAlchemy ORM 模型，仅用于数据持久化。
"""
from datetime import datetime
from typing import List, Dict, Optional
import uuid
from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer, Boolean, ForeignKey, JSON
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

Base = declarative_base()

# === SQLAlchemy ORM 实体定义 ===
class TankDB(Base):
    __tablename__ = 'tank'
    tank_id = Column(String(50), primary_key=True)  
    site_id = Column(String(50), primary_key=True)  
    tank_name = Column(String(50))
    tank_area = Column(String(50))
    tank_type = Column(JSON, default=["TARGET","MIDDLE"])
    oil_type = Column(String(50)) # 罐内油种
    inventory = Column(Float) # 库存
    current_level = Column(Float) # 当前液位
    tank_capacity_per_meter = Column(Float) # 每米罐容
    maximum_tank_capacity = Column(Float) # 极限罐容
    safe_tank_capacity = Column(Float) # 安全罐容
    maximum_tank_level = Column(Float) # 极限罐位
    safe_tank_level = Column(Float) # 安全罐位
    min_safe_level = Column(Float, default=0.0) # 最低罐位
    status = Column(String(20), default="AVAILABLE")

    def to_object(self):
        """将 SQLAlchemy 模型转换为业务对象"""
        from data_class import Tank
        return Tank(
            tank_id=self.tank_id,
            site_id=self.site_id,
            tank_name=self.tank_name,
            tank_area=self.tank_area,
            tank_type=self.tank_type,
            oil_type=self.oil_type,
            inventory=self.inventory,
            current_level=self.current_level,
            tank_capacity_per_meter=self.tank_capacity_per_meter,
            maximum_tank_capacity=self.maximum_tank_capacity,
            safe_tank_capacity=self.safe_tank_capacity,
            maximum_tank_level=self.maximum_tank_level,
            safe_tank_level=self.safe_tank_level,
            min_safe_level=self.min_safe_level,
            status=self.status
        )

class CustomerDB(Base):
    __tablename__ = 'customer'

    customer_id = Column(String(50), primary_key=True)
    customer_name = Column(String(100))

    def to_object(self):
        """将 SQLAlchemy 模型转换为业务对象"""
        from data_class import Customer
        return Customer(
            customer_id=self.customer_id,
            customer_name=self.customer_name
        )

class CustomerOrderDB(Base):
    __tablename__ = 'customer_order'

    customer_order_id = Column(Integer, primary_key=True, autoincrement=True)  # 修正拼写错误
    customer_id = Column(String(50))
    customer_name = Column(String(50))
    oil_type = Column(String(50))
    required_volume = Column(Float)
    dispatched_volume = Column(Float)
    undispatched_volume = Column(Float)
    start_time = Column(DateTime) # 本批次发油时间
    end_time = Column(DateTime) # 到油时间
    priority = Column(Integer, default=1)
    entry_tank_id = Column(String(50)) # 进罐号（最终要存储到哪个罐里面）
    finish_storage_tank_time = Column(DateTime) # 到油之后，油完全存储到罐里所需要的时间
    branch_start_time = Column(DateTime) # 支线启输时间
    branch_end_time = Column(DateTime) # 支线计划完成输送时间
    status = Column(String(50), default="PENDING")

    def to_object(self):
        """将 SQLAlchemy 模型转换为业务对象"""
        from data_class import CustomerOrder
        return CustomerOrder(
            customer_order_id=self.customer_order_id,
            customer_id=self.customer_id,
            customer_name=self.customer_name,
            oil_type=self.oil_type,
            required_volume=self.required_volume,
            dispatched_volume=self.dispatched_volume,
            undispatched_volume=self.undispatched_volume,
            start_time=self.start_time,
            end_time=self.end_time,
            priority=self.priority,
            entry_tank_id=self.entry_tank_id,
            finish_storage_tank_time=self.finish_storage_tank_time,
            branch_start_time=self.branch_start_time,
            branch_end_time=self.branch_end_time,
            status=self.status
        )

class DispatchOrderDB(Base):
    __tablename__ = 'dispatch_order'

    dispatch_order_id = Column(Integer, primary_key=True, autoincrement=True)
    customer_order_id = Column(String(50))  # 修正字段名
    oil_type = Column(String(50))
    required_volume = Column(Float)
    source_tank_id = Column(String(50))
    target_tank_id = Column(String(50))
    pipeline_path = Column(JSON)  # 管线ID列表
    start_time = Column(Integer)
    end_time = Column(Integer)
    status = Column(String(50), default="DRAFT")  # 状态: DRAFT/SCHEDULED/RUNNING/COMPLETED/CONFLICT
    cleaning_required = Column(Boolean, default=False)  # 是否需要清洗

    def to_object(self):
        """将 SQLAlchemy 模型转换为业务对象"""
        from data_class import DispatchOrder
        return DispatchOrder(
            dispatch_order_id=self.dispatch_order_id,
            customer_order_id=self.customer_order_id,
            oil_type=self.oil_type,
            required_volume=self.required_volume,
            source_tank_id=self.source_tank_id,
            target_tank_id=self.target_tank_id,
            pipeline_path=self.pipeline_path,
            start_time=self.start_time,
            end_time=self.end_time,
            status=self.status,
            cleaning_required=self.cleaning_required
        )

class SiteDB(Base):
    __tablename__ = 'site'
    site_id = Column(String(50), primary_key=True)
    site_name = Column(String(50))

    def to_object(self):
        """将 SQLAlchemy 模型转换为业务对象"""
        from data_class import Site
        return Site(
            site_id=self.site_id,
            site_name=self.site_name
        )

class PipelineDB(Base):
    __tablename__ = 'pipeline' 

    pipe_id = Column(String(50), primary_key=True)
    pipe_name = Column(String(50))
    pipe_capacity_per_meter = Column(Float) # 每米管容
    pipe_shutdown_start_time = Column(DateTime) # 管道停输开始时间
    pipe_shutdown_end_time = Column(DateTime) # 管道停输结束时间
    pipe_shutdown_reason = Column(String(50)) # 管道停输原因

    def to_object(self):
        """将 SQLAlchemy 模型转换为业务对象"""
        from data_class import Pipeline
        return Pipeline(
            pipe_id=self.pipe_id,
            pipe_name=self.pipe_name,
            pipe_capacity_per_meter=self.pipe_capacity_per_meter,
            pipe_shutdown_start_time=self.pipe_shutdown_start_time,
            pipe_shutdown_end_time=self.pipe_shutdown_end_time,
            pipe_shutdown_reason=self.pipe_shutdown_reason
        )

# 记录管道干线上站点的信息
class BranchDB(Base):
    __tablename__ = 'branch' 

    branch_id = Column(String(50), primary_key=True)
    from_id = Column(String(50)) 
    to_id = Column(String(50), primary_key=True)
    is_direct_connection = Column(Boolean, default=False)
    branch_name = Column(String(50))
    branch_mileage = Column(Float) # 绝对里程
    branch_elevation = Column(Float) # 高程
    branch_capacity = Column(Float) # 高程
    is_begin = Column(String(10)) # 是否作为起点
    is_end = Column(String(10)) # 是否作为终点
    is_middle = Column(String(10)) # 是否中间站点

    def to_object(self):
        """将 SQLAlchemy 模型转换为业务对象"""
        from data_class import Branch
        return Branch(
            branch_id=self.branch_id,
            from_id=self.from_id,
            to_id=self.to_id,
            is_direct_connection=self.is_direct_connection,
            branch_name=self.branch_name,
            branch_mileage=self.branch_mileage,
            branch_elevation=self.branch_elevation,
            branch_capacity=self.branch_capacity,
            is_begin=self.is_begin,
            is_end=self.is_end,
            is_middle=self.is_middle
        )

class OilDB(Base):
    __tablename__ = 'oil'

    id = Column(Integer, primary_key=True, autoincrement=True)
    oil_name = Column(String(50))
    oil_id = Column(String(50))
    p20 = Column(Float) 
    freezing_point = Column(Float)
    h2s = Column(String(50))
    kinematic_viscosity = Column(String(50))
    place_of_origin = Column(String(50))
    transfer_way = Column(String(50))

    def to_object(self):
        """将 SQLAlchemy 模型转换为业务对象"""
        from data_class import Oil
        return Oil(
            id=self.id,
            oil_name=self.oil_name,
            oil_id=self.oil_id,
            p20=self.p20,
            freezing_point=self.freezing_point,
            h2s=self.h2s,
            kinematic_viscosity=self.kinematic_viscosity,
            place_of_origin=self.place_of_origin,
            transfer_way=self.transfer_way
        )





# """
# 批次管输调度系统 - 核心数据模型 + 数据持久化接口
# 适用于每周批次调度 + 动态调整模式。
# 提供基础 ORM 持久化接口（SQLite）。
# """
# from datetime import datetime, timedelta
# from typing import List, Dict, Optional
# import uuid
# from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer, Boolean, ForeignKey, JSON
# from sqlalchemy.orm import declarative_base, relationship, sessionmaker
# 
# Base = declarative_base()
# 
# # === ORM 实体定义 ===
# class Tank(Base):
#     __tablename__ = 'tank'
#     tank_id = Column(String(50), primary_key=True)  
#     tank_name = Column(String(50))
#     tank_area = Column(String(50))
#     oil_type = Column(String(50)) # 罐内油种
#     inventory = Column(Float) # 库存
#     current_level = Column(Float) # 当前液位
#     tank_capacity_per_meter = Column(Float) # 每米罐容
#     maximum_tank_capacity = Column(Float) # 极限罐容
#     safe_tank_capacity = Column(Float) # 安全罐容
#     maximum_tank_level = Column(Float) # 极限罐位
#     safe_tank_level = Column(Float) # 安全罐位
#     min_safe_level = Column(Float, default=0.0) # 最低罐位
#     status = Column(String(20), default="AVAILABLE")
# 
#     def can_supply(self, oil_type: str, required_volume: float) -> bool:
#         if self.status != "AVAILABLE":
#             return False
#         if self.inventory - required_volume < self.min_safe_level:
#             return False
#         return True
# 
#     def reserve(self, volume: float):
#         if volume > self.inventory - self.min_safe_level:
#             raise ValueError("库存不足，无法预留该体积")
#         self.inventory -= volume
#         self.status = "RESERVED"
# 
#     def release(self):
#         self.status = "AVAILABLE"
# 
# class Customer(Base):
#     __tablename__ = 'customer'
# 
#     customer_id= Column(String(50), primary_key=True)
#     customer_name = Column(String(100))
# 
# 
# class CustomerOrder(Base):
#     __tablename__ = 'customer_order'
# 
#     custormer_order_id = Column(Integer, primary_key=True, autoincrement=True)
#     customer_id= Column(String(50))
#     customer_name = Column(String(50))
#     oil_type = Column(String(50))
#     required_volume = Column(Float)
#     dispatched_volume = Column(Float)
#     undispatched_volume = Column(Float)
#     start_time = Column(DateTime) # 本批次发油时间
#     end_time = Column(DateTime) # 到油时间
#     priority = Column(Integer, default=1)
#     entry_tank_id = Column(String(50)) # 进罐号（最终要存储到哪个罐里面）
#     finish_storage_tank_time = Column(DateTime) # 到油之后，油完全存储到罐里所需要的时间
#     branch_start_time = Column(DateTime) # 支线启输时间
#     branch_end_time = Column(DateTime) # 支线计划完成输送时间
#     # allow_multi_tank = Column(Boolean, default=True)
#     # preferred_branches = Column(JSON, default=list)
#     status = Column(String(50), default="PENDING")
# 
#     def is_fully_scheduled(self):
#         if self.dispatched_volume == self.required_volume:
#             return True
#         else:
#             return False
# 
# 
# class DispatchOrder(Base):
#     __tablename__ = 'dispatch_order'
# 
#     dispatch_order_id = Column(Integer, primary_key=True, autoincrement=True)
#     custormer_order_id = Column(String(50), primary_key=True)
#     oil_type = Column(String(50))
#     required_volume = Column(Float)
#     source_tank_id = Column(String(50))
#     target_tank_id = Column(String(50))
#     pipeline_path = Column(JSON)  # 管线ID列表
#     start_time = Column(Integer)
#     end_time = Column(Integer)
#     status = Column(String(50), default="DRAFT")  # 状态: DRAFT/SCHEDULED/RUNNING/COMPLETED/CONFLICT
#     cleaning_required = Column(Boolean, default=False)  # 是否需要清洗
# 
# # class Branch(Base):
# #     __tablename__ = 'branches'
# # 
# #     branch_id = Column(String(50), primary_key=True)
# #     max_rate_m3h = Column(Float)
# #     stop_windows = Column(JSON, default=list)
# #     status = Column(String(20), default="AVAILABLE")
# 
# class Site(Base):
#     __tablename__ = 'site'
#     site_id = Column(String(50), primary_key=True)
#     site_name = Column(String(50))
# 
# 
# 
# class Pipeline(Base):
#     __tablename__ = 'pipeline' 
# 
#     pipe_id = Column(String(50), primary_key=True)
#     pipe_name = Column(String(50))
#     pipe_capacity_per_meter = Column(Float) # 每米管容
#     pipe_shutdown_start_time = Column(DateTime) # 管道停输开始时间
#     pipe_shutdown_end_time = Column(DateTime) # 管道停输结束时间
#     pipe_shutdown_reason = Column(String(50)) # 管道停输原因
# 
# 
# # 记录管道干线上站点的信息
# class Branch(Base):
#     __tablename__ = 'branch' 
# 
#     branch_id = Column(Integer, primary_key=True, autoincrement=True)
#     pipe_id = Column(String(50)) 
#     site_id = Column(String(50), primary_key=True)
#     pipe_name = Column(String(50))
#     site_name = Column(String(50))
#     pipe_mileage = Column(Float) # 绝对里程
#     pipe_elevation = Column(Float) # 高程
#     is_begin = Column(String(10)) # 是否作为起点
#     is_end = Column(String(10)) # 是否作为终点
#     is_middle = Column(String(10)) # 是否中间站点
# 
# 
# class Oil(Base):
#     __tablename__ = 'oil'
# 
#     id = Column(Integer, primary_key=True, autoincrement=True)
#     oil_name = Column(String(50))
#     oil_id = Column(String(50))
#     p20 = Column(Float) 
#     freezing_point = Column(Float)
#     h2s = Column(String(50))
#     kinematic_viscosity = Column(String(50))
#     place_of_origin = Column(String(50))
#     transfer_way = Column(String(50))
# 
# 
# # class Plan(Base):
# #     __tablename__ = 'plans'
# # 
# #     plan_id = Column(String(50), primary_key=True)
# #     order_id = Column(String(50), ForeignKey('orders.order_id'))
# #     branch_id = Column(String(50), ForeignKey('branches.branch_id'))
# #     start_time = Column(DateTime)
# #     end_time = Column(DateTime)
# #     rate_m3h = Column(Float)
# #     status = Column(String(20), default="DRAFT")
# # 
# #     order = relationship('Order')
# #     branch = relationship('Branch')
# 
