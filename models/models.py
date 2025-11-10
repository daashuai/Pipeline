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

Base = declarative_base()

# === ORM 实体定义 ===
class Tank(Base):
    __tablename__ = 'tanks'
    tank_id = Column(String(50), primary_key=True)  # ✅ 指定长度，比如 50
    owner = Column(String(100))
    oil_type = Column(String(50))
    inventory_m3 = Column(Float)
    max_capacity_m3 = Column(Float)
    min_safe_level_m3 = Column(Float)
    compatible_oils = Column(JSON)  # 注意：MySQL 5.7+ 才支持 JSON
    available_from = Column(DateTime)
    status = Column(String(20))

    def can_supply(self, oil_type: str, required_volume: float) -> bool:
        if self.status != "AVAILABLE":
            return False
        if oil_type not in self.compatible_oils:
            return False
        if self.inventory_m3 - required_volume < self.min_safe_level_m3:
            return False
        return True

    def reserve(self, volume: float):
        if volume > self.inventory_m3 - self.min_safe_level_m3:
            raise ValueError("库存不足，无法预留该体积")
        self.inventory_m3 -= volume
        self.status = "RESERVED"

    def release(self):
        self.status = "AVAILABLE"


class Order(Base):
    __tablename__ = 'orders'

    order_id = Column(String(50), primary_key=True)
    customer = Column(String(100))
    oil_type = Column(String(50))
    required_volume_m3 = Column(Float)
    earliest_start = Column(DateTime)
    deadline = Column(DateTime)
    priority = Column(Integer)
    allow_multi_tank = Column(Boolean, default=True)
    preferred_branches = Column(JSON, default=list)
    status = Column(String(50), default="PENDING")


class Branch(Base):
    __tablename__ = 'branches'

    branch_id = Column(String(50), primary_key=True)
    max_rate_m3h = Column(Float)
    stop_windows = Column(JSON, default=list)
    status = Column(String(20), default="AVAILABLE")


class Pipeline(Base):
    __tablename__ = 'pipeline'

    trunk_id = Column(String(50), primary_key=True, default=lambda: f"TR-{uuid.uuid4().hex[:6]}")
    max_rate_m3h = Column(Float)


class Plan(Base):
    __tablename__ = 'plans'

    plan_id = Column(String(50), primary_key=True)
    order_id = Column(String(50), ForeignKey('orders.order_id'))
    branch_id = Column(String(50), ForeignKey('branches.branch_id'))
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    rate_m3h = Column(Float)
    status = Column(String(20), default="DRAFT")

    order = relationship('Order')
    branch = relationship('Branch')


# === ORM Session 工具 ===
def init_db(db_url: str = 'sqlite:///pipeline_batch.db'):
    engine = create_engine(db_url, echo=False, future=True)
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)
    return SessionLocal()


if __name__ == "__main__":
    SessionLocal = init_db()
    session = SessionLocal()

    # 添加示例数据
    t1 = Tank(tank_id="T101", owner="华星", oil_type="MURBAN", inventory_m3=35000,
              max_capacity_m3=50000, min_safe_level_m3=2000,
              compatible_oils=["MURBAN"], available_from=datetime.now())
    session.add(t1)

    o1 = Order(order_id="ORD-001", customer="华星", oil_type="MURBAN", required_volume_m3=15000,
               earliest_start=datetime.now(), deadline=datetime.now()+timedelta(days=2), priority=1)
    session.add(o1)

    session.commit()

    print("已保存油罐和订单到数据库。")

