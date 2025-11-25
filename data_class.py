# -*- coding: utf-8 -*-
"""
批次管输调度系统 - 业务对象模型
纯业务对象，不依赖 ORM 框架，包含业务逻辑。
"""
from datetime import datetime
from typing import List, Dict, Optional
from dataclasses import dataclass,field


@dataclass
class Tank:
    """
    油罐业务对象
    """
    tank_id: str
    site_id: str
    tank_name: str = ""
    tank_area: str = ""
    oil_type: str = ""
    inventory: float = 0.0
    current_level: float = 0.0
    tank_capacity_per_meter: float = 0.0
    maximum_tank_capacity: float = 0.0
    safe_tank_capacity: float = 0.0
    maximum_tank_level: float = 0.0
    safe_tank_level: float = 0.0
    min_safe_level: float = 0.0
    tank_type: list = field(default_factory=lambda: ["TARGET", "MIDDLE"])
    status: str = "AVAILABLE"

    def can_supply(self, oil_type: str, required_volume: float) -> bool:
        """检查是否可以供应指定油种和体积"""
        if self.status != "AVAILABLE":
            return False
        if self.inventory - required_volume < self.min_safe_level:
            return False
        return True

    def reserve(self, volume: float):
        """预留指定体积的油"""
        if volume > self.inventory - self.min_safe_level:
            raise ValueError("库存不足，无法预留该体积")
        self.inventory -= volume
        self.status = "RESERVED"

    def release(self):
        """释放预留状态"""
        self.status = "AVAILABLE"

    def to_db(self):
        """将业务对象转换为 SQLAlchemy 模型"""
        from models import TankDB
        return TankDB(
            tank_id=self.tank_id,
            tank_name=self.tank_name,
            tank_area=self.tank_area,
            oil_type=self.oil_type,
            inventory=self.inventory,
            current_level=self.current_level,
            tank_capacity_per_meter=self.tank_capacity_per_meter,
            maximum_tank_capacity=self.maximum_tank_capacity,
            safe_tank_capacity=self.safe_tank_capacity,
            maximum_tank_level=self.maximum_tank_level,
            safe_tank_level=self.safe_tank_level,
            min_safe_level=self.min_safe_level,
            tank_type=self.tank_type,
            status=self.status
        )


@dataclass
class Customer:
    """
    客户业务对象
    """
    customer_id: str
    customer_name: str = ""

    def to_db(self):
        """将业务对象转换为 SQLAlchemy 模型"""
        from models import CustomerDB
        return CustomerDB(
            customer_id=self.customer_id,
            customer_name=self.customer_name
        )


@dataclass
class CustomerOrder:
    """
    客户订单业务对象
    """
    customer_order_id: str = ""
    customer_id: str = ""
    site_id: str=""
    customer_name: str = ""
    oil_type: str = ""
    required_volume: float = 0.0
    dispatched_volume: float = 0.0
    undispatched_volume: float = 0.0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    priority: int = 1
    entry_tank_id: str = ""
    finish_storage_tank_time: Optional[datetime] = None
    branch_start_time: Optional[datetime] = None
    branch_end_time: Optional[datetime] = None
    status: str = "PENDING"

    def is_fully_scheduled(self) -> bool:
        """检查是否已完全调度"""
        if self.required_volume == 0:
            return True
        return abs(self.dispatched_volume - self.required_volume) < 1e-6

    def calculate_undispatched_volume(self) -> float:
        """计算未调度体积"""
        return max(0, self.required_volume - self.dispatched_volume)

    def update_undispatched_volume(self):
        """更新未调度体积"""
        self.undispatched_volume = self.calculate_undispatched_volume()

    def is_complete(self) -> bool:
        """检查订单是否已完成"""
        return self.status == "COMPLETED" or self.is_fully_scheduled()

    def to_db(self):
        """将业务对象转换为 SQLAlchemy 模型"""
        from models import CustomerOrderDB
        return CustomerOrderDB(
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


@dataclass
class DispatchOrder:
    """
    调度订单业务对象
    """
    dispatch_order_id: str = ""
    customer_order_id: str = ""
    site_id: str = ""
    oil_type: str = ""
    required_volume: float = 0.0
    source_tank_id: str = ""
    target_tank_id: str = ""
    pipeline_path: Optional[List[str]] = None
    start_time: int = 0
    end_time: int = 0
    status: str = "DRAFT"  # 状态: DRAFT/SCHEDULED/RUNNING/COMPLETED/CONFLICT
    cleaning_required: bool = False

    def is_scheduled(self) -> bool:
        """检查是否已调度"""
        return self.status in ["SCHEDULED", "RUNNING", "COMPLETED"]

    def is_running(self) -> bool:
        """检查是否正在运行"""
        return self.status == "RUNNING"

    def is_completed(self) -> bool:
        """检查是否已完成"""
        return self.status == "COMPLETED"

    def to_db(self):
        """将业务对象转换为 SQLAlchemy 模型"""
        from models import DispatchOrderDB
        return DispatchOrderDB(
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


@dataclass
class Site:
    """
    站点业务对象
    """
    site_id: str
    site_name: str = ""

    def to_db(self):
        """将业务对象转换为 SQLAlchemy 模型"""
        from models import SiteDB
        return SiteDB(
            site_id=self.site_id,
            site_name=self.site_name
        )


@dataclass
class Pipeline:
    """
    管道业务对象
    """
    pipe_id: str
    pipe_name: str = ""
    pipe_capacity_per_meter: float = 0.0
    pipe_shutdown_start_time: Optional[datetime] = None
    pipe_shutdown_end_time: Optional[datetime] = None
    pipe_shutdown_reason: str = ""

    def to_db(self):
        """将业务对象转换为 SQLAlchemy 模型"""
        from models import PipelineDB
        return PipelineDB(
            pipe_id=self.pipe_id,
            pipe_name=self.pipe_name,
            pipe_capacity_per_meter=self.pipe_capacity_per_meter,
            pipe_shutdown_start_time=self.pipe_shutdown_start_time,
            pipe_shutdown_end_time=self.pipe_shutdown_end_time,
            pipe_shutdown_reason=self.pipe_shutdown_reason
        )


@dataclass
class Branch:
    """
    管线分支业务对象
    """
    branch_id: str = ""
    from_id: str = ""
    to_id: str = ""
    is_direct_connection: bool = False
    branch_name: str = ""
    branch_mileage: float = 0.0
    branch_elevation: float = 0.0
    branch_capacity: float = 0.0
    is_begin: str = ""
    is_end: str = ""
    is_middle: str = ""

    def to_db(self):
        """将业务对象转换为 SQLAlchemy 模型"""
        from models import BranchDB
        return BranchDB(
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


@dataclass
class Oil:
    """
    油品业务对象
    """
    id: Optional[int] = None
    oil_name: str = ""
    oil_id: str = ""
    p20: float = 0.0
    freezing_point: float = 0.0
    h2s: str = ""
    kinematic_viscosity: str = ""
    place_of_origin: str = ""
    transfer_way: str = ""

    def to_db(self):
        """将业务对象转换为 SQLAlchemy 模型"""
        from models import OilDB
        return OilDB(
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




