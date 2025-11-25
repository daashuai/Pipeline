from abc import ABC, abstractmethod
from copy import deepcopy
from typing import List, Dict, Tuple, Optional, Any
import time
from data_class import Tank, Pipeline, CustomerOrder, DispatchOrder
from state import State
from scheduler import Scheduler
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import yaml
from datetime import datetime, timedelta
import uuid
from utils.database import load_table_data, load_table_as_objects
from dispatch_order_queue import DispatchOrderQueueManager

def main():
    """主函数：演示调度流程"""
    # 模拟滚动订单
    # 订单数据（含优先级）
    now = int(time.time())
    orders = [
        CustomerOrder(
            customer_order_id="1001",
            customer_id="CUST001",
            site_id="SITE003",
            customer_name="中石化加油站(北京朝阳)",
            oil_type="ESPO",
            required_volume=5000.0,
            dispatched_volume=3000.0,
            undispatched_volume=2000.0,
            start_time=datetime(2025, 11, 20, 8, 30),
            end_time=datetime(2025, 11, 25, 17, 0),
            priority=2,
            entry_tank_id="TANK-A101",
            finish_storage_tank_time=datetime(2025, 11, 21, 14, 20),
            branch_start_time=datetime(2025, 11, 21, 15, 0),
            branch_end_time=datetime(2025, 11, 23, 9, 30),
            status="PROCESSING"
        )
#         CustomerOrder(
#             customer_order_id="1002",
#             customer_id="ESPO",
#             site_id="SITE3",
#             customer_name="长城润滑油有限公司",
#             oil_type="润滑油基础油",
#             required_volume=8000.0,
#             dispatched_volume=0.0,
#             undispatched_volume=8000.0,
#             start_time=datetime(2025, 11, 22, 9, 0),
#             end_time=datetime(2025, 12, 5, 18, 0),
#             priority=1,
#             entry_tank_id="TANK-B205",
#             status="PENDING"
#         )
    ]

    tanks = load_table_as_objects("Tank")
    pipelines = load_table_as_objects("Pipeline")
    branches = load_table_as_objects("Branch")
    base_state = State(tanks, pipelines, branches)
    queue = DispatchOrderQueueManager(base_state)
    
    # 2. 创建调度器（使用规则基础策略）
    scheduler = Scheduler()
    
    # 3. 执行调度
    queue = scheduler.rolling_schedule(orders, queue)
    
    # 5. 优化指标
    print(f"优化指标:")
    print(f"- 油品切换次数: {base_state.oil_switch_count}")
    print(f"- 高优先级订单满足率: {base_state.high_priority_satisfied}/{len([o for o in orders if o.priority>=7])}")

if __name__ == "__main__":
    main()


