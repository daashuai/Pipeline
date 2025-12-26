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
        ),
        CustomerOrder(
            customer_order_id="1002",
            customer_id="CUST002",
            site_id="SITE004",
            customer_name="中石油储备库(天津)",
            oil_type="ESPO",
            required_volume=8000.0,
            dispatched_volume=0.0,
            undispatched_volume=8000.0,
            start_time=datetime(2025, 11, 22, 9, 0),
            end_time=datetime(2025, 11, 28, 18, 0),
            priority=1,
            entry_tank_id="TANK-A102",
            status="PROCESSING"
        ),
        CustomerOrder(
            customer_order_id="1003",
            customer_id="CUST003",
            site_id="SITE003",
            customer_name="壳牌润滑油厂",
            oil_type="ESPO",
            required_volume=4000.0,
            dispatched_volume=0.0,
            undispatched_volume=4000.0,
            start_time=datetime(2025, 11, 23, 10, 0),
            end_time=datetime(2025, 11, 26, 12, 0),
            priority=3,
            entry_tank_id="TANK-A101",
            status="PROCESSING"
        ),
        CustomerOrder(
            customer_order_id="1005",
            customer_id="CUST005",
            site_id="SITE001",
            customer_name="壳牌润滑油厂",
            oil_type="ESPO",
            required_volume=3000.0,
            dispatched_volume=1000.0,
            undispatched_volume=2000.0,
            start_time=datetime(2025, 11, 23, 10, 0),
            end_time=datetime(2025, 11, 26, 12, 0),
            priority=3,
            entry_tank_id="TANK-A101",
            status="PROCESSING"
        )
    #     CustomerOrder(
    #         customer_order_id="1004",
    #         customer_id="ESPO",
    #         site_id="SITE3",
    #         customer_name="长城润滑油有限公司",
    #         oil_type="润滑油基础油",
    #         required_volume=8000.0,
    #         dispatched_volume=0.0,
    #         undispatched_volume=8000.0,
    #         start_time=datetime(2025, 11, 22, 9, 0),
    #         end_time=datetime(2025, 12, 5, 18, 0),
    #         priority=4,
    #         entry_tank_id="TANK-B205",
    #         status="PENDING"
    #     )
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

    # for dispatch_order in queue:
    #     print(dispatch_order)

    # 1. 定义表头，增加 Status, Start, End 列
    # <10 表示左对齐占10格，<5 表示左对齐占5格
    header = (
        f"{'ID':<10} | {'Cust':<6} | {'Vol':<8} | "
        f"{'Status':<10} | {'StartTime':<16} | {'EndTime':<16} | "
        f"{'Route (Src->Tgt)':<20} | {'Path Info'}"
    )

    print(header)
    print("-" * 150) # 加长横线以适应更宽的内容

    for order in queue:
        # 处理路径字符串
        path_str = " -> ".join(order.pipeline_path) if order.pipeline_path else "No Path"

        start_time = datetime.fromtimestamp(order.start_time).strftime('%Y-%m-%d %H:%M') if order.start_time else "N/A"
        end_time = datetime.fromtimestamp(order.end_time).strftime('%Y-%m-%d %H:%M') if order.end_time else "N/A"
        
        # 2. 打印每一行数据，对应上面的表头
        print(f"{order.dispatch_order_id:<10} | "
            f"{order.customer_order_id:<6} | "
            f"{order.required_volume:<8.1f} | "
            f"{order.status:<10} | "       # 新增：状态
            f"{start_time:<10} | "    # 新增：开始时间
            f"{end_time:<10} | "      # 新增：结束时间
            f"{order.source_tank_id}->{order.target_tank_id:<11} | "
            f"{path_str}")
    
    # 5. 优化指标
    print(f"优化指标:")
    print(f"- 油品切换次数: {base_state.oil_switch_count}")
    print(f"- 高优先级订单满足率: {base_state.high_priority_satisfied}/{len([o for o in orders if o.priority>=7])}")

if __name__ == "__main__":
    main()


