from abc import ABC, abstractmethod
from copy import deepcopy
from typing import List, Dict, Tuple, Optional, Any
import time
from models.models import Tank, Pipeline, CustomerOrder, DispatchOrder
from state import TankState, PipelineState, SchedulingState
from scheduler import PipelineScheduler
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import yaml
from datetime import datetime, timedelta
import uuid
from utils.database import load_table_data

def main():
    """主函数：演示调度流程"""
    
    # 模拟滚动订单
    # 订单数据（含优先级）
    now = int(time.time())
    orders = [
        CustomerOrder("order1", "custA", "oilA", 200, (now, now + 8*3600), 9, "tank2"),
        CustomerOrder("order2", "custB", "oilB", 300, (now, now + 12*3600), 7, "tank3"),
        CustomerOrder("order3", "custC", "oilA", 400, (now + 4*3600, now + 24*3600), 5, "tank2"),
        CustomerOrder("order4", "custD", "oilC", 150, (now + 2*3600, now + 6*3600), 8, "tank3")
    ]
    tanks = load_table_data("Tank")
    pipelines = load_table_data("Pipeline")

    base_state = SchedulingState(tanks, pipelines)
    
    # 2. 创建调度器（使用规则基础策略）
    scheduler = PipelineScheduler()
    
    # 3. 执行调度
    dispatch_orders, failed_orders = scheduler.rolling_schedule(orders, base_state)
    
    # 4. 输出结果
    print(f"成功调度 {len(dispatch_orders)-len(failed_orders)} 个工单，{len(failed_orders)} 个冲突")
    print("-" * 50)
    
    for dispatch_order in dispatch_orders:
        status = "✓ SUCCESS" if dispatch_order.status == "DRAFT" else "✗ CONFLICT"
        path_str = " -> ".join(dispatch_order.pipeline_path) if dispatch_order.pipeline_path else "N/A"
        print(f"[{status}] 订单 {dispatch_order.order_id} (优先级 {next(o.priority for o in orders if o.id==dispatch_order.order_id)}):")
        print(f"  油品: {dispatch_order.oil_type}, 数量: {dispatch_order.quantity}吨")
        print(f"  路径: {dispatch_order.source_tank_id} -> {path_str} -> {dispatch_order.target_tank_id}")
        print(f"  时间: {time.strftime('%Y-%m-%d %H:%M', time.localtime(dispatch_order.start_time))} "
              f"至 {time.strftime('%Y-%m-%d %H:%M', time.localtime(dispatch_order.end_time))}")
        if dispatch_order.cleaning_required:
            print("  ** 需要清洗 **")
        print("-" * 50)
    
    # 5. 优化指标
    print(f"优化指标:")
    print(f"- 油品切换次数: {base_state.oil_switch_count}")
    print(f"- 高优先级订单满足率: {base_state.high_priority_satisfied}/{len([o for o in orders if o.priority>=7])}")

if __name__ == "__main__":
    main()


