from collections import deque
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any, Union, Tuple
import uuid
import time
from dataclasses import dataclass, field
from data_class import DispatchOrder, Tank, Pipeline, Branch
from copy import deepcopy
from state import State

class DispatchOrderQueueManager:
    """
    调度工单队列管理器
    管理所有调度工单，不依赖特定站点
    支持在初始化时传入工单列表
    """
    
    def __init__(self, 
                 real_system_state: State,
                 dispatch_orders: Optional[List[Dict[str, Any]]] = None,
                 ):
        """
        初始化调度工单队列管理器
        
        Args:
            real_system_state: 真实系统状态
            dispatch_orders: 初始调度工单列表，每个工单是字典格式
            default_flow_rate: 默认流量(立方米/小时)
        """
        self.queue = deque()
        self.order_registry: Dict[str, DispatchOrder] = {}
        self.default_flow_rate = 500
        
        # 真实系统状态 - 反映当前实际系统状态
        self.real_system_state = real_system_state
        
        # 虚拟状态映射 - 每个dispatch order对应一个虚拟状态
        self.virtual_state_map: Dict[str, 'State'] = {}
        
        # 状态链 - 按顺序记录状态演进
        self.state_chain: List[Tuple[str, 'State']] = []  # (dispatch_order_id, state)
        
        self.last_calculation_time = int(time.time())
        
        # 处理初始调度工单
        if dispatch_orders:
            self._initialize_from_orders(dispatch_orders)
    
    def _generate_order_id(self) -> str:
        """生成唯一订单ID"""
        return f"DISPATCH_{int(time.time())}_{uuid.uuid4().hex[:8]}"
    
    def _estimate_duration(self, volume: float, oil_type: str = None) -> int:
        """
        估算执行时长(秒)
        根据油品种类和体积计算
        """
        if volume <= 0:
            return 0
            
        flow_rate = self.default_flow_rate
        
        # 根据油品类型调整流速
        flow_rate_modifiers = {
            "heavy_oil": 0.7,    # 重油流速较慢
            "bitumen": 0.6,      # 沥青更慢
            "gasoline": 1.1,     # 汽油流速较快
            "diesel": 1.1,       # 柴油流速较快
            "jetfuel": 1.05,     # 航煤略快
        }
        
        modifier = flow_rate_modifiers.get(oil_type.lower(), 1.0) if oil_type else 1.0
        flow_rate *= modifier
        
        # 计算小时数，转换为秒
        hours = volume / flow_rate
        return max(60, int(hours * 3600))  # 最少1分钟
    
    def _initialize_from_orders(self, orders: List[Dict[str, Any]]):
        """
        从初始订单字典列表初始化队列
        """
        if not orders:
            return
        
        # 转换字典为 DispatchOrder 对象
        dispatch_orders = []
        for order_dict in orders:
            dispatch_order = self._create_dispatch_order_from_dict(order_dict)
            dispatch_orders.append(dispatch_order)
        
        # 按开始时间排序，如果开始时间为0则按优先级排序
        dispatch_orders.sort(key=lambda x: (
            x.start_time if x.start_time > 0 else float('inf'),
            -x.priority  # 高优先级排在前面
        ))
        
        # 验证和修复时间安排
        self._validate_and_fix_schedule(dispatch_orders)
        
        # 添加到队列和注册表
        for order in dispatch_orders:
            self.queue.append(order)
            self.order_registry[order.dispatch_order_id] = order
            
            # 为每个工单创建对应的虚拟状态
            self._create_virtual_state_for_order(order)
    
    def _create_dispatch_order_from_dict(self, order_dict: Dict[str, Any]) -> DispatchOrder:
        """从字典创建 DispatchOrder 对象"""
        # 生成ID如果不存在
        dispatch_order_id = order_dict.get('dispatch_order_id') or self._generate_order_id()
        
        # 提取必要字段，使用默认值填充缺失字段
        customer_order_id = order_dict.get('customer_order_id', "")
        site_id = order_dict.get('site_id', "")
        oil_type = order_dict.get('oil_type', "")
        required_volume = float(order_dict.get('required_volume', 0.0))
        source_tank_id = order_dict.get('source_tank_id', "")
        target_tank_id = order_dict.get('target_tank_id', "")
        
        # 可选字段
        pipeline_path = order_dict.get('pipeline_path', [])
        status = order_dict.get('status', "DRAFT")
        cleaning_required = bool(order_dict.get('cleaning_required', False))
        priority = int(order_dict.get('priority', 1))
        notes = order_dict.get('notes', "")
        
        # 处理时间
        start_time = int(order_dict.get('start_time', 0))
        end_time = int(order_dict.get('end_time', 0))
        created_at = int(order_dict.get('created_at', time.time()))
        
        # 如果有体积但没有时间，估算时间
        if required_volume > 0 and (start_time <= 0 or end_time <= start_time):
            if start_time <= 0:
                # 如果队列为空，使用当前时间，否则使用最后一个订单的结束时间
                if not self.queue:
                    start_time = int(time.time())
                else:
                    last_order = list(self.queue)[-1]
                    start_time = last_order.end_time if last_order.end_time > 0 else int(time.time())
            
            # 估算持续时间
            duration = self._estimate_duration(required_volume, oil_type)
            end_time = start_time + duration
        
        return DispatchOrder(
            dispatch_order_id=dispatch_order_id,
            customer_order_id=customer_order_id,
            site_id=site_id,
            oil_type=oil_type,
            required_volume=required_volume,
            source_tank_id=source_tank_id,
            target_tank_id=target_tank_id,
            pipeline_path=pipeline_path,
            start_time=start_time,
            end_time=end_time,
            status=status,
            cleaning_required=cleaning_required,
            priority=priority,
            created_at=created_at,
            notes=notes
        )
    
    def _validate_and_fix_schedule(self, orders: List[DispatchOrder]):
        """
        验证和修复时间安排
        确保订单时间不重叠，按顺序排列
        """
        if not orders:
            return
        
        current_time = int(time.time())
        
        # 第一个订单的开始时间不能早于当前时间
        first_order = orders[0]
        if first_order.start_time <= 0 or first_order.start_time < current_time:
            first_order.start_time = current_time
            if first_order.end_time <= first_order.start_time and first_order.required_volume > 0:
                duration = self._estimate_duration(first_order.required_volume, first_order.oil_type)
                first_order.end_time = first_order.start_time + duration
        
        # 修复后续订单
        for i in range(1, len(orders)):
            prev_order = orders[i-1]
            curr_order = orders[i]
            
            # 确保当前订单在前一个订单结束后开始
            expected_start = prev_order.end_time
            if curr_order.start_time < expected_start or curr_order.start_time <= 0:
                curr_order.start_time = expected_start
            
            # 重新计算结束时间
            if curr_order.end_time <= curr_order.start_time and curr_order.required_volume > 0:
                duration = self._estimate_duration(curr_order.required_volume, curr_order.oil_type)
                curr_order.end_time = curr_order.start_time + duration
    
    def _create_virtual_state_for_order(self, dispatch_order: DispatchOrder) -> 'State':
        """为调度工单创建对应的虚拟状态"""
        # 获取前一个状态（如果没有则使用真实系统状态）
        if self.state_chain:
            prev_state = self.state_chain[-1][1]
        else:
            prev_state = self.real_system_state
        
        # 创建新状态并应用调度工单
        new_state = prev_state.apply_dispatch_order(dispatch_order.__dict__)
        
        # 添加到状态链
        self.state_chain.append((dispatch_order.dispatch_order_id, new_state))
        
        # 添加到虚拟状态映射
        self.virtual_state_map[dispatch_order.dispatch_order_id] = new_state
        
        return new_state

    def add_order(self, dispatch_order: DispatchOrder) -> str:
        """
        添加调度订单到队列尾部，并创建对应的虚拟状态
        
        Args:
            dispatch_order: 调度订单对象
            
        Returns:
            调度订单ID
        """
        # 设置订单状态为已调度
        # if dispatch_order.status == "DRAFT":
        dispatch_order.status = "SCHEDULED"
        
        # 添加到队列
        self.queue.append(dispatch_order)
        self.order_registry[dispatch_order.dispatch_order_id] = dispatch_order
        
        # 为新订单创建虚拟状态
        self._create_virtual_state_for_order(dispatch_order)
        
        return dispatch_order.dispatch_order_id
    


    
    
    def insert_order_at_position(self, position: int, customer_order_id: str, oil_type: str,
                               required_volume: float, source_tank_id: str, target_tank_id: str,
                               site_id: str, pipeline_path: Optional[List[str]] = None,
                               cleaning_required: bool = False, priority: int = 1, 
                               start_time: int = 0, notes: str = "") -> str:
        """
        在指定位置插入调度订单，并重新计算状态链
        """
        position = max(0, min(position, len(self.queue)))
        dispatch_order_id = self._generate_order_id()
        duration = self._estimate_duration(required_volume, oil_type)
        
        # 确定插入点的开始时间
        if position == 0:
            # 插入到队首
            if not self.queue:
                start_time = start_time if start_time > 0 else int(time.time())
            else:
                # 队首插入时，如果未指定开始时间，使用当前时间
                if start_time <= 0:
                    start_time = max(int(time.time()), self.queue[0].start_time)
        else:
            # 插入到中间位置，使用前一个订单的结束时间
            if start_time <= 0:
                prev_order = list(self.queue)[position - 1]
                start_time = prev_order.end_time
        
        end_time = start_time + duration
        
        dispatch_order = DispatchOrder(
            dispatch_order_id=dispatch_order_id,
            customer_order_id=customer_order_id,
            site_id=site_id,
            oil_type=oil_type,
            required_volume=required_volume,
            source_tank_id=source_tank_id,
            target_tank_id=target_tank_id,
            pipeline_path=pipeline_path or [],
            start_time=start_time,
            end_time=end_time,
            status="SCHEDULED",
            cleaning_required=cleaning_required,
            priority=priority,
            notes=notes
        )
        
        # 将队列转换为列表以便插入
        queue_list = list(self.queue)
        queue_list.insert(position, dispatch_order)
        self.queue = deque(queue_list)
        self.order_registry[dispatch_order_id] = dispatch_order
        
        # 重新计算整个状态链
        self._recalculate_state_chain()
        
        return dispatch_order_id
    
    def _recalculate_state_chain(self):
        """重新计算整个状态链"""
        # 清空现有的状态链和虚拟状态映射
        self.state_chain = []
        self.virtual_state_map = {}
        
        # 从真实系统状态开始，逐步应用每个订单
        current_state = deepcopy(self.real_system_state)
        
        # 按队列顺序重新应用所有订单
        for order in self.queue:
            # 应用订单到当前状态
            new_state = current_state.apply_dispatch_order(order.__dict__)
            
            # 添加到状态链
            self.state_chain.append((order.dispatch_order_id, new_state))
            
            # 添加到虚拟状态映射
            self.virtual_state_map[order.dispatch_order_id] = new_state
            
            # 更新当前状态
            current_state = new_state
    
    def remove_order(self, dispatch_order_id: str) -> bool:
        """移除指定订单并重新计算状态链"""
        if dispatch_order_id not in self.order_registry:
            return False
        
        # 从队列中移除
        new_queue = deque()
        for order in self.queue:
            if order.dispatch_order_id != dispatch_order_id:
                new_queue.append(order)
        
        self.queue = new_queue
        del self.order_registry[dispatch_order_id]
        
        # 从虚拟状态映射中移除
        if dispatch_order_id in self.virtual_state_map:
            del self.virtual_state_map[dispatch_order_id]
        
        # 重新计算整个状态链
        self._recalculate_state_chain()
        
        return True
    
    def get_order_state_last(self) -> Optional['State']:
        """
        获取队列最新订单的状态
        
        Returns:
            最新订单对应的状态，如果队列为空则返回None
        """
        if not self.queue:
            # 如果队列为空，返回真实系统状态
            return self.real_system_state
        
        # 获取队列中的最后一个订单
        last_order = list(self.queue)[-1]
        last_order_id = last_order.dispatch_order_id
        
        # 返回该订单对应的虚拟状态
        return self.virtual_state_map.get(last_order_id)

    def get_virtual_state_for_order(self, dispatch_order_id: str) -> Optional['State']:
        """获取指定调度工单对应的虚拟状态"""
        return self.virtual_state_map.get(dispatch_order_id)
    
    def get_real_system_state(self) -> 'State':
        """获取真实系统状态"""
        return self.real_system_state
    
    def update_real_system_state_with_order(self, dispatch_order_id: str):
        """将指定调度工单的状态应用到真实系统状态"""
        if dispatch_order_id in self.order_registry:
            order = self.order_registry[dispatch_order_id]
            
            # 应用订单到真实系统状态
            self.real_system_state = self.real_system_state.apply_dispatch_order(order.__dict__)
            
            # 从队列和状态映射中移除已完成的订单
            if dispatch_order_id in self.order_registry:
                del self.order_registry[dispatch_order_id]
            
            if dispatch_order_id in self.virtual_state_map:
                del self.virtual_state_map[dispatch_order_id]
            
            # 从队列中移除订单
            new_queue = deque()
            for order in self.queue:
                if order.dispatch_order_id != dispatch_order_id:
                    new_queue.append(order)
            self.queue = new_queue
            
            # 重新计算状态链（基于剩余订单）
            self._recalculate_state_chain()
    
    def get_state_chain(self) -> List[Tuple[str, 'State']]:
        """获取状态链"""
        return self.state_chain[:]
    
    def get_virtual_states(self) -> Dict[str, 'State']:
        """获取所有虚拟状态映射"""
        return self.virtual_state_map.copy()
    
    def get_next_order(self) -> Optional[DispatchOrder]:
        """获取下一个待执行的订单"""
        if not self.queue:
            return None
        
        current_time = int(time.time())
        first_order = self.queue[0]
        
        # 检查第一个订单是否已经开始
        if first_order.start_time <= current_time:
            return first_order
        
        return None
    
    def complete_order(self, dispatch_order_id: str) -> bool:
        """完成指定订单，更新真实系统状态"""
        if dispatch_order_id not in self.order_registry:
            return False
        
        order = self.order_registry[dispatch_order_id]
        
        # 确认是队首订单
        if not self.queue or self.queue[0].dispatch_order_id != dispatch_order_id:
            return False
        
        # 更新真实系统状态
        self.update_real_system_state_with_order(dispatch_order_id)
        
        return True
    
    def cancel_order(self, dispatch_order_id: str) -> bool:
        """取消指定订单"""
        if dispatch_order_id not in self.order_registry:
            return False
        
        order = self.order_registry[dispatch_order_id]
        order.status = "CANCELLED"
        
        # 从队列中移除
        return self.remove_order(dispatch_order_id)
    
    def move_order(self, dispatch_order_id: str, new_position: int) -> bool:
        """移动订单到新位置并重新计算状态链"""
        if dispatch_order_id not in self.order_registry:
            return False
        
        if new_position < 0 or new_position > len(self.queue):
            new_position = max(0, min(new_position, len(self.queue)))
        
        # 找到当前订单位置
        current_position = -1
        for i, order in enumerate(self.queue):
            if order.dispatch_order_id == dispatch_order_id:
                current_position = i
                break
        
        if current_position == -1:
            return False
        
        if current_position == new_position:
            return True
        
        # 重新排列队列
        queue_list = list(self.queue)
        order = queue_list.pop(current_position)
        queue_list.insert(new_position, order)
        self.queue = deque(queue_list)
        
        # 重新计算整个状态链
        self._recalculate_state_chain()
        
        return True
    
    def get_queue_status(self) -> Dict[str, Any]:
        """获取队列状态摘要"""
        current_time = int(time.time())
        return {
            "total_orders": len(self.queue),
            "next_order_id": self.queue[0].dispatch_order_id if self.queue else None,
            "estimated_completion_time": self._get_queue_completion_time(),
            "orders": [self._order_to_dict(order) for order in self.queue],
            "is_idle": not any(order.start_time <= current_time <= order.end_time for order in self.queue) if self.queue else True,
            "real_system_state_info": {
                "oil_switch_count": self.real_system_state.oil_switch_count,
                "total_volume_dispatched": self.real_system_state.total_volume_dispatched,
                "total_dispatch_orders": self.real_system_state.total_dispatch_orders
            }
        }
    
    def _get_queue_completion_time(self) -> int:
        """获取队列预计完成时间(时间戳)"""
        if not self.queue:
            return int(time.time())
        
        last_order = list(self.queue)[-1]
        return last_order.end_time
    
    def _order_to_dict(self, order: DispatchOrder) -> Dict[str, Any]:
        """将订单转换为字典，用于API响应"""
        return {
            "dispatch_order_id": order.dispatch_order_id,
            "customer_order_id": order.customer_order_id,
            "site_id": order.site_id,
            "oil_type": order.oil_type,
            "required_volume": order.required_volume,
            "source_tank_id": order.source_tank_id,
            "target_tank_id": order.target_tank_id,
            "pipeline_path": order.pipeline_path,
            "start_time": order.start_time,
            "end_time": order.end_time,
            "status": order.status,
            "cleaning_required": order.cleaning_required,
            "priority": order.priority,
            "created_at": order.created_at,
            "notes": order.notes,
            "has_virtual_state": order.dispatch_order_id in self.virtual_state_map,
            "start_time_formatted": datetime.fromtimestamp(order.start_time).strftime("%Y-%m-%d %H:%M:%S") if order.start_time > 0 else None,
            "end_time_formatted": datetime.fromtimestamp(order.end_time).strftime("%Y-%m-%d %H:%M:%S") if order.end_time > 0 else None,
            "duration_minutes": (order.end_time - order.start_time) // 60 if order.end_time > order.start_time else 0
        }
    
    def get_gantt_chart_data(self) -> List[Dict[str, Any]]:
        """获取甘特图数据"""
        current_time = int(time.time())
        chart_data = []
        
        for order in self.queue:
            # 评估订单状态
            status = order.status
            if status == "SCHEDULED":
                if order.start_time <= current_time <= order.end_time:
                    status = "RUNNING"
            
            chart_data.append({
                "id": order.dispatch_order_id,
                "task": f"{order.oil_type} ({order.required_volume}m³)",
                "site": order.site_id,
                "start_time": order.start_time,
                "end_time": order.end_time,
                "status": status,
                "priority": order.priority,
                "has_virtual_state": order.dispatch_order_id in self.virtual_state_map,
                "color": self._get_status_color(status),
                "label": f"{order.customer_order_id[:8]}... [{order.site_id}]"
            })
        
        return chart_data
    
    def _get_status_color(self, status: str) -> str:
        """根据状态获取颜色"""
        colors = {
            "DRAFT": "#6c757d",
            "SCHEDULED": "#17a2b8",
            "RUNNING": "#28a745",
            "COMPLETED": "#6c757d",
            "CANCELLED": "#dc3545",
            "CONFLICT": "#ffc107"
        }
        return colors.get(status, "#007bff")
    
    def validate_queue(self) -> Tuple[bool, List[str]]:
        """
        验证队列中的所有订单
        返回(是否有效, 错误消息列表)
        """
        errors = []
        current_time = int(time.time())
        
        # 检查重复订单
        seen_ids = set()
        for order in self.queue:
            if order.dispatch_order_id in seen_ids:
                errors.append(f"重复的订单ID: {order.dispatch_order_id}")
            seen_ids.add(order.dispatch_order_id)
        
        # 检查时间冲突
        prev_end_time = 0
        for i, order in enumerate(self.queue):
            # 检查时间是否有效
            if order.start_time <= 0:
                errors.append(f"订单 {order.dispatch_order_id} 的开始时间无效")
            
            if order.end_time <= order.start_time:
                errors.append(f"订单 {order.dispatch_order_id} 的结束时间早于或等于开始时间")
            
            # 检查时间重叠
            if order.start_time < prev_end_time:
                errors.append(f"订单 {order.dispatch_order_id} 与前一订单时间重叠")
            
            prev_end_time = order.end_time
        
        # 检查过期订单
        for order in self.queue:
            if order.end_time < current_time and order.status in ["DRAFT", "SCHEDULED"]:
                errors.append(f"订单 {order.dispatch_order_id} 已过期但状态为 {order.status}")
        
        return (len(errors) == 0, errors)
    
    def reschedule_from_current_time(self):
        """从当前时间重新安排所有订单并重新计算状态"""
        current_time = int(time.time())
        self.last_calculation_time = current_time
        
        # 重新计算时间安排
        queue_list = list(self.queue)
        current_start_time = current_time
        
        for order in queue_list:
            order.start_time = current_start_time
            duration = self._estimate_duration(order.required_volume, order.oil_type)
            order.end_time = current_start_time + duration
            current_start_time = order.end_time
        
        # 重新计算状态链
        self._recalculate_state_chain()
        
        return len(queue_list)
    
    def get_orders_by_site(self, site_id: str) -> List[DispatchOrder]:
        """获取指定站点的所有订单"""
        return [order for order in self.queue if order.site_id == site_id]
    
    def get_orders_by_status(self, status: str) -> List[DispatchOrder]:
        """获取指定状态的所有订单"""
        return [order for order in self.queue if order.status == status]
    
    def get_conflicting_orders(self) -> List[Tuple[DispatchOrder, DispatchOrder]]:
        """获取所有时间冲突的订单对"""
        conflicts = []
        orders = list(self.queue)
        
        for i in range(len(orders)):
            for j in range(i + 1, len(orders)):
                order1 = orders[i]
                order2 = orders[j]
                
                # 检查时间重叠
                if not (order1.end_time <= order2.start_time or order2.end_time <= order1.start_time):
                    conflicts.append((order1, order2))
        
        return conflicts
    
    def clear_completed_orders(self):
        """清理已完成的订单"""
        to_remove = []
        for order in self.queue:
            if order.status == "COMPLETED":
                to_remove.append(order.dispatch_order_id)
        
        for order_id in to_remove:
            self.remove_order(order_id)
    
    def __str__(self) -> str:
        """返回队列的字符串表示"""
        status = self.get_queue_status()
        return f"DispatchOrderQueueManager(orders={status['total_orders']}, completion_time={status['estimated_completion_time']})"
    
    def __len__(self) -> int:
        """返回队列中订单数量"""
        return len(self.queue)




