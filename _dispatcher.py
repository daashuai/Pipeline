from typing import List, Dict, Tuple, Optional, Any
from datetime import datetime, timedelta
import json

class Dispatcher:
    """调度工单管理器 - 专门管理所有调度工单的生命周期"""
    
    def __init__(self, dispatch_orders: Optional[List[Dict[str, Any]]] = None):
        self.current_time = datetime.now()
        
        # 按状态分类存储调度工单
        self.completed_orders = {}  # 已完成: {dispatch_order_id: order_data}
        self.running_orders = {}    # 运输中: {dispatch_order_id: order_data}
        self.pending_orders = {}    # 待运输: {dispatch_order_id: order_data}
        self.conflict_orders = {}   # 冲突工单: {dispatch_order_id: order_data}
        
        # 所有工单的统一索引
        self.all_orders = {}  # {dispatch_order_id: order_data}
        
        # 初始化
        if dispatch_orders:
            for order_data in dispatch_orders:
                self.add_order(order_data)
    
    def add_order(self, order_data: Dict[str, Any]):
        """添加新的调度工单"""
        order_id = order_data.get('dispatch_order_id', '')
        if not order_id:
            raise ValueError("调度工单必须包含dispatch_order_id")
        
        # 检查是否已存在
        if order_id in self.all_orders:
            raise ValueError(f"调度工单 {order_id} 已存在")
        
        # 根据时间自动分类
        start_time = self._parse_datetime(order_data.get('start_time'))
        end_time = self._parse_datetime(order_data.get('end_time'))
        
        # 添加到统一索引
        self.all_orders[order_id] = order_data
        
        # 分类存储
        if end_time < self.current_time:
            # 已完成
            self.completed_orders[order_id] = order_data
        elif start_time <= self.current_time <= end_time:
            # 运输中
            self.running_orders[order_id] = order_data
        elif start_time > self.current_time:
            # 待运输
            self.pending_orders[order_id] = order_data
        else:
            # 异常情况，添加到冲突订单
            self.conflict_orders[order_id] = order_data
    
    def update_order(self, order_id: str, new_order_data: Dict[str, Any]):
        """更新调度工单（用于插单、调整等操作）"""
        # 从所有分类中移除旧工单
        self._remove_from_all_categories(order_id)
        
        # 用新数据添加工单
        new_order_data['dispatch_order_id'] = order_id
        self.add_order(new_order_data)
    
    def remove_order(self, order_id: str):
        """移除调度工单"""
        # 从所有分类中移除
        self._remove_from_all_categories(order_id)
        
        # 从统一索引中移除
        if order_id in self.all_orders:
            del self.all_orders[order_id]
    
    def _remove_from_all_categories(self, order_id: str):
        """从所有分类中移除指定工单"""
        for category in [self.completed_orders, self.running_orders, 
                        self.pending_orders, self.conflict_orders]:
            if order_id in category:
                del category[order_id]
    
    def get_order(self, order_id: str) -> Optional[Dict[str, Any]]:
        """获取指定调度工单"""
        return self.all_orders.get(order_id)
    
    def get_completed_orders(self) -> List[Dict[str, Any]]:
        """获取所有已完成的调度工单"""
        return list(self.completed_orders.values())
    
    def get_running_orders(self) -> List[Dict[str, Any]]:
        """获取所有运输中的调度工单"""
        return list(self.running_orders.values())
    
    def get_pending_orders(self) -> List[Dict[str, Any]]:
        """获取所有待运输的调度工单"""
        return list(self.pending_orders.values())
    
    def get_conflict_orders(self) -> List[Dict[str, Any]]:
        """获取所有冲突的调度工单"""
        return list(self.conflict_orders.values())
    
    def get_all_orders(self) -> List[Dict[str, Any]]:
        """获取所有调度工单"""
        return list(self.all_orders.values())
    
    def move_order_to_completed(self, order_id: str):
        """将工单移动到已完成状态"""
        order_data = self.get_order(order_id)
        if not order_data:
            raise ValueError(f"调度工单 {order_id} 不存在")
        
        # 从当前分类移除
        self._remove_from_all_categories(order_id)
        
        # 设置为已完成
        order_data['status'] = 'COMPLETED'
        order_data['end_time'] = self.current_time
        self.completed_orders[order_id] = order_data
    
    def move_order_to_running(self, order_id: str):
        """将工单移动到运输中状态"""
        order_data = self.get_order(order_id)
        if not order_data:
            raise ValueError(f"调度工单 {order_id} 不存在")
        
        # 从当前分类移除
        self._remove_from_all_categories(order_id)
        
        # 设置为运输中
        order_data['status'] = 'RUNNING'
        self.running_orders[order_id] = order_data
    
    def move_order_to_pending(self, order_id: str):
        """将工单移动到待运输状态"""
        order_data = self.get_order(order_id)
        if not order_data:
            raise ValueError(f"调度工单 {order_id} 不存在")
        
        # 从当前分类移除
        self._remove_from_all_categories(order_id)
        
        # 设置为待运输
        order_data['status'] = 'PENDING'
        self.pending_orders[order_id] = order_data
    
    def move_order_to_conflict(self, order_id: str, reason: str = ""):
        """将工单移动到冲突状态"""
        order_data = self.get_order(order_id)
        if not order_data:
            raise ValueError(f"调度工单 {order_id} 不存在")
        
        # 从当前分类移除
        self._remove_from_all_categories(order_id)
        
        # 设置为冲突
        order_data['status'] = 'CONFLICT'
        order_data['conflict_reason'] = reason
        self.conflict_orders[order_id] = order_data
    
    def get_orders_by_tank(self, tank_id: str) -> List[Dict[str, Any]]:
        """获取涉及指定油罐的调度工单"""
        result = []
        for order in self.all_orders.values():
            if (order.get('source_tank_id') == tank_id or 
                order.get('target_tank_id') == tank_id):
                result.append(order)
        return result
    
    def get_orders_by_pipeline(self, pipe_id: str) -> List[Dict[str, Any]]:
        """获取使用指定管道的调度工单"""
        result = []
        for order in self.all_orders.values():
            pipeline_path = order.get('pipeline_path', [])
            if pipe_id in pipeline_path:
                result.append(order)
        return result
    
    def get_orders_by_time_range(self, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        """获取指定时间范围内的调度工单"""
        result = []
        for order in self.all_orders.values():
            order_start = self._parse_datetime(order.get('start_time'))
            order_end = self._parse_datetime(order.get('end_time'))
            
            # 检查时间范围是否有重叠
            if not (order_end <= start_time or order_start >= end_time):
                result.append(order)
        return result
    
    def get_orders_by_status(self, status: str) -> List[Dict[str, Any]]:
        """根据状态获取调度工单"""
        if status == 'COMPLETED':
            return self.get_completed_orders()
        elif status == 'RUNNING':
            return self.get_running_orders()
        elif status == 'PENDING':
            return self.get_pending_orders()
        elif status == 'CONFLICT':
            return self.get_conflict_orders()
        else:
            # 返回所有匹配指定状态的工单
            result = []
            for order in self.all_orders.values():
                if order.get('status') == status:
                    result.append(order)
            return result
    
    def get_overlapping_orders(self, start_time: datetime, end_time: datetime, 
                              exclude_order_id: str = None) -> List[Dict[str, Any]]:
        """获取时间范围有重叠的调度工单（用于冲突检测）"""
        overlapping = []
        for order_id, order in self.all_orders.items():
            if exclude_order_id and order_id == exclude_order_id:
                continue
                
            order_start = self._parse_datetime(order.get('start_time'))
            order_end = self._parse_datetime(order.get('end_time'))
            
            # 检查时间范围是否有重叠
            if not (order_end <= start_time or order_start >= end_time):
                overlapping.append(order)
        return overlapping
    
    def update_current_time(self, new_time: datetime):
        """更新当前时间，可能会影响工单状态"""
        old_time = self.current_time
        self.current_time = new_time
        
        # 重新评估所有工单状态
        orders_to_update = []
        
        # 检查运输中的工单是否已完成
        for order_id, order in list(self.running_orders.items()):
            end_time = self._parse_datetime(order.get('end_time'))
            if new_time > end_time:
                orders_to_update.append((order_id, 'COMPLETED'))
        
        # 检查待运输的工单是否开始运输
        for order_id, order in list(self.pending_orders.items()):
            start_time = self._parse_datetime(order.get('start_time'))
            end_time = self._parse_datetime(order.get('end_time'))
            
            if start_time <= new_time <= end_time:
                orders_to_update.append((order_id, 'RUNNING'))
            elif new_time > end_time:
                orders_to_update.append((order_id, 'COMPLETED'))
        
        # 执行状态更新
        for order_id, new_status in orders_to_update:
            self._remove_from_all_categories(order_id)
            order_data = self.all_orders[order_id]
            
            if new_status == 'COMPLETED':
                order_data['status'] = 'COMPLETED'
                self.completed_orders[order_id] = order_data
            elif new_status == 'RUNNING':
                order_data['status'] = 'RUNNING'
                self.running_orders[order_id] = order_data
    
    def _parse_datetime(self, dt_value) -> datetime:
        """解析时间值，支持多种格式"""
        if isinstance(dt_value, datetime):
            return dt_value
        elif isinstance(dt_value, str):
            return datetime.fromisoformat(dt_value.replace('Z', '+00:00'))
        elif isinstance(dt_value, (int, float)):
            # 假设是时间戳
            return datetime.fromtimestamp(dt_value)
        else:
            # 默认返回当前时间
            return datetime.now()
    
    def get_statistics(self) -> Dict[str, int]:
        """获取调度工单统计信息"""
        return {
            'total_orders': len(self.all_orders),
            'completed_orders': len(self.completed_orders),
            'running_orders': len(self.running_orders),
            'pending_orders': len(self.pending_orders),
            'conflict_orders': len(self.conflict_orders)
        }
    
    def serialize(self) -> Dict[str, Any]:
        """序列化调度工单管理器"""
        return {
            'current_time': self.current_time.isoformat(),
            'completed_orders': self.completed_orders,
            'running_orders': self.running_orders,
            'pending_orders': self.pending_orders,
            'conflict_orders': self.conflict_orders,
            'all_orders': self.all_orders
        }
    
    def deserialize(self, data: Dict[str, Any]):
        """反序列化调度工单管理器"""
        self.current_time = datetime.fromisoformat(data['current_time'])
        self.completed_orders = data['completed_orders']
        self.running_orders = data['running_orders']
        self.pending_orders = data['pending_orders']
        self.conflict_orders = data['conflict_orders']
        self.all_orders = data['all_orders']




