from typing import List, Dict, Tuple, Optional, Any
from datetime import datetime, timedelta
import json
from dispatch_queue import DispatchOrderQueueManager
from data_class import Tank, Pipeline, Branch

class SchedulingState:
    """当前调度状态（包含已占用资源和调度工单管理）"""
    
    def __init__(self, tanks: List[Tank], pipelines: List[Pipeline], 
                 branches: Optional[List[Branch]] = None, 
                 order_dispatcher: Optional[DispatchOrderQueueManager] = None):
        # 将列表转换为字典以便快速查找
        self.tanks = {tank.tank_id: tank for tank in tanks}
        self.pipelines = {pipe.pipe_id: pipe for pipe in pipelines}
        self.branches = {branch.branch_id: branch for branch in branches} if branches else {}
        
        # 使用传入的调度工单管理器
        self.order_dispatcher = order_dispatcher
        
        # 全局状态指标
        self.oil_switch_count = 0  # 油品切换总次数（优化目标）
        self.high_priority_satisfied = 0  # 高优先级订单满足数
        self.total_dispatch_orders = len(self.order_dispatcher.get_all_orders()) if order_dispatcher else 0
        self.total_volume_dispatched = 0.0  # 总输送量
        self.current_time = datetime.now()  # 当前时间
        
        # 资源利用率统计
        self.tank_utilization = {}  # 油罐利用率
        self.pipeline_utilization = {}  # 管线利用率
        
        # 约束和限制
        self.constraints = {
            'min_safe_level': 0.0,  # 最小安全液位
            'max_cleaning_time': 2,  # 最大清洗时间(小时)
            'max_transport_rate': 100.0  # 最大输送速率
        }
        
        # 初始化状态统计
        # self._initialize_statistics()
        
        # 初始化资源状态
        # self._initialize_from_existing_orders()
    
    def _initialize_from_existing_orders(self):
        """根据现有调度工单初始化资源状态"""
        if not self.order_dispatcher:
            return
            
        all_orders = self.order_dispatcher.get_all_orders()
        for order_data in all_orders:
            self._apply_dispatch_order_to_resources(order_data)
    
    def _apply_dispatch_order_to_resources(self, order_data: Dict[str, Any]):
        """将调度工单应用到资源状态中"""
        # 更新源油罐状态
        source_tank_id = str(order_data.get('source_tank_id', ''))
        if source_tank_id in self.tanks:
            source_tank = self.tanks[source_tank_id]
            required_volume = order_data.get('required_volume', 0.0)
            oil_type = order_data.get('oil_type', '')
            
            # 对于已完成或正在运输的工单，减少库存
            end_time = self._parse_datetime(order_data.get('end_time'))
            if end_time >= self.current_time:
                source_tank.update_inventory(-required_volume, oil_type)
                branch_end_time = self._parse_datetime(order_data.get('branch_end_time', end_time))
                source_tank.occupied_until = max(source_tank.occupied_until, branch_end_time)
                
                # 检查是否需要清洗（油品切换）
                if (source_tank.last_oil_type and source_tank.last_oil_type != oil_type):
                    self.oil_switch_count += 1
                    source_tank.last_oil_type = oil_type
                    source_tank.cleaning_required = True
        
        # 更新目标油罐状态
        target_tank_id = str(order_data.get('target_tank_id', ''))
        if target_tank_id in self.tanks:
            target_tank = self.tanks[target_tank_id]
            required_volume = order_data.get('required_volume', 0.0)
            oil_type = order_data.get('oil_type', '')
            
            # 对于已完成或正在运输的工单，增加库存
            end_time = self._parse_datetime(order_data.get('end_time'))
            if end_time >= self.current_time:
                target_tank.update_inventory(required_volume, oil_type)
                branch_end_time = self._parse_datetime(order_data.get('branch_end_time', end_time))
                target_tank.occupied_until = max(target_tank.occupied_until, branch_end_time)
                target_tank.last_oil_type = oil_type
        
        # 更新管道状态
        pipeline_path = order_data.get('pipeline_path', [])
        for pipe_id in pipeline_path:
            str_pipe_id = str(pipe_id)
            if str_pipe_id in self.pipelines:
                pipeline = self.pipelines[str_pipe_id]
                start_time = self._parse_datetime(order_data.get('start_time'))
                end_time = self._parse_datetime(order_data.get('end_time'))
                oil_type = order_data.get('oil_type', '')
                required_volume = order_data.get('required_volume', 0.0)
                source_tank_id = str(order_data.get('source_tank_id', ''))
                target_tank_id = str(order_data.get('target_tank_id', ''))
                dispatch_order_id = order_data.get('dispatch_order_id', '')
                
                pipeline.add_occupancy(
                    start_time, end_time, oil_type, required_volume, 
                    source_tank_id, target_tank_id, dispatch_order_id
                )
    
    def _parse_datetime(self, dt_value) -> datetime:
        """解析时间值，支持多种格式"""
        if isinstance(dt_value, datetime):
            return dt_value
        elif isinstance(dt_value, str):
            return datetime.fromisoformat(dt_value.replace('Z', '+00:00'))
        elif isinstance(dt_value, (int, float)):  # 假设是时间戳
            return datetime.fromtimestamp(dt_value)
        else:  # 默认返回当前时间
            return datetime.now()
    
    def _initialize_statistics(self):
        """初始化统计信息"""
        # 计算初始油罐利用率
        for tank_id, tank in self.tanks.items():
            capacity_utilization = (tank.inventory / tank.safe_tank_capacity) if tank.safe_tank_capacity > 0 else 0
            self.tank_utilization[tank_id] = capacity_utilization
        
        # 初始化管线利用率
        for pipe_id in self.pipelines.keys():
            self.pipeline_utilization[pipe_id] = 0.0
    
    def add_dispatch_order(self, dispatch_order_data: Dict[str, Any]):
        """添加新的调度工单到状态中"""
        if self.order_dispatcher:
            # 通过调度工单管理器添加
            self.order_dispatcher.add_order(dispatch_order_data)
        
        # 更新资源占用状态
        self._apply_dispatch_order_to_resources(dispatch_order_data)
        
        # 更新全局指标
        if self.order_dispatcher:
            self.total_dispatch_orders = len(self.order_dispatcher.get_all_orders())
        self.total_volume_dispatched += dispatch_order_data.get('required_volume', 0.0)
    
    def remove_dispatch_order(self, dispatch_order_id: str):
        """从状态中移除调度工单（用于插单调整）"""
        if self.order_dispatcher:
            # 从调度工单管理器中移除
            self.order_dispatcher.remove_order(dispatch_order_id)
        
        # 从资源占用中移除
        for pipeline in self.pipelines.values():
            pipeline.remove_occupancy(dispatch_order_id)
        
        # 更新全局指标
        if self.order_dispatcher:
            self.total_dispatch_orders = len(self.order_dispatcher.get_all_orders())
    
    def update_dispatch_order(self, old_dispatch_order_id: str, new_dispatch_order_data: Dict[str, Any]):
        """更新调度工单（用于插单调整）"""
        if self.order_dispatcher:
            # 通过调度工单管理器更新
            self.order_dispatcher.update_order(old_dispatch_order_id, new_dispatch_order_data)
        
        # 移除旧的资源占用
        for pipeline in self.pipelines.values():
            pipeline.remove_occupancy(old_dispatch_order_id)
        
        # 添加新的资源占用
        self._apply_dispatch_order_to_resources(new_dispatch_order_data)
    
    def get_available_tanks_for_oil_type(self, oil_type: str, min_volume: float = 0, check_time: Optional[datetime] = None) -> List[str]:
        """获取指定时间可供应特定油品的油罐列表"""
        available_tanks = []
        check_time = check_time or self.current_time
        
        for tank_id, tank in self.tanks.items():
            # 检查是否可用时间
            if not tank.is_available_at_time(check_time):
                continue
            
            # 检查油品类型匹配
            if tank.oil_type != oil_type and tank.oil_type is not None:
                # 检查是否兼容（简化：假定可以转换）
                continue
            
            # 检查容量
            available_capacity = tank.inventory - tank.reserved_volume
            if available_capacity >= min_volume + tank.min_safe_level:
                available_tanks.append(tank_id)
        
        return available_tanks
    
    def get_available_pipelines(self, start_time: datetime, end_time: datetime) -> List[str]:
        """获取指定时间段内可用的管道列表"""
        available_pipelines = []
        
        for pipe_id, pipeline in self.pipelines.items():
            if pipeline.is_available_at_time(start_time, end_time):
                available_pipelines.append(pipe_id)
        
        return available_pipelines
    
    def calculate_resource_utilization(self) -> float:
        """计算整体资源利用率"""
        if not self.tanks:
            return 0.0
        
        total_utilization = 0.0
        count = 0
        
        for tank_id, tank in self.tanks.items():
            utilization = (tank.inventory / tank.safe_tank_capacity) if tank.safe_tank_capacity > 0 else 0
            total_utilization += utilization
            count += 1
        
        return total_utilization / count if count > 0 else 0.0
    
    def get_conflicts(self) -> List[Dict]:
        """获取当前状态下的冲突列表"""
        conflicts = []
        
        # 检查油罐冲突
        for tank_id, tank in self.tanks.items():
            if tank.inventory < tank.min_safe_level:
                conflicts.append({
                    'type': 'tank_inventory_low',
                    'resource_id': tank_id,
                    'current_level': tank.inventory,
                    'min_safe_level': tank.min_safe_level
                })
        
        # 检查管道时间冲突
        for pipe_id, pipeline in self.pipelines.items():
            schedule = pipeline.occupancy_schedule
            # 检查时间重叠
            for i in range(len(schedule)):
                for j in range(i + 1, len(schedule)):
                    start1, end1, _, _, _, _, _ = schedule[i]
                    start2, end2, _, _, _, _, _ = schedule[j]
                    if not (end1 <= start2 or start1 >= end2):
                        conflicts.append({
                            'type': 'pipeline_time_conflict',
                            'resource_id': pipe_id,
                            'schedule1': schedule[i],
                            'schedule2': schedule[j]
                        })
        
        return conflicts
    
    def serialize_state(self) -> Dict:
        """序列化当前状态为字典（用于保存和传输）"""
        return {
            'tanks': {tid: {
                'tank_id': t.tank_id,
                'status': t.status,
                'inventory': t.inventory,
                'current_level': t.current_level,
                'oil_type': t.oil_type,
                'reserved_volume': t.reserved_volume,
                'occupied_until': t.occupied_until.isoformat() if t.occupied_until != datetime.min else None,
                'cleaning_required': t.cleaning_required,
                'last_oil_type': t.last_oil_type
            } for tid, t in self.tanks.items()},
            'pipelines': {pid: {
                'pipe_id': p.pipe_id,
                'status': p.status,
                'current_oil_type': p.current_oil_type,
                'occupancy_schedule': [(s[0].isoformat(), s[1].isoformat(), s[2], s[3], s[4], s[5], s[6]) for s in p.occupancy_schedule],
                'cleaning_required': p.cleaning_required,
                'last_oil_type': p.last_oil_type
            } for pid, p in self.pipelines.items()},
            'global_metrics': {
                'oil_switch_count': self.oil_switch_count,
                'high_priority_satisfied': self.high_priority_satisfied,
                'total_dispatch_orders': self.total_dispatch_orders,
                'total_volume_dispatched': self.total_volume_dispatched,
                'current_time': self.current_time.isoformat()
            }
        }
    
    def deserialize_state(self, state_dict: Dict):
        """从字典反序列化状态"""
        # 恢复油罐状态
        for tank_id, tank_data in state_dict['tanks'].items():
            if tank_id in self.tanks:
                tank = self.tanks[tank_id]
                tank.inventory = tank_data['inventory']
                tank.current_level = tank_data['current_level']
                tank.oil_type = tank_data['oil_type']
                tank.reserved_volume = tank_data['reserved_volume']
                tank.status = tank_data['status']
                tank.cleaning_required = tank_data['cleaning_required']
                tank.last_oil_type = tank_data['last_oil_type']
                if tank_data['occupied_until']:
                    tank.occupied_until = datetime.fromisoformat(tank_data['occupied_until'])
        
        # 恢复管道状态
        for pipe_id, pipe_data in state_dict['pipelines'].items():
            if pipe_id in self.pipelines:
                pipeline = self.pipelines[pipe_id]
                pipeline.current_oil_type = pipe_data['current_oil_type']
                pipeline.status = pipe_data['status']
                pipeline.cleaning_required = pipe_data['cleaning_required']
                pipeline.last_oil_type = pipe_data['last_oil_type']
                pipeline.occupancy_schedule = [
                    (datetime.fromisoformat(s[0]), datetime.fromisoformat(s[1]), s[2], s[3], s[4], s[5], s[6])
                    for s in pipe_data['occupancy_schedule']
                ]
        
        # 恢复全局指标
        metrics = state_dict['global_metrics']
        self.oil_switch_count = metrics['oil_switch_count']
        self.high_priority_satisfied = metrics['high_priority_satisfied']
        self.total_dispatch_orders = metrics['total_dispatch_orders']
        self.total_volume_dispatched = metrics['total_volume_dispatched']
        self.current_time = datetime.fromisoformat(metrics['current_time'])
