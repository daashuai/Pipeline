from typing import List, Dict, Optional, Any
from datetime import datetime
from data_class import Tank, Pipeline, Branch
from copy import deepcopy

class State:
    """基础状态类 - 只包含基础资源信息和状态指标"""
    
    def __init__(self, 
                 tanks: List[Tank], 
                 pipelines: List[Pipeline], 
                 branches: Optional[List[Branch]] = None):
        """
        初始化状态
        
        Args:
            tanks: 油罐列表
            pipelines: 管道列表
            branches: 分支列表
        """
        # 将列表转换为字典以便快速查找
        self.tanks = {tank.tank_id: deepcopy(tank) for tank in tanks}
        self.pipelines = {pipe.pipe_id: deepcopy(pipe) for pipe in pipelines}
        self.branches = {branch.branch_id: deepcopy(branch) for branch in branches} if branches else {}
        
        # 全局状态指标
        self.oil_switch_count = 0  # 油品切换总次数（优化目标）
        self.high_priority_satisfied = 0  # 高优先级订单满足数
        self.total_dispatch_orders = 0
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
    
    def apply_dispatch_order(self, order_data: Dict[str, Any]) -> 'State':
        """
        应用调度工单到当前状态，返回新的状态
        
        Args:
            order_data: 调度工单数据
            
        Returns:
            应用工单后的新状态
        """
        # 创建新状态的副本
        new_state = State(
            tanks=list(self.tanks.values()),
            pipelines=list(self.pipelines.values()),
            branches=list(self.branches.values()) if self.branches else []
        )
        
        # 复制当前状态的指标
        new_state.oil_switch_count = self.oil_switch_count
        new_state.high_priority_satisfied = self.high_priority_satisfied
        new_state.total_dispatch_orders = self.total_dispatch_orders
        new_state.total_volume_dispatched = self.total_volume_dispatched
        new_state.current_time = self.current_time
        new_state.tank_utilization = self.tank_utilization.copy()
        new_state.pipeline_utilization = self.pipeline_utilization.copy()
        new_state.constraints = self.constraints.copy()
        
        # 更新源油罐状态
        source_tank_id = str(order_data.get('source_tank_id', ''))
        if source_tank_id in new_state.tanks:
            source_tank = new_state.tanks[source_tank_id]
            required_volume = order_data.get('required_volume', 0.0)
            oil_type = order_data.get('oil_type', '')
            
            # 减少库存（确保不低于安全液位）
            new_inventory = source_tank.inventory - required_volume
            source_tank.inventory = max(new_inventory, source_tank.min_safe_level)
            
            # 更新液位
            if source_tank.safe_tank_capacity > 0:
                source_tank.current_level = source_tank.inventory / source_tank.safe_tank_capacity
            
            # 更新占用时间
            end_time = self._parse_datetime(order_data.get('end_time'))
            branch_end_time = self._parse_datetime(order_data.get('branch_end_time', end_time))
            # source_tank.occupied_until = max(source_tank.occupied_until, branch_end_time)
            
            # 检查是否需要清洗（油品切换）
            if source_tank.oil_type and source_tank.oil_type != oil_type:
                source_tank.oil_type = oil_type
                # source_tank.cleaning_required = True
        
        # 更新目标油罐状态
        target_tank_id = str(order_data.get('target_tank_id', ''))
        if target_tank_id in new_state.tanks:
            target_tank = new_state.tanks[target_tank_id]
            required_volume = order_data.get('required_volume', 0.0)
            oil_type = order_data.get('oil_type', '')
            
            # 增加库存
            target_tank.inventory += required_volume
            
            # 更新液位
            if target_tank.safe_tank_capacity > 0:
                target_tank.current_level = min(
                    target_tank.inventory / target_tank.safe_tank_capacity,
                    target_tank.safe_tank_level
                )
            
            # 更新占用时间
            end_time = self._parse_datetime(order_data.get('end_time'))
            branch_end_time = self._parse_datetime(order_data.get('branch_end_time', end_time))
            # target_tank.occupied_until = max(target_tank.occupied_until, branch_end_time)
            target_tank.oil_type = oil_type
        
        # 更新管道状态
        pipeline_path = order_data.get('pipeline_path', [])
        for pipe_id in pipeline_path:
            str_pipe_id = str(pipe_id)
            if str_pipe_id in new_state.pipelines:
                pipeline = new_state.pipelines[str_pipe_id]
                start_time = self._parse_datetime(order_data.get('start_time'))
                end_time = self._parse_datetime(order_data.get('end_time'))
                oil_type = order_data.get('oil_type', '')
                required_volume = order_data.get('required_volume', 0.0)
                source_tank_id = str(order_data.get('source_tank_id', ''))
                target_tank_id = str(order_data.get('target_tank_id', ''))
                dispatch_order_id = order_data.get('dispatch_order_id', '')
                
                # # 添加占用计划
                # pipeline.occupancy_schedule.append((
                #     start_time, end_time, oil_type, required_volume, 
                #     source_tank_id, target_tank_id, dispatch_order_id
                # ))
                # 
                # # 更新当前油品
                # pipeline.current_oil = oil_type
        
        # 更新全局指标
        new_state.total_dispatch_orders += 1
        # new_state.total_volume_dispatched += required_volume
        
        # 检查油品切换
        if self._is_oil_switch_needed_in_state(new_state, order_data):
            new_state.oil_switch_count += 1
        
        return new_state
    
    def _is_oil_switch_needed_in_state(self, state: 'State', order_data: Dict[str, Any]) -> bool:
        """
        检查在给定状态下是否需要油品切换
        
        Args:
            state: 当前状态
            order_data: 调度工单数据
            
        Returns:
            是否需要油品切换
        """
        source_tank_id = str(order_data.get('source_tank_id', ''))
        oil_type = order_data.get('oil_type', '')
        
        if source_tank_id in state.tanks:
            source_tank = state.tanks[source_tank_id]
            if source_tank.oil_type and source_tank.oil_type != oil_type:
                return True
        
        return False
    
    def _parse_datetime(self, dt_value) -> datetime:
        """
        解析时间值
        
        Args:
            dt_value: 时间值，支持多种格式
            
        Returns:
            datetime对象
        """
        if isinstance(dt_value, datetime):
            return dt_value
        elif isinstance(dt_value, str):
            return datetime.fromisoformat(dt_value.replace('Z', '+00:00'))
        elif isinstance(dt_value, (int, float)):  # 假设是时间戳
            return datetime.fromtimestamp(dt_value)
        else:  # 默认返回当前时间
            return datetime.now()
    
    def get_available_tanks_for_oil_type(self, oil_type: str, min_volume: float = 0, 
                                         check_time: Optional[datetime] = None) -> List[str]:
        """
        获取指定时间可供应特定油品的油罐列表
        
        Args:
            oil_type: 油品类型
            min_volume: 最小需要体积
            check_time: 检查时间，默认为当前时间
            
        Returns:
            可用油罐ID列表
        """
        available_tanks = []
        check_time = check_time or self.current_time
        
        for tank_id, tank in self.tanks.items():
            # 检查油品类型匹配
            if tank.oil_type != oil_type and tank.oil_type is not None:
                continue
            
            # 检查容量
            reserved_volume = getattr(tank, 'reserved_volume', 0, 0)
            available_capacity = tank.inventory - reserved_volume
            if available_capacity >= min_volume + tank.min_safe_level:
                available_tanks.append(tank_id)
        
        return available_tanks
    
    def get_available_pipelines(self, start_time: datetime, end_time: datetime) -> List[str]:
        """
        获取指定时间段内可用的管道列表
        
        Args:
            start_time: 开始时间
            end_time: 结束时间
            
        Returns:
            可用管道ID列表
        """
        available_pipelines = []
        
        for pipe_id, pipeline in self.pipelines.items():
            if hasattr(pipeline, 'is_available_at_time') and pipeline.is_available_at_time(start_time, end_time):
                available_pipelines.append(pipe_id)
        
        return available_pipelines
    
    def calculate_resource_utilization(self) -> float:
        """
        计算整体资源利用率
        
        Returns:
            资源利用率
        """
        if not self.tanks:
            return 0.0
        
        total_utilization = 0.0
        count = 0
        
        for tank in self.tanks.values():
            utilization = (tank.inventory / tank.safe_tank_capacity) if tank.safe_tank_capacity > 0 else 0
            total_utilization += utilization
            count += 1
        
        return total_utilization / count if count > 0 else 0.0
    
    def get_conflicts(self) -> List[Dict]:
        """
        获取当前状态下的冲突列表
        
        Returns:
            冲突列表
        """
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
            if hasattr(pipeline, 'occupancy_schedule'):
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
    
    def get_state_summary(self) -> Dict[str, Any]:
        """
        获取状态摘要信息
        
        Returns:
            状态摘要字典
        """
        return {
            'tank_count': len(self.tanks),
            'pipeline_count': len(self.pipelines),
            'branch_count': len(self.branches),
            'oil_switch_count': self.oil_switch_count,
            'high_priority_satisfied': self.high_priority_satisfied,
            'total_dispatch_orders': self.total_dispatch_orders,
            'total_volume_dispatched': self.total_volume_dispatched,
            'current_time': self.current_time.isoformat(),
            'resource_utilization': self.calculate_resource_utilization()
        }
    
    def serialize_state(self) -> Dict[str, Any]:
        """
        序列化当前状态为字典（用于保存和传输）
        
        Returns:
            序列化后的状态字典
        """
        return {
            'tanks': {tid: {
                'tank_id': t.tank_id,
                'site_id': t.site_id,
                'tank_name': t.tank_name,
                'tank_area': t.tank_area,
                'oil_type': t.oil_type,
                'inventory': t.inventory,
                'current_level': t.current_level,
                'tank_capacity_per_meter': t.tank_capacity_per_meter,
                'maximum_tank_capacity': t.maximum_tank_capacity,
                'safe_tank_capacity': t.safe_tank_capacity,
                'maximum_tank_level': t.maximum_tank_level,
                'safe_tank_level': t.safe_tank_level,
                'min_safe_level': t.min_safe_level,
                'tank_type': t.tank_type,
                'status': t.status,
                'occupied_until': t.occupied_until.isoformat() if hasattr(t, 'occupied_until') and t.occupied_until != datetime.min else None,
                'cleaning_required': getattr(t, 'cleaning_required', False)
            } for tid, t in self.tanks.items()},
            'pipelines': {pid: {
                'pipe_id': p.pipe_id,
                'pipe_name': p.pipe_name,
                'pipe_capacity_per_meter': p.pipe_capacity_per_meter,
                'pipe_shutdown_start_time': p.pipe_shutdown_start_time.isoformat() if p.pipe_shutdown_start_time else None,
                'pipe_shutdown_end_time': p.pipe_shutdown_end_time.isoformat() if p.pipe_shutdown_end_time else None,
                'pipe_shutdown_reason': p.pipe_shutdown_reason,
                'occupancy_schedule': [(s[0].isoformat(), s[1].isoformat(), s[2], s[3], s[4], s[5], s[6]) for s in getattr(p, 'occupancy_schedule', [])],
                'current_oil': getattr(p, 'current_oil', None)
            } for pid, p in self.pipelines.items()},
            'branches': {bid: {
                'branch_id': b.branch_id,
                'from_id': b.from_id,
                'to_id': b.to_id,
                'is_direct_connection': b.is_direct_connection,
                'branch_name': b.branch_name,
                'branch_mileage': b.branch_mileage,
                'branch_elevation': b.branch_elevation,
                'branch_capacity': b.branch_capacity,
                'is_begin': b.is_begin,
                'is_end': b.is_end,
                'is_middle': b.is_middle
            } for bid, b in self.branches.items()},
            'global_metrics': {
                'oil_switch_count': self.oil_switch_count,
                'high_priority_satisfied': self.high_priority_satisfied,
                'total_dispatch_orders': self.total_dispatch_orders,
                'total_volume_dispatched': self.total_volume_dispatched,
                'current_time': self.current_time.isoformat()
            },
            'constraints': self.constraints
        }
    
    def deserialize_state(self, state_dict: Dict[str, Any]) -> 'State':
        """
        从字典反序列化状态
        
        Args:
            state_dict: 序列化的状态字典
            
        Returns:
            反序列化后的状态对象
        """
        # 恢复油罐状态
        for tank_id, tank_data in state_dict['tanks'].items():
            if tank_id in self.tanks:
                tank = self.tanks[tank_id]
                tank.oil_type = tank_data['oil_type']
                tank.inventory = tank_data['inventory']
                tank.current_level = tank_data['current_level']
                tank.status = tank_data['status']
                tank.cleaning_required = tank_data.get('cleaning_required', False)
                if tank_data.get('occupied_until'):
                    tank.occupied_until = datetime.fromisoformat(tank_data['occupied_until'])
        
        # 恢复管道状态
        for pipe_id, pipe_data in state_dict['pipelines'].items():
            if pipe_id in self.pipelines:
                pipeline = self.pipelines[pipe_id]
                pipeline.current_oil = pipe_data.get('current_oil')
                if 'occupancy_schedule' in pipe_data:
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
        
        # 恢复约束
        self.constraints = state_dict.get('constraints', self.constraints)
        
        return self




