from models.models import Tank, Pipeline
from typing import List, Dict, Tuple, Optional, Any

# ======================
# 2. 动态状态模型 (滚动调度核心)
# ======================

class TankState(Tank):
    """动态油罐状态（继承静态属性）"""
    def __init__(self, tank: Tank):
        super().__init__(tank.id, tank.capacity, tank.current_level, 
                        tank.compatible_oils, (tank.safety_min, tank.safety_max))
        self.current_oil = None  # 当前存储油品类型(动态)
        self.occupied_until = 0  # 被占用到的时间戳(动态)
        self.last_clean_time = 0  # 上次清洗时间(动态)

class PipelineState(Pipeline):
    """动态管线状态（继承静态属性）"""
    def __init__(self, pipeline: Pipeline):
        super().__init__(pipeline.id, pipeline.start_node, pipeline.end_node,
                        pipeline.capacity, pipeline.max_pressure)
        self.current_oil = None  # 当前输送油品(动态)
        self.last_clean_time = 0  # 上次清洗时间(动态)
        self.occupancy_schedule = []  # 占用计划 [(start_time, end_time, oil_type, quantity), ...]

class SchedulingState:
    """当前调度状态（包含已占用资源）"""
    def __init__(self, tanks: Dict[str, Tank], pipelines: Dict[str, Pipeline]):
        # 基础资源（动态状态）
        self.tanks = {tid: TankState(t) for tid, t in tanks.items()}
        self.pipelines = {pid: PipelineState(p) for pid, p in pipelines.items()}
        
        # 全局状态
        self.oil_switch_count = 0  # 油品切换总次数（优化目标）
        self.high_priority_satisfied = 0  # 高优先级订单满足数
        self.total_dispatch_orders = 0  # 总调度工单数
        
        # 初始化当前油品（简化处理）
        for tank_id, tank in self.tanks.items():
            if tank.current_level > 0:
                tank.current_oil = "default_oil"  # 实际系统应记录真实油品
        
        # 初始化管线占用
        for pipeline_id, pipeline in self.pipelines.items():
            pipeline.current_oil = None


