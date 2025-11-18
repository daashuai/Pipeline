import numpy as np
import copy
from datetime import datetime, timedelta

class Order:
    def __init__(self, order_id, arrival_time, processing_time, deadline, priority=1):
        self.order_id = order_id
        self.arrival_time = arrival_time  # 订单到达时间
        self.processing_time = processing_time  # 处理时间
        self.deadline = deadline  # 截止时间
        self.priority = priority  # 优先级
        self.resource_requirements = {}  # 资源需求字典

class Schedule:
    def __init__(self):
        self.scheduled_orders = []  # 已排程的订单列表
        self.resource_utilization = {}  # 资源利用率记录
    
    def add_order(self, order, start_time, resource_allocation):
        """将订单添加到当前排程中"""
        order_schedule = {
            'order': order,
            'start_time': start_time,
            'end_time': start_time + timedelta(minutes=order.processing_time),
            'resource_allocation': resource_allocation
        }
        self.scheduled_orders.append(order_schedule)
        # 更新资源利用率
        self._update_resource_utilization(order, start_time, resource_allocation)
    
    def _update_resource_utilization(self, order, start_time, resource_allocation):
        """更新资源利用率"""
        end_time = start_time + timedelta(minutes=order.processing_time)
        for resource, amount in resource_allocation.items():
            if resource not in self.resource_utilization:
                self.resource_utilization[resource] = []
            self.resource_utilization[resource].append({
                'start': start_time,
                'end': end_time,
                'amount': amount
            })

class RollingScheduler:
    def __init__(self, resources):
        """
        初始化滚动调度器
        resources: 系统可用资源字典，格式 {'resource_name': capacity, ...}
        """
        self.current_schedule = Schedule()
        self.resources = resources
        self.order_counter = 0
        self.order_history = []
    
    def receive_new_order(self, order):
        """接收新订单并触发调度"""
        self.order_counter += 1
        self.order_history.append(order)
        
        # 为新订单生成可行的排程方案
        best_schedule = self._schedule_new_order(order)
        
        # 更新当前排程
        self.current_schedule = best_schedule
        return best_schedule
    
    def _schedule_new_order(self, new_order):
        """使用GA为新订单生成排程方案"""
        # 创建当前排程的深拷贝，用于评估不同方案
        current_schedule_copy = copy.deepcopy(self.current_schedule)
        
        # 定义GA需要的编码范围 - 只针对新订单的插入位置和资源分配
        encoding_info = self._define_encoding_for_new_order(new_order, current_schedule_copy)
        
        # GA优化 - 寻找新订单的最佳插入位置
        best_chromosome = self._run_ga_for_new_order(encoding_info, new_order, current_schedule_copy)
        
        # 解码最佳染色体，获取具体排程
        best_schedule = self._decode_chromosome(best_chromosome, new_order, current_schedule_copy)
        
        return best_schedule
    
    def _define_encoding_for_new_order(self, new_order, current_schedule):
        """
        定义GA编码，只针对新订单的调度决策
        返回编码范围信息
        """
        # 可能的插入位置数（在已有订单之间和末尾）
        num_positions = len(current_schedule.scheduled_orders) + 1
        
        # 可用资源及其可能的分配方案
        resource_options = {}
        for resource, capacity in self.resources.items():
            # 简化的资源分配范围，实际可根据订单需求定制
            min_alloc = min(1, capacity)
            max_alloc = min(capacity, 10)  # 假设最大分配10个单位
            resource_options[resource] = (min_alloc, max_alloc)
        
        return {
            'num_positions': num_positions,
            'resource_options': resource_options,
            'time_window': self._get_feasible_time_window(new_order, current_schedule)
        }
    
    def _get_feasible_time_window(self, new_order, current_schedule):
        """获取新订单可行的时间窗口"""
        earliest_start = new_order.arrival_time
        latest_start = new_order.deadline - timedelta(minutes=new_order.processing_time)
        
        # 考虑已有排程的约束
        if current_schedule.scheduled_orders:
            last_order = current_schedule.scheduled_orders[-1]
            earliest_start = max(earliest_start, last_order['end_time'])
        
        return (earliest_start, latest_start)
    
    def _run_ga_for_new_order(self, encoding_info, new_order, current_schedule):
        """
        运行GA算法，只优化新订单的排程
        此处简写GA实现，假设用户有自己的GA框架
        """
        # 伪代码：设置GA参数
        ga_params = {
            'chromosome_length': 1 + len(encoding_info['resource_options']),  # 位置+资源分配
            'population_size': 50,
            'generations': 100,
            'mutation_rate': 0.1,
            'crossover_rate': 0.8
        }
        
        # 伪代码：定义适应度函数
        def fitness_function(chromosome):
            return self._evaluate_schedule(chromosome, new_order, current_schedule, encoding_info)
        
        # 伪代码：调用用户的GA实现
        # best_chromosome = user_ga_framework.run(ga_params, fitness_function)
        
        # 简化的示例返回，实际应由GA算法填充
        best_chromosome = [0.5] * ga_params['chromosome_length']  # 示例染色体
        return best_chromosome
    
    def _evaluate_schedule(self, chromosome, new_order, current_schedule, encoding_info):
        """
        评估新订单插入方案的适应度
        考虑因素：
        1. 是否满足截止时间
        2. 资源利用率
        3. 对已有订单的影响
        4. 优先级权重
        """
        # 解码染色体获取具体安排
        position_idx, resource_alloc = self._decode_partial_chromosome(chromosome, encoding_info)
        
        # 计算插入后的时间
        insert_time = self._calculate_insertion_time(position_idx, new_order, current_schedule)
        
        # 检查资源可行性
        if not self._check_resource_feasibility(new_order, insert_time, resource_alloc, current_schedule):
            return -10000  # 不可行方案给予很低的适应度
        
        # 计算各项指标
        metrics = {
            'deadline_satisfaction': self._calculate_deadline_satisfaction(new_order, insert_time),
            'resource_efficiency': self._calculate_resource_efficiency(resource_alloc, self.resources),
            'disruption': self._calculate_schedule_disruption(position_idx, current_schedule),
            'priority_weight': new_order.priority
        }
        
        # 综合适应度 - 实际可调整权重
        fitness = (metrics['deadline_satisfaction'] * 0.4 + 
                   metrics['resource_efficiency'] * 0.3 + 
                   (1 - metrics['disruption']) * 0.2 + 
                   metrics['priority_weight'] * 0.1)
        
        return fitness
    
    def _decode_chromosome(self, chromosome, new_order, current_schedule):
        """将最佳染色体解码为完整排程"""
        # 创建当前排程的深拷贝
        new_schedule = copy.deepcopy(current_schedule)
        
        # 获取插入位置和资源分配
        position_idx, resource_alloc = self._decode_partial_chromosome(chromosome, None)
        
        # 计算插入时间
        insert_time = self._calculate_insertion_time(position_idx, new_order, current_schedule)
        
        # 将新订单插入到排程中
        new_schedule.add_order(new_order, insert_time, resource_alloc)
        
        return new_schedule
    
    def _decode_partial_chromosome(self, chromosome, encoding_info):
        """解码染色体的部分信息 (位置索引和资源分配)"""
        # 位置索引
        position_ratio = chromosome[0]
        if encoding_info:
            position_idx = int(position_ratio * (encoding_info['num_positions'] - 1))
        else:
            position_idx = int(position_ratio * len(self.current_schedule.scheduled_orders))
        
        # 资源分配
        resource_alloc = {}
        resource_names = list(self.resources.keys())
        for i, resource in enumerate(resource_names):
            if i+1 < len(chromosome):
                ratio = chromosome[i+1]
                min_alloc, max_alloc = 1, self.resources[resource]  # 简化处理
                alloc_amount = int(min_alloc + ratio * (max_alloc - min_alloc))
                resource_alloc[resource] = alloc_amount
        
        return position_idx, resource_alloc
    
    def _calculate_insertion_time(self, position_idx, new_order, current_schedule):
        """计算在指定位置插入订单的开始时间"""
        scheduled_orders = current_schedule.scheduled_orders
        
        if position_idx == 0:
            # 插入到最前面
            return max(new_order.arrival_time, datetime.now())
        elif position_idx >= len(scheduled_orders):
            # 插入到末尾
            if scheduled_orders:
                last_order = scheduled_orders[-1]
                return max(new_order.arrival_time, last_order['end_time'])
            else:
                return max(new_order.arrival_time, datetime.now())
        else:
            # 插入到中间
            prev_order = scheduled_orders[position_idx-1]
            next_order = scheduled_orders[position_idx]
            earliest_start = max(new_order.arrival_time, prev_order['end_time'])
            
            # 确保不会影响下一个订单
            if earliest_start + timedelta(minutes=new_order.processing_time) > next_order['start_time']:
                # 需要推迟后续订单，这会在适应度函数中被惩罚
                pass
                
            return earliest_start
    
    def _check_resource_feasibility(self, order, start_time, resource_alloc, current_schedule):
        """检查资源可行性"""
        end_time = start_time + timedelta(minutes=order.processing_time)
        
        for resource, required_amount in resource_alloc.items():
            if resource not in self.resources:
                return False
            
            capacity = self.resources[resource]
            if required_amount > capacity:
                return False
            
            # 检查时间重叠的资源使用
            if resource in current_schedule.resource_utilization:
                for usage in current_schedule.resource_utilization[resource]:
                    # 检查时间重叠
                    if not (end_time <= usage['start'] or start_time >= usage['end']):
                        # 有时间重叠，检查总资源需求
                        if usage['amount'] + required_amount > capacity:
                            return False
        
        return True
    
    def _calculate_deadline_satisfaction(self, order, start_time):
        """计算截止时间满足度"""
        completion_time = start_time + timedelta(minutes=order.processing_time)
        if completion_time <= order.deadline:
            # 提前完成有奖励
            early_ratio = (order.deadline - completion_time).total_seconds() / max((order.deadline - order.arrival_time).total_seconds(), 1)
            return 1.0 + min(0.2, early_ratio * 0.2)  # 最多20%奖励
        else:
            # 延迟完成的惩罚
            delay_ratio = (completion_time - order.deadline).total_seconds() / max((order.deadline - order.arrival_time).total_seconds(), 1)
            return max(0.1, 1.0 - delay_ratio)  # 至少0.1适应度
    
    def _calculate_resource_efficiency(self, resource_alloc, resources):
        """计算资源利用效率"""
        efficiency_score = 0
        total_resources = len(resources)
        
        for resource, alloc in resource_alloc.items():
            if resource in resources:
                utilization_ratio = alloc / resources[resource]
                # 适度利用比过度利用或利用不足更好
                if 0.6 <= utilization_ratio <= 0.8:
                    efficiency_score += 1.0
                elif utilization_ratio < 0.4 or utilization_ratio > 0.9:
                    efficiency_score += 0.5
                else:
                    efficiency_score += 0.8
        
        return efficiency_score / max(total_resources, 1) if total_resources > 0 else 1.0
    
    def _calculate_schedule_disruption(self, position_idx, current_schedule):
        """计算对已有排程的干扰程度"""
        # 插入位置越靠后，对已有排程的干扰越小
        total_orders = len(current_schedule.scheduled_orders)
        
        if total_orders == 0:
            return 0.0
        
        disruption_ratio = position_idx / total_orders
        return 1.0 - disruption_ratio  # 0表示无干扰，1表示最大干扰
    
    def get_current_schedule(self):
        """获取当前排程"""
        return self.current_schedule
    
    def simulate_order_arrivals(self, order_stream):
        """
        模拟订单流的到达和调度
        order_stream: 按到达时间排序的订单列表
        """
        for order in order_stream:
            print(f"处理新订单: {order.order_id}, 到达时间: {order.arrival_time}")
            self.receive_new_order(order)
        
        return self.current_schedule

# 初始化资源
resources = {
    'machine': 5,
    'worker': 10,
    'material_A': 100
}

# 创建滚动调度器
scheduler = RollingScheduler(resources)

# 模拟订单流
order_stream = [
    Order('O1', datetime.now(), 30, datetime.now() + timedelta(hours=2), priority=2),
    Order('O2', datetime.now() + timedelta(minutes=15), 45, datetime.now() + timedelta(hours=3)),
    # 更多订单...
]

# 运行模拟
final_schedule = scheduler.simulate_order_arrivals(order_stream)

# 查看结果
for scheduled_order in final_schedule.scheduled_orders:
    print(f"订单 {scheduled_order['order'].order_id}: "
          f"开始时间 {scheduled_order['start_time']}, "
          f"结束时间 {scheduled_order['end_time']}")


