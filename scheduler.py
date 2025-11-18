from abc import ABC, abstractmethod
from copy import deepcopy
from typing import List, Dict, Tuple, Optional, Any
import time
from .state import SchedulingState, TankState, PipelineState
from models.models import CustomerOrder, DispatchOrder

class PathScoringStrategy(ABC):
    """路径评分策略接口（为GA优化预留）"""
    
    @abstractmethod
    def calculate_score(self, path: List[str], oil_type: str, start_time: int, 
                       state: SchedulingState, quantity: float) -> float:
        """
        计算路径评分，分数越高越好
        核心优化目标：最小化油品切换、最大化高优先级订单满足率
        """
        pass

class RuleBasedScoring(PathScoringStrategy):
    """规则基础评分策略（初版实现）"""
    
    def calculate_score(self, path: List[str], oil_type: str, start_time: int,
                       state: SchedulingState, quantity: float) -> float:
        """
        评分规则（按业务重要性排序）：
        1. 无需清洗（+100分）
        2. 与当前输送油品相同（+50分）
        3. 高优先级订单满足时间窗（+30分）
        4. 管线能力充足（+20分）
        5. 需要清洗（-80分）
        """
        score = 0.0
        
        # 检查路径上所有管线
        for pipeline_id in path:
            pipeline = state.pipelines[pipeline_id]
            
            # 规则1: 无需清洗（当前油品匹配）
            if pipeline.current_oil == oil_type:
                score += 100
            # 规则5: 需要清洗
            elif pipeline.current_oil is not None:
                score -= 80
            
            # 规则2: 与当前输送油品相同（连续输送）
            if pipeline.current_oil == oil_type:
                score += 50
            
            # 规则4: 管线能力校验
            if quantity <= pipeline.capacity:
                score += 20
            else:
                # 能力不足，但初版允许部分满足
                score -= 30
        
        # 规则3: 高优先级订单满足时间窗（在评分函数外处理，此处预留）
        # 实际实现中，此逻辑在外层优先级排序中处理
        
        return score

# ======================
# 4. 调度核心算法 - 重点重构部分
# ======================

class PipelineScheduler:
    """管线调度器（核心引擎）"""
    
    def __init__(self, path_scoring_strategy: PathScoringStrategy = None,
                 max_batch_ratio: float = 0.4,  # 单次最大调度比例
                 min_batch_size: float = 50.0):  # 最小批次大小(吨)
        self.path_scoring_strategy = path_scoring_strategy or RuleBasedScoring()
        self.max_batch_ratio = max_batch_ratio  # 防止单次调度过大比例
        self.min_batch_size = min_batch_size     # 保证最小批次有意义
    
    def find_feasible_path(self, source_tank_id: str, target_tank_id: str, 
                          oil_type: str, quantity: float, start_time: int,
                          state: SchedulingState) -> Optional[Tuple[List[str], float]]:
        """
        寻找可行路径
        返回: (路径列表, 评分) 或 None
        """
        # 1. 获取所有可能路径（简化：直接返回预定义路径）
        # 实际系统应使用图算法（BFS/DFS）计算所有可行路径
        possible_paths = self._get_all_paths(source_tank_id, target_tank_id, state)
        
        if not possible_paths:
            return None
        
        # 2. 为每条路径评分
        scored_paths = []
        for path in possible_paths:
            # 能力校验
            if not self._check_capacity(path, quantity, start_time, state):
                continue
                
            # 评分
            score = self.path_scoring_strategy.calculate_score(
                path, oil_type, start_time, state, quantity
            )
            scored_paths.append((path, score))
        
        # 3. 选择最高分路径
        if not scored_paths:
            return None
            
        best_path = max(scored_paths, key=lambda x: x[1])
        return best_path
    
    def _get_all_paths(self, source: str, target: str, state: SchedulingState) -> List[List[str]]:
        """获取所有可能路径（简化实现）"""
        # 实际系统应使用图遍历算法
        # 示例数据: 从tank1到tank2的路径
        if source == "tank1" and target == "tank2":
            return [["pipe1"], ["pipe2", "pipe3"]]
        elif source == "tank1" and target == "tank3":
            return [["pipe1", "pipe4"], ["pipe2", "pipe5"]]
        return [["pipe1"]]  # 默认路径
    
    def _check_capacity(self, path: List[str], quantity: float, 
                       start_time: int, state: SchedulingState) -> bool:
        """检查路径能力是否满足需求"""
        duration = quantity / 10.0  # 假设管线速度10吨/小时
        end_time = start_time + duration * 3600  # 转换为秒
        
        for pipeline_id in path:
            pipeline = state.pipelines[pipeline_id]
            
            # 检查管线能力
            if quantity > pipeline.capacity:
                return False
            
            # 检查时间冲突（简化：只检查当前占用）
            for occ in pipeline.occupancy_schedule:
                occ_start, occ_end, _, _ = occ
                if not (end_time <= occ_start or start_time >= occ_end):
                    return False  # 时间冲突
        
        return True
    
    def calculate_duration(self, quantity: float, path: List[str], state: SchedulingState) -> float:
        """计算输送时间（小时）"""
        # 简化：使用路径中最小能力的管线
        min_capacity = min(state.pipelines[pid].capacity for pid in path if pid in state.pipelines)
        return quantity / min_capacity if min_capacity > 0 else float('inf')
    
    def calculate_wash_time(self, tank: TankState) -> float:
        """计算清洗时间（小时）"""
        # 简化：固定2小时
        return 2.0
    
    def determine_batch_size(self, order: CustomerOrder, state: SchedulingState) -> float:
        """
        智能确定批次大小，考虑：
        1. 订单总量和剩余量
        2. 源油罐可用库存
        3. 管线能力
        4. 其他订单优先级
        5. 时间窗口紧迫性
        """
        # 基本批次大小：不超过剩余量的 max_batch_ratio
        base_batch = order.remaining_quantity * self.max_batch_ratio
        
        # 考虑源油罐可用量（找最匹配的油罐）
        available_tanks = self._find_available_tanks(order, state)
        max_available = 0
        for tank_id in available_tanks:
            tank = state.tanks[tank_id]
            available = tank.current_level - tank.safety_min
            if tank.current_oil == order.oil_type:  # 同油品优先
                available = min(available, tank.current_level * 0.8)  # 保留20%余量
            max_available = max(max_available, available)
        
        # 限制批次大小不超过最大可用量
        batch_size = min(base_batch, max_available)
        
        # 考虑管线能力（取路径中最窄管线）
        min_pipeline_capacity = float('inf')
        for pipeline in state.pipelines.values():
            if pipeline.capacity < min_pipeline_capacity:
                min_pipeline_capacity = pipeline.capacity
        
        # 限制单次调度不超过管线4小时输送能力
        max_pipeline_batch = min_pipeline_capacity * 4  # 4小时最大输送量
        batch_size = min(batch_size, max_pipeline_batch)
        
        # 确保不小于最小批次
        batch_size = max(batch_size, self.min_batch_size)
        
        # 确保不超过订单剩余量
        batch_size = min(batch_size, order.remaining_quantity)
        
        # 特殊处理：如果时间窗口紧迫，可增加批次大小
        now = int(time.time())
        time_remaining = order.time_window[1] - now
        if time_remaining < 6 * 3600:  # 6小时内到期
            batch_size = min(order.remaining_quantity, max_available)
        
        # 特殊处理：高优先级订单
        if order.priority >= 8:
            batch_size = min(order.remaining_quantity * 0.6, max_available)  # 高优先级可调度更大比例
        
        return round(batch_size, 2)  # 保留两位小数

    def schedule_order(self, order: CustomerOrder, state: SchedulingState) -> List[DispatchOrder]:
        """
        为单个订单生成调度工单，支持部分调度
        返回: 调度工单列表（可能包含单个批次）
        """
        dispatch_orders = []
        original_remaining = order.remaining_quantity
        
        # 1. 如果订单已完成，直接返回空列表
        if order.is_fully_scheduled():
            return dispatch_orders
        
        # 2. 计算订单的紧急程度，用于选择合适的启发式规则
        current_time = time.time()
        time_until_deadline = order.time_window[1] - current_time
        processing_estimate = self.estimate_processing_time(order, state)
        
        # 紧急度计算 (0-1，1表示最紧急)
        if time_until_deadline <= 0:
            urgency = 1.0
        else:
            urgency = min(1.0, processing_estimate / max(time_until_deadline, 1.0))
        
        # 3. 根据紧急程度和订单特性选择启发式规则
        if urgency > 0.85 or order.priority > 7:
            # 规则1: 紧急订单 - 确保截止时间优先
            batch_size, selected_tank, path, start_time, end_time, need_cleaning = self._apply_deadline_priority_rule(
                order, state, current_time
            )
        elif not order.oil_type or self._has_compatible_tanks(order, state):
            # 规则2: 油品兼容性优先 - 减少清洗次数
            batch_size, selected_tank, path, start_time, end_time, need_cleaning = self._apply_compatibility_priority_rule(
                order, state, current_time
            )
        elif state.calculate_resource_utilization() > 0.8:
            # 规则3: 资源平衡 - 当系统负载高时
            batch_size, selected_tank, path, start_time, end_time, need_cleaning = self._apply_resource_balancing_rule(
                order, state, current_time
            )
        else:
            # 规则4: 默认 - 最小化总处理时间
            batch_size, selected_tank, path, start_time, end_time, need_cleaning = self._apply_processing_time_rule(
                order, state, current_time
            )
        
        # 4. 验证启发式规则返回的结果
        if selected_tank is None or path is None or batch_size < self.min_batch_size * 0.5:
            # 尝试原版逻辑作为备选
            return self._fallback_original_scheduling(order, state)
        
        # 5. 检查时间窗口约束
        if end_time > order.time_window[1]:
            # 尝试缩小批次以满足时间窗口
            reduced_batch = batch_size * 0.7
            if reduced_batch >= self.min_batch_size:
                batch_size = reduced_batch
                duration = self.calculate_duration(batch_size, path, state)
                end_time = start_time + int(duration * 3600)  # 小时转秒
        
        # 6. 再次验证缩小后的批次
        if batch_size < self.min_batch_size * 0.5 or end_time > order.time_window[1]:
            return []  # 无法满足约束，返回空列表
        
        # 7. 创建调度工单
        is_partial = batch_size < order.remaining_quantity
        dispatch_order = DispatchOrder(
            order_id=order.id,
            oil_type=order.oil_type,
            quantity=batch_size,
            source_tank_id=selected_tank,
            target_tank_id=order.target_tank_id,
            pipeline_path=path,
            start_time=start_time,
            end_time=end_time,
            is_partial=is_partial,
            remaining_after=order.remaining_quantity - batch_size
        )
        dispatch_order.cleaning_required = need_cleaning
        
        # 8. 应用调度结果到状态
        self._update_state_with_order(state, dispatch_order, need_cleaning)
        
        # 9. 更新优化指标
        if order.priority >= 7:  # 高优先级阈值
            state.high_priority_satisfied += 1
        if need_cleaning:
            state.oil_switch_count += 1
        state.total_dispatch_orders += 1
        
        # 10. 更新订单状态
        order.mark_partial_fulfillment(dispatch_order.dispatch_id, batch_size, end_time)
        
        # 11. 跟踪部分调度
        if order.id not in state.partially_scheduled_orders:
            state.partially_scheduled_orders[order.id] = original_remaining
        state.partially_scheduled_orders[order.id] -= batch_size
        
        dispatch_orders.append(dispatch_order)
        
        # 注意：一次只调度一个批次，下轮循环再处理剩余部分
        return dispatch_orders
    
    def schedule_order_completely(scheduler: OilScheduler, order: CustomerOrder, state: SchedulingState, 
    
                                max_attempts=10, timeout_seconds=30):
    
        """
    
        完整调度一个订单，支持多批次调度
    
        返回: 所有调度工单的列表
    
        """
    
        all_dispatch_orders = []
    
        start_time = time.time()
    
        attempts = 0
    
        
    
        print(f"\n=== 开始完整调度订单 {order.id} (总需求: {order.quantity}) ===")
    
        
    
        while not order.is_fully_scheduled() and attempts < max_attempts:
    
            # 检查超时
    
            if time.time() - start_time > timeout_seconds:
    
                print(f"  ⚠️ 调度超时 ({timeout_seconds}秒)，停止进一步调度尝试")
    
                break
    
                
    
            attempts += 1
    
            print(f"\n尝试 #{attempts}: 剩余需求 = {order.remaining_quantity:.1f}")
    
            
    
            # 尝试调度当前批次
    
            dispatch_orders = scheduler.schedule_order(order, state)
    
            
    
            if not dispatch_orders:
    
                # 无可行调度方案
    
                if attempts == 1:
    
                    print(f"  ❌ 无法为订单 {order.id} 生成任何调度方案")
    
                else:
    
                    print(f"  ⚠️ 无法生成更多批次，订单部分调度完成")
    
                break
    
            
    
            # 处理返回的调度工单
    
            for disp in dispatch_orders:
    
                all_dispatch_orders.append(disp)
    
                
    
                # 打印批次详情
    
                start_time_str = datetime.fromtimestamp(disp.start_time).strftime('%H:%M:%S')
    
                end_time_str = datetime.fromtimestamp(disp.end_time).strftime('%H:%M:%S')
    
                remaining_str = f"(完全调度)" if order.is_fully_scheduled() else f"(剩余: {order.remaining_quantity:.1f})"
    
                
    
                print(f"  ✓ 批次 #{len(all_dispatch_orders)} 调度成功:")
    
                print(f"    - 源油罐: {disp.source_tank_id}, 数量: {disp.quantity:.1f}")
    
                print(f"    - 时间: {start_time_str} - {end_time_str} ({(disp.end_time-disp.start_time)/3600:.2f}小时)")
    
                print(f"    - 路径: {'->'.join(disp.pipeline_path)}")
    
                print(f"    - 状态: {'需要清洗' if disp.cleaning_required else '无需清洗'}, {remaining_str}")
    
            
    
            # 检查是否达到最小剩余量（避免无限小批次）
    
            if order.remaining_quantity < scheduler.min_batch_size * 0.3:
    
                print(f"  ⚠️ 剩余量 ({order.remaining_quantity:.1f}) 小于最小批次的30%，停止调度")
    
                break
    
        
    
        # 调度结果总结
    
        print(f"\n=== 订单 {order.id} 调度结果 ===")
    
        if order.is_fully_scheduled():
    
            print(f"  ✅ 订单完全调度成功! 共 {len(all_dispatch_orders)} 个批次")
    
        else:
    
            completion_rate = (order.quantity - order.remaining_quantity) / order.quantity * 100
    
            print(f"  ⚠️ 订单部分调度完成 ({completion_rate:.1f}%)")
    
            print(f"    - 已调度: {order.quantity - order.remaining_quantity:.1f}")
    
            print(f"    - 剩余: {order.remaining_quantity:.1f}")
    
            print(f"    - 尝试次数: {attempts}/{max_attempts}")
    
        
    
        print(f"  📊 系统状态: 高优先级满足={state.high_priority_satisfied}, 油品切换={state.oil_switch_count}, 总工单={state.total_dispatch_orders}")
    
        
    
        return all_dispatch_orders

    def _apply_deadline_priority_rule(self, order: CustomerOrder, state: SchedulingState, current_time: float):
        """
        启发式规则1: 截止时间优先
        适用: 紧急订单(高优先级或接近截止时间)
        目标: 确保按时交付
        """
        # 1. 查找可用源油罐，按最早可用时间排序
        available_tanks = self._find_available_tanks(order, state)
        sorted_tanks = sorted(available_tanks, key=lambda t: state.tanks[t].occupied_until)
        
        best_option = None
        min_end_time = float('inf')
        
        # 2. 尝试每个油罐，找到能最早完成的选项
        for tank_id in sorted_tanks:
            tank = state.tanks[tank_id]
            
            # 检查油品兼容性和库存
            if not self._is_tank_compatible(tank, order.oil_type):
                continue
                
            available_oil = tank.current_level - tank.safety_min
            if tank.current_oil == order.oil_type:
                available_oil = min(available_oil, tank.current_level * 0.9)  # 紧急情况下只保留10%余量
            
            if available_oil < self.min_batch_size:
                continue
            
            # 确定批次大小
            batch_size = min(available_oil, order.remaining_quantity, self.max_batch_size)
            
            # 检查是否需要清洗
            need_cleaning = tank.current_oil is not None and tank.current_oil != order.oil_type
            
            # 计算开始时间
            earliest_start = max(tank.occupied_until, order.time_window[0])
            if need_cleaning:
                wash_time = self.calculate_wash_time(tank)
                earliest_start += wash_time * 3600  # 转换为秒
            
            # 寻找可行路径
            path_result = self.find_feasible_path(
                tank_id, order.target_tank_id, order.oil_type, batch_size, 
                int(earliest_start), state
            )
            
            if path_result is None:
                continue
            
            path, _ = path_result
            duration = self.calculate_duration(batch_size, path, state)
            end_time = earliest_start + duration * 3600
            
            # 评估此选项
            if end_time < min_end_time and end_time <= order.time_window[1]:
                min_end_time = end_time
                best_option = (batch_size, tank_id, path, int(earliest_start), int(end_time), need_cleaning)
        
        return best_option if best_option else (None, None, None, None, None, None)
    
    def _apply_compatibility_priority_rule(self, order: CustomerOrder, state: SchedulingState, current_time: float):
        """
        启发式规则2: 油品兼容性优先
        适用: 有兼容油罐可用的情况
        目标: 最小化清洗次数，提高效率
        """
        # 1. 优先查找无需清洗的油罐
        compatible_tanks = self._find_compatible_tanks(order, state)
        sorted_tanks = sorted(compatible_tanks, key=lambda t: (
            state.tanks[t].occupied_until, 
            -state.tanks[t].current_level  # 优先使用库存多的油罐
        ))
        
        # 2. 如果没有完全兼容的油罐，查找需要清洗但油品相似的
        if not sorted_tanks:
            all_available = self._find_available_tanks(order, state)
            sorted_tanks = sorted(all_available, key=lambda t: (
                state.tanks[t].occupied_until + self.calculate_wash_time(state.tanks[t]) * 3600,
                self._oil_compatibility_score(state.tanks[t].current_oil, order.oil_type)
            ), reverse=True)
        
        best_option = None
        
        # 3. 评估每个选项
        for tank_id in sorted_tanks:
            tank = state.tanks[tank_id]
            
            # 检查库存
            available_oil = tank.current_level - tank.safety_min
            if tank.current_oil == order.oil_type:
                available_oil = min(available_oil, tank.current_level * 0.8)  # 保留20%余量
            
            if available_oil < self.min_batch_size:
                continue
            
            # 确定批次大小 (中等大小批次，平衡效率和灵活性)
            batch_size = min(
                available_oil * 0.7,  # 使用70%可用库存
                order.remaining_quantity * 0.5,  # 不超过剩余订单的一半
                self.max_batch_size
            )
            
            if batch_size < self.min_batch_size:
                continue
            
            # 检查是否需要清洗
            need_cleaning = tank.current_oil is not None and tank.current_oil != order.oil_type
            
            # 计算开始时间
            earliest_start = max(tank.occupied_until, order.time_window[0])
            if need_cleaning:
                wash_time = self.calculate_wash_time(tank)
                earliest_start += wash_time * 3600
            
            # 寻找可行路径
            path_result = self.find_feasible_path(
                tank_id, order.target_tank_id, order.oil_type, batch_size, 
                int(earliest_start), state
            )
            
            if path_result is None:
                continue
            
            path, _ = path_result
            duration = self.calculate_duration(batch_size, path, state)
            end_time = earliest_start + duration * 3600
            
            # 优先选择无需清洗且能在时间窗口内完成的方案
            if not need_cleaning or end_time <= order.time_window[1]:
                best_option = (batch_size, tank_id, path, int(earliest_start), int(end_time), need_cleaning)
                break  # 找到最佳兼容选项，立即返回
        
        return best_option if best_option else (None, None, None, None, None, None)
    
    def _apply_resource_balancing_rule(self, order: CustomerOrder, state: SchedulingState, current_time: float):
        """
        启发式规则3: 资源平衡
        适用: 系统负载高时
        目标: 平衡油罐和管道使用，避免瓶颈
        """
        # 1. 评估所有油罐的负载和兼容性
        tank_scores = []
        available_tanks = self._find_available_tanks(order, state)
        
        for tank_id in available_tanks:
            tank = state.tanks[tank_id]
            
            # 检查油品兼容性和库存
            if not self._is_tank_compatible(tank, order.oil_type):
                continue
                
            available_oil = tank.current_level - tank.safety_min
            if available_oil < self.min_batch_size:
                continue
            
            # 计算资源平衡分数 (1.0 = 完美平衡)
            utilization_score = 1.0 - min(1.0, tank.occupied_until / (current_time + 86400))  # 未来24小时利用率
            inventory_score = min(1.0, available_oil / tank.capacity)  # 库存利用率
            compatibility_score = 1.0 if tank.current_oil == order.oil_type else 0.6  # 油品兼容性
            
            # 综合评分
            score = (
                utilization_score * 0.4 +
                inventory_score * 0.3 +
                compatibility_score * 0.3
            )
            
            tank_scores.append((tank_id, score, available_oil))
        
        # 2. 按评分排序
        tank_scores.sort(key=lambda x: x[1], reverse=True)
        
        # 3. 选择最佳选项
        for tank_id, score, available_oil in tank_scores:
            tank = state.tanks[tank_id]
            
            # 确定批次大小 (平衡大小)
            batch_size = min(
                available_oil * 0.6,  # 使用60%可用库存
                order.remaining_quantity * 0.4,  # 不超过剩余订单的40%
                self.max_batch_size
            )
            
            if batch_size < self.min_batch_size:
                continue
            
            # 检查是否需要清洗
            need_cleaning = tank.current_oil is not None and tank.current_oil != order.oil_type
            
            # 计算开始时间
            earliest_start = max(tank.occupied_until, order.time_window[0])
            if need_cleaning:
                wash_time = self.calculate_wash_time(tank)
                earliest_start += wash_time * 3600
            
            # 寻找可行路径 (考虑负载平衡)
            path_result = self.find_feasible_path(
                tank_id, order.target_tank_id, order.oil_type, batch_size, 
                int(earliest_start), state
            )
            
            if path_result is None:
                continue
            
            path, path_score = path_result
            duration = self.calculate_duration(batch_size, path, state)
            end_time = earliest_start + duration * 3600
            
            # 确保在时间窗口内
            if end_time <= order.time_window[1]:
                return (batch_size, tank_id, path, int(earliest_start), int(end_time), need_cleaning)
        
        return (None, None, None, None, None, None)
    
    def _apply_processing_time_rule(self, order: CustomerOrder, state: SchedulingState, current_time: float):
        """
        启发式规则4: 最小化处理时间
        适用: 一般情况
        目标: 最小化总处理时间，提高吞吐量
        """
        # 1. 查找可用油罐，按预计完成时间排序
        available_tanks = self._find_available_tanks(order, state)
        best_option = None
        min_total_time = float('inf')
        
        for tank_id in available_tanks:
            tank = state.tanks[tank_id]
            
            # 检查油品兼容性和库存
            if not self._is_tank_compatible(tank, order.oil_type):
                continue
                
            available_oil = tank.current_level - tank.safety_min
            if available_oil < self.min_batch_size:
                continue
            
            # 确定批次大小 (偏大批次，提高吞吐量)
            batch_size = min(
                available_oil * 0.8,  # 使用80%可用库存
                order.remaining_quantity * 0.6,  # 不超过剩余订单的60%
                self.max_batch_size
            )
            
            if batch_size < self.min_batch_size:
                continue
            
            # 检查是否需要清洗
            need_cleaning = tank.current_oil is not None and tank.current_oil != order.oil_type
            
            # 计算开始时间
            earliest_start = max(tank.occupied_until, order.time_window[0])
            wash_time_sec = 0
            if need_cleaning:
                wash_time = self.calculate_wash_time(tank)
                wash_time_sec = wash_time * 3600
                earliest_start += wash_time_sec
            
            # 寻找可行路径
            path_result = self.find_feasible_path(
                tank_id, order.target_tank_id, order.oil_type, batch_size, 
                int(earliest_start), state
            )
            
            if path_result is None:
                continue
            
            path, _ = path_result
            duration = self.calculate_duration(batch_size, path, state)
            end_time = earliest_start + duration * 3600
            
            # 计算总处理时间 (包括清洗时间)
            total_time = wash_time_sec + duration * 3600
            
            # 选择总处理时间最短且在时间窗口内的选项
            if total_time < min_total_time and end_time <= order.time_window[1]:
                min_total_time = total_time
                best_option = (batch_size, tank_id, path, int(earliest_start), int(end_time), need_cleaning)
        
        return best_option if best_option else (None, None, None, None, None, None)

   
    def _find_available_tanks(self, order: CustomerOrder, state: SchedulingState) -> List[str]:
        """查找可用源油罐（按规则排序）"""
        candidates = []
        
        for tank_id, tank in state.tanks.items():
            # 1. 油品兼容性检查
            if order.oil_type not in tank.compatible_oils:
                continue
            
            # 2. 安全液位检查
            available_capacity = tank.current_level - tank.safety_min
            if available_capacity < self.min_batch_size:  # 至少要能满足最小批次
                continue
            
            # 3. 计算优先级得分
            score = 0
            # 同油品优先
            if tank.current_oil == order.oil_type:
                score += 100
            # 未使用油罐优先
            elif tank.current_oil is None:
                score += 50
            # 位置优先级
            if tank.location == "station":
                score += 20
            
            candidates.append((tank_id, score))
        
        # 按得分排序
        candidates.sort(key=lambda x: x[1], reverse=True)
        return [tid for tid, _ in candidates]
    
    def _update_state_with_order(self, state: SchedulingState, dispatch_order: DispatchOrder, need_cleaning: bool):
        """更新状态（模拟执行工单）"""
        tank = state.tanks[dispatch_order.source_tank_id]
        
        # 更新油罐状态
        tank.current_level -= dispatch_order.quantity
        tank.current_oil = dispatch_order.oil_type
        tank.occupied_until = dispatch_order.end_time
        
        if need_cleaning:
            tank.last_clean_time = dispatch_order.start_time
        
        # 更新管线状态
        for pipeline_id in dispatch_order.pipeline_path:
            pipeline = state.pipelines[pipeline_id]
            pipeline.current_oil = dispatch_order.oil_type
            pipeline.occupancy_schedule.append((
                dispatch_order.start_time, dispatch_order.end_time, 
                dispatch_order.oil_type, dispatch_order.quantity
            ))
    
    def rolling_schedule(self, orders: List[CustomerOrder], base_state: SchedulingState, 
                        max_cycles: int = 10) -> Tuple[List[DispatchOrder], List[CustomerOrder]]:
        """
        滚动调度主流程 - 支持订单拆分
        1. 按优先级排序订单
        2. 多轮调度，每轮处理优先级最高的可调度部分
        3. 冲突处理
        """
        # 1. 创建状态副本（不修改原始状态）
        state = deepcopy(base_state)
        
        # 2. 创建订单副本（不修改原始订单）
        order_copies = [deepcopy(order) for order in orders]
        
        # 3. 调度结果
        all_dispatch_orders = []
        infeasible_orders = []
        
        # 4. 多轮调度
        for cycle in range(max_cycles):
            # 按优先级和剩余量排序（高优先级且剩余量大的优先）
            sorted_orders = sorted(
                [o for o in order_copies if not o.is_fully_scheduled()],
                key=lambda o: (o.priority, o.remaining_quantity),
                reverse=True
            )
            
            # 如果没有可调度的订单，退出
            if not sorted_orders:
                break
            
            # 检查是否所有订单都无法调度
            any_scheduled = False
            
            # 5. 逐个处理订单
            for order in sorted_orders:
                # 智能跳过：如果订单剩余量很小，且时间窗口还很宽裕，可稍后处理
                now = int(time.time())
                time_pressure = (order.time_window[1] - now) / 3600  # 剩余小时数
                if order.remaining_quantity < self.min_batch_size * 1.5 and time_pressure > 4:
                    continue
                
                dispatch_orders = self.schedule_order_completely(order, state)
                
                if dispatch_orders:
                    all_dispatch_orders.extend(dispatch_orders)
                    any_scheduled = True
            
            # 如果本轮没有调度任何订单，退出
            if not any_scheduled:
                break
        
        # 6. 收集未完全调度的订单
        partially_scheduled = []
        fully_infeasible = []
        
        for original_order in orders:
            copy = next((o for o in order_copies if o.id == original_order.id), None)
            if copy:
                if copy.is_fully_scheduled():
                    continue
                elif copy.remaining_quantity < copy.total_quantity:
                    partially_scheduled.append(copy)
                else:
                    fully_infeasible.append(copy)
        
        # 7. 标记冲突订单
        for order in fully_infeasible:
            dispatch_order = DispatchOrder(
                order_id=order.id,
                oil_type=order.oil_type,
                quantity=order.remaining_quantity,
                source_tank_id="",
                target_tank_id=order.target_tank_id,
                pipeline_path=[],
                start_time=order.time_window[0],
                end_time=order.time_window[1]
            )
            dispatch_order.status = "CONFLICT"
            all_dispatch_orders.append(dispatch_order)
        
        return all_dispatch_orders, partially_scheduled + fully_infeasible


