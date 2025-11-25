from abc import ABC, abstractmethod
from copy import deepcopy
from typing import List, Dict, Tuple, Optional, Any
import time
import math
from state import State
from data_class import CustomerOrder, DispatchOrder
from dispatch_order_queue import DispatchOrderQueueManager
import logging

logger = logging.getLogger(__name__)

class PathScoring:
    """简化版路径评分策略"""
    def calculate_score(self, path: List[str], oil_type: str, start_time: int, 
                       state: State, quantity: float) -> float:
        """简单评分：优先选择无需清洗的路径"""
        score = 0.0
        
        for pipeline_id in path:
            pipeline = state.pipelines[pipeline_id]
            # 无需清洗的路径优先
            if pipeline.current_oil == oil_type:
                score += 100
            elif pipeline.current_oil is not None:
                score -= 80  # 需要清洗
        
        # 考虑管线容量
        min_capacity = min(state.pipelines[pid].capacity for pid in path)
        if quantity <= min_capacity:
            score += 20
        else:
            score -= 30
            
        return score

class Scheduler:
    """简化版调度器，专注于订单拆分、路径搜索和状态更新"""
    
    def __init__(self, min_batch_size: float = 50.0):
        """初始化调度器
        
        Args:
            min_batch_size: 最小批次大小(吨)
        """
        self.path_scoring = PathScoring()
        self.min_batch_size = min_batch_size
        self.failed_orders = []  # 记录失败的订单

    def split_order(self, order: CustomerOrder) -> List[DispatchOrder]:
        """将订单拆分为两个调度工单
        
        Args:
            order: 原始客户订单
            
        Returns:
            拆分后的调度工单列表
        """
        # 检查是否需要拆分
        if order.undispatched_volume <= self.min_batch_size * 2:
            # 如果订单太小，不拆分，只创建一个调度工单
            single_dispatch = DispatchOrder(
                dispatch_order_id=str(order.customer_order_id) + "_01",
                customer_order_id=order.customer_order_id,
                site_id=order.site_id,
                oil_type=order.oil_type,
                required_volume=order.undispatched_volume,
                status="DRAFT"
            )
            return [single_dispatch]
        
        # 简单拆分：50%-50%
        first_quantity = order.undispatched_volume / 2
        second_quantity = order.undispatched_volume - first_quantity
        
        # 创建第一个调度工单
        first_dispatch = DispatchOrder(
            dispatch_order_id=str(order.customer_order_id) + "_01",
            customer_order_id=order.customer_order_id,
            site_id=order.site_id,
            oil_type=order.oil_type,
            required_volume=first_quantity,
            status="DRAFT"
        )
        
        # 创建第二个调度工单
        second_dispatch = DispatchOrder(
            dispatch_order_id=str(order.customer_order_id) + "_02",
            customer_order_id=order.customer_order_id,
            oil_type=order.oil_type,
            required_volume=second_quantity,
            status="DRAFT"
        )
        
        return [first_dispatch, second_dispatch]

    # def split_order(self, order: CustomerOrder) -> List[CustomerOrder]:
    #     """将订单拆分为两个子订单
    #     
    #     Args:
    #         order: 原始订单
    #         
    #     Returns:
    #         拆分后的子订单列表
    #     """
    #     if order.undispatched_volume <= self.min_batch_size * 2:
    #         # 如果订单太小，不拆分
    #         return [order]
    #     
    #     # 简单拆分：50%-50%
    #     first_quantity = order.undispatched_volume / 2
    #     second_quantity = order.undispatched_volume - first_quantity
    #     
    #     # 创建第一个子订单
    #     first_order = deepcopy(order)
    #     first_order.undispatched_volume = first_quantity
    #     first_order.customer_order_id = order.customer_order_id
    #     first_order. = f"{order.id}_part1"
    #     
    #     # 创建第二个子订单
    #     second_order = deepcopy(order)
    #     second_order.remaining_quantity = second_quantity
    #     second_order.original_order_id = order.id
    #     second_order.id = f"{order.id}_part2"
    #     
    #     return [first_order, second_order]

    def _get_all_paths(self, source_tank_id: str, target_tank_id: str, state: State):
        """
        获取从源油罐到目标油罐的所有可能路径
        
        路径表示:
        - 跨站点路径: [pipeline_id, branch_id, target_tank_id]
        - 同站点路径: ["LOCAL", branch_id, target_tank_id] 或 ["DIRECT", branch_id, target_tank_id]
        
        Args:
            source_tank_id: 源油罐ID
            target_tank_id: 目标油罐ID
            state: 调度状态
            
        Returns:
            所有可能路径的列表 [source_tank_id, pipeline_id, branch_id, target_tank_id]
        """
        # 检查源油罐和目标油罐是否存在
        if source_tank_id not in state.tanks or target_tank_id not in state.tanks:
            print(f"警告：源油罐 {source_tank_id} 或目标油罐 {target_tank_id} 不存在")
            return []
        
        source_tank = state.tanks[source_tank_id]
        target_tank = state.tanks[target_tank_id]
        source_site_id = source_tank.site_id
        target_site_id = target_tank.site_id
        
        paths = [] # [source_tank_id, pipeline_id, branch_id, target_tank_id]
        
        # 情况1: 源油罐和目标油罐在同一站点
        if source_site_id == target_site_id:
            path = [source_tank.tank_id, "LOCAL", "LOCAL", "LOCAL", target_tank.tank_id]
            paths.extend(path)
            return paths
        
        # 情况2: 源油罐和目标油罐在不同站点
        # 先尝试查找通过主管道的路径
        pipeline_paths = self._get_pipeline_paths(source_tank, target_tank, state)
        paths.extend(pipeline_paths)
        
        # 再尝试查找直接通过分支的路径（不经过主管道）
        direct_branch_paths = self._get_direct_branch_paths(source_tank, target_tank, state)
        paths.extend(direct_branch_paths)
        
        # 如果没有找到任何路径，返回空列表
        if not paths:
            print(f"警告：未找到从站点 {source_site_id} 到站点 {target_site_id} 的有效路径")
        
        return paths
    
    def _get_pipeline_paths(self, source_tank, target_tank, state: State):
        """
        根据source_tank，找到从tank到source site的branch，找到从site到pipeline的branch，
        然后根据target tank，找到从pipeline到target site的branch，从target site 到tank的branch，
        输出为[source_tank_id, branch_id, pipeline_id, branch_id, target_tank_id]
    
        Args:
            source_tank: 源油罐对象
            target_tank: 目标油罐对象
            state: 调度状态对象
    
        Returns:
            List[List[str]]: 路径列表，每条路径格式为 [source_tank_id, branch_id, pipeline_id, branch_id, target_tank_id]
        """
        paths = []
        
        # 获取源站点和目标站点ID
        source_site_id = source_tank.site_id
        target_site_id = target_tank.site_id
        
        
        # 步骤1: 找到从source_tank到source_site的branch
        source_branches = []
        for branch in state.branches.values():
            # 查找连接到源站点的分支，from_id为tank_id，to_id为site_id
            if branch.to_id == str(source_site_id) and branch.from_id == source_tank.tank_id:
                source_branches.append(branch)
        
        # 步骤2: 找到从source_site到pipeline的branch
        source_site_to_pipeline_branches = []
        for branch in state.branches.values():
            # 查找连接源站点到主管道的分支，from_id为site_id，to_id为pipe_id
            if branch.from_id == str(source_site_id) and branch.to_id in state.pipelines:
                source_site_to_pipeline_branches.append(branch)
        
        # 步骤3: 找到从pipeline到target_site的branch
        pipeline_to_target_site_branches = []
        for branch in state.branches.values():
            # 查找连接主管道到目标站点的分支，from_id为pipe_id，to_id为site_id
            if branch.to_id == str(target_site_id) and branch.from_id in state.pipelines:
                pipeline_to_target_site_branches.append(branch)
        
        # 步骤4: 找到从target_site到target_tank的branch
        target_branches = []
        for branch in state.branches.values():
            # 查找连接目标站点到目标油罐的分支，from_id为site_id，to_id为tank_id
            if branch.from_id == str(target_site_id) and branch.to_id == target_tank.tank_id:
                target_branches.append(branch)
        
        # 构建完整路径
        for source_branch in source_branches:
            for source_site_to_pipe_branch in source_site_to_pipeline_branches:
                # 确保源站点到主管道的分支与源站点一致
                if source_site_to_pipe_branch.from_id == source_site_id:
                    # 找到共同的主管道
                    pipe_id = source_site_to_pipe_branch.to_id
                    
                    for pipe_to_target_site_branch in pipeline_to_target_site_branches:
                        # 确保主管道一致
                        if pipe_to_target_site_branch.from_id == pipe_id:
                            for target_branch in target_branches:
                                # 确保目标站点一致
                                if target_branch.from_id == target_site_id:
                                    # 构建完整路径
                                    path = [
                                        source_tank.tank_id,
                                        source_branch.branch_id,
                                        pipe_id,
                                        pipe_to_target_site_branch.branch_id,
                                        target_tank.tank_id
                                    ]
                                    paths.append(path)
        
        return paths
    


    def _get_direct_branch_paths(self, source_tank, target_tank, state: State) -> List[List[str]]:
        """
        获取直接连接源油罐和目标油罐的分支路径
        
        Args:
            source_tank: 源油罐对象
            target_tank: 目标油罐对象
            state: 调度状态对象
            
        Returns:
            List[List[str]]: 直接分支路径列表，每条路径格式为 [source_tank_id, branch_id, target_tank_id]
        """
        paths = []
        
        # 在所有分支中寻找 from_id 为 source_tank_id, to_id 为 target_tank_id 的分支
        for branch in state.branches.values():
            if branch.from_id == source_tank.tank_id and branch.to_id == target_tank.tank_id:
                # 构建直接路径
                path = [
                    source_tank.tank_id,
                    branch.branch_id,
                    "DIRECT",
                    branch.branch_id,
                    target_tank.tank_id
                ]
                paths.append(path)
        
        return paths

    def _check_capacity(self, path: List[str], quantity: float, start_time: int, state: State) -> bool:
        """检查路径容量是否满足需求"""
        for pipeline_id in path:
            pipeline = state.pipelines[pipeline_id]
            if quantity > pipeline.capacity:
                return False
        return True

    def _check_time_conflict(self, path: List[str], quantity: float, start_time: int, state: State) -> bool:
        """检查时间冲突"""
        duration = self.calculate_duration(quantity, path, state)
        end_time = start_time + int(duration * 3600)  # 小时转换为秒
        
        for pipeline_id in path:
            pipeline = state.pipelines[pipeline_id]
            for occ_start, occ_end, _, _ in pipeline.occupancy_schedule:
                if not (end_time <= occ_start or start_time >= occ_end):
                    return False  # 时间冲突
        return True

    def calculate_duration(self, quantity: float, path: List[str], state: State) -> float:
        """计算输送时间（小时）"""
        min_capacity = float('inf')
        for pipeline_id in path:
            pipeline = state.pipelines[pipeline_id]
            if pipeline.capacity < min_capacity:
                min_capacity = pipeline.capacity
        
        if min_capacity <= 0:
            return float('inf')
        
        return quantity / min_capacity

    def calculate_wash_time(self, pipeline_ids: List[str], state: State) -> float:
        """计算清洗时间（小时）"""
        # 简化：固定2小时
        return 2.0

    def schedule_dispatch_order(self, dispatch_order: DispatchOrder, queue: DispatchOrderQueueManager) -> Optional[DispatchOrder]:
        """调度单个调度工单，找到合适的源油罐、目标油罐和运输路径，并更新状态"""

        # 1. 为调度工单找到合适的source-tank（从哪个油罐出油）
        state = queue.get_order_state_last()
        source_tank_id = self._find_best_source_tank(dispatch_order, state)
        if not source_tank_id:
            print(f"找不到合适的源油罐，油品类型: {dispatch_order.oil_type}, 需要体积: {dispatch_order.required_volume}")
            return None
        
        # 2. 找到往哪个终点的油罐存放（目标油罐）
        # 根据dispatch_order中的信息确定目标油罐，或者根据业务逻辑查找
        target_tank_id = self._find_best_target_tank(dispatch_order, state)
        if not target_tank_id:
            # 如果调度工单中没有指定目标油罐，则根据业务需求确定
            # 例如，可以使用dispatch_order.customer_order_id关联的entry_tank_id
            # 这里我们假设目标油罐已经指定在dispatch_order中
            print(f"调度工单 {dispatch_order.dispatch_order_id} 未指定目标油罐")
            return None
        
        # 3. 寻找运输路径
        path = self._find_feasible_path(source_tank_id,target_tank_id,dispatch_order,state)
        if path is None:
            print(f"找不到可行路径")
            return None
        
        # 4. 根据找到的信息更新调度工单
        # 更新调度工单中的源油罐、目标油罐和路径信息
        dispatch_order.source_tank_id = source_tank_id
        dispatch_order.target_tank_id = target_tank_id
        dispatch_order.pipeline_path = path

        queue.add_order(dispatch_order)

        
        # 计算开始和结束时间
        # duration = self.calculate_duration(dispatch_order.required_volume, path, state)
        # start_time = dispatch_order.start_time if dispatch_order.start_time > 0 else int(time.time())
        # end_time = start_time + int(duration * 3600)  # 转换为秒
        # 
        # dispatch_order.start_time = start_time
        # dispatch_order.end_time = end_time
        # 设置工单状态为已调度
        # dispatch_order.status = "SCHEDULED"
        
        # 5. 用更新好的调度工单去更新state信息
        # 更新基础信息
        # state.add_dispatch_order(dispatch_order.__dict__)
        # 
        # # 更新调度工单队列信息（dispatcher）
        # # 这里通过state.order_dispatcher来管理调度工单队列
        # if state.order_dispatcher:
        #     # 根据时间将工单移动到相应状态
        #     from datetime import datetime
        #     start_dt = datetime.fromtimestamp(dispatch_order.start_time)
        #     end_dt = datetime.fromtimestamp(dispatch_order.end_time)
        #     current_time = datetime.now()
        #     
        #     if end_dt < current_time:
        #         state.order_dispatcher.move_order_to_completed(dispatch_order.dispatch_order_id)
        #     elif start_dt <= current_time <= end_dt:
        #         state.order_dispatcher.move_order_to_running(dispatch_order.dispatch_order_id)
        #     else:
        #         state.order_dispatcher.move_order_to_pending(dispatch_order.dispatch_order_id)
        return dispatch_order

    def _find_best_target_tank(self, dispatch_order: DispatchOrder, state: State) -> Optional[str]:
        """根据dispatch_order的目标站点ID，在该站点的所有油罐中查找最适合存放的油罐"""
        
        target_site_id = dispatch_order.site_id
        available_site_tanks = []
        for tank_id, tank in state.tanks.items():
            if tank.site_id == target_site_id:
                # 检查油罐状态是否可用
                if tank.status != "AVAILABLE":
                    continue
                    
                # 检查油品兼容性
                if tank.oil_type != dispatch_order.oil_type and tank.oil_type is not None:
                    # 检查是否兼容（简化：假定可以转换，但可能需要清洗）
                    continue
                    
                # 检查容量是否足够
                available_capacity = tank.safe_tank_capacity - tank.inventory
                if available_capacity < dispatch_order.required_volume:
                    continue

                available_site_tanks.append(tank)
        # 在目标站点的油罐中查找最适合的油罐
        best_tank_id = None
        best_score = -float('inf')
        
        for tank in available_site_tanks:
            # tank = state.tanks[tank_id]
            
                            
            # 评分：同油品优先，容量余量适中的优先
            score = 0
            if tank.oil_type == dispatch_order.oil_type:
                score += 100  # 同油品无清洗
            elif tank.oil_type is None:
                score += 50  # 空罐
            else:
                score -= 20  # 需要清洗
                
            # 容量利用率评分（避免过度填充）
            if tank.safe_tank_capacity > 0:
                target_inventory = tank.inventory + dispatch_order.required_volume
                capacity_utilization = min(target_inventory / tank.safe_tank_capacity, 1.0) * 30
                score += capacity_utilization
                
            # 避免液位过高
            if tank.safe_tank_capacity > 0:
                target_level = (tank.current_level * tank.safe_tank_capacity + 
                               dispatch_order.required_volume) / tank.safe_tank_capacity
                if target_level >= tank.safe_tank_level * 0.9:
                    score -= 50
                elif target_level >= tank.safe_tank_level * 0.8:
                    score -= 20
                
            if score > best_score:
                best_score = score
                best_tank_id = tank.tank_id
        
        return best_tank_id

    def _find_best_source_tank(self, dispatch_order: DispatchOrder, state: State) -> Optional[str]:
        """查找最佳源油罐"""
        oil_type = dispatch_order.oil_type
        required_volume = dispatch_order.required_volume
        best_tank_id = None
        best_score = -float('inf')
        
        for tank_id, tank in state.tanks.items():
            if not "SOURCE" in tank.tank_type:
                continue

            # 检查油罐状态是否可用
            if tank.status != "AVAILABLE":
                continue
                
            # 检查油品兼容性
            if tank.oil_type != oil_type and tank.oil_type is not None:
                # 检查是否兼容（简化：假定可以转换，但可能需要清洗）
                continue
                
            # 检查可用库存
            available_inventory = tank.inventory - tank.min_safe_level
            if available_inventory < required_volume:
                continue
                
            # 评分：同油品优先，库存多的优先，避免低液位
            score = 0
            if tank.oil_type == oil_type:
                score += 100  # 同油品无清洗
            elif tank.oil_type is None:
                score += 50  # 空罐
            else:
                score -= 20  # 需要清洗
                
            # 库存利用率评分
            if tank.safe_tank_capacity > 0:
                inventory_utilization = (tank.inventory / tank.safe_tank_capacity) * 30
                score += inventory_utilization
                
            # 避免液位过低
            if tank.current_level <= tank.safe_tank_level:
                score -= 50
            elif tank.current_level <= tank.safe_tank_level * 0.3:
                score -= 30
                
            if score > best_score:
                best_score = score
                best_tank_id = tank_id
        
        return best_tank_id

    def _find_feasible_path(self, source_tank_id: str, target_tank_id: str,
                            dispatch_order: DispatchOrder, state: State):
        """寻找可行路径
        
        Args:
            source_tank_id: 源油罐ID
            target_tank_id: 目标油罐ID
            oil_type: 油品类型
            quantity: 输送数量
            start_time: 开始时间
            state: 调度状态
            
        Returns:
            (路径, 评分, 失败原因) 或 None
        """
        # 获取所有可能路径
        possible_paths = self._get_all_paths(source_tank_id, target_tank_id, state)
        if not possible_paths:
            return None, -float('inf'), "NO_PATH_AVAILABLE"
        
        # # 为每条路径评分
        # scored_paths = []
        # failure_reasons = []
        # 
        # for path in possible_paths:
        #     # 检查容量
        #     if not self._check_capacity(path, quantity, start_time, state):
        #         failure_reasons.append("INSUFFICIENT_CAPACITY")
        #         continue
        #         
        #     # 检查时间冲突
        #     if not self._check_time_conflict(path, quantity, start_time, state):
        #         failure_reasons.append("TIME_CONFLICT")
        #         continue
        #         
        #     # 评分
        #     score = self.path_scoring.calculate_score(path, oil_type, start_time, state, quantity)
        #     scored_paths.append((path, score))
        # 
        # if not scored_paths:
        #     # 没有找到可行路径，返回第一个失败原因
        #     return None, -float('inf'), failure_reasons[0] if failure_reasons else "UNKNOWN_FAILURE"
        # 
        # # 选择最高分路径
        # best_path = max(scored_paths, key=lambda x: x[1])
        # return best_path[0], best_path[1], "SUCCESS"

        return possible_paths[0]


    def update_state(self, state: State, dispatch_order: DispatchOrder):
        """更新状态"""
        # 1. 更新源油罐
        source_tank = state.tanks[dispatch_order.source_tank_id]
        source_tank.current_level -= dispatch_order.quantity
        
        # 如果油罐变空，重置油品类型
        if source_tank.current_level <= source_tank.safety_min + 0.1:
            source_tank.current_oil = None
        
        # 设置新的油品类型（如果需要）
        if dispatch_order.cleaning_required:
            source_tank.last_clean_time = dispatch_order.start_time
            source_tank.current_oil = dispatch_order.oil_type
        
        # 更新占用时间
        source_tank.occupied_until = max(source_tank.occupied_until, dispatch_order.end_time)
        
        # 2. 更新管线
        for pipeline_id in dispatch_order.pipeline_path:
            pipeline = state.pipelines[pipeline_id]
            
            # 更新当前油品
            pipeline.current_oil = dispatch_order.oil_type
            
            # 添加占用计划
            pipeline.occupancy_schedule.append((
                dispatch_order.start_time,
                dispatch_order.end_time,
                dispatch_order.oil_type,
                dispatch_order.quantity
            ))
            
            # 如果需要清洗，记录清洗时间
            if dispatch_order.cleaning_required:
                pipeline.last_clean_time = dispatch_order.start_time
        
        # 3. 更新优化指标
        if dispatch_order.cleaning_required:
            state.oil_switch_count += 1
        state.total_dispatch_orders += 1



    def schedule_order(self, order: CustomerOrder, queue: DispatchOrderQueueManager) -> List[DispatchOrder]:
        """
        调度单个订单（兼容旧接口）
        
        Args:
            order: 要调度的客户订单
            state: 当前调度状态
            
        Returns:
            生成的调度工单列表
        """
        dispatch_orders = []
        undispatched_volume = order.undispatched_volume
        
        # 1. 如果订单已完成，直接返回空列表
        if order.is_fully_scheduled():
            return dispatch_orders
        
        # 2. 尝试拆分订单
        dispatch_orders = self.split_order(order)
        
        # 3. 为每个子订单生成调度
        for o in dispatch_orders:
            # 调度单个批次
            dispatch_order = self.schedule_dispatch_order(o, queue)
            
            # if dispatch_order:
            #     # 更新状态
            #     self.update_state(state, dispatch_order)
            #     
            #     # 更新订单状态
            #     if dispatch_order.is_partial:
            #         sub_order.mark_partial_fulfillment(
            #             dispatch_order.dispatch_id,
            #             dispatch_order.quantity,
            #             dispatch_order.end_time
            #         )
            #         # 更新原始订单的剩余量
            #         order.remaining_quantity -= dispatch_order.quantity
            #     else:
            #         sub_order.mark_fully_fulfilled(dispatch_order.dispatch_id, dispatch_order.end_time)
            #         order.remaining_quantity = 0
            #     
            #     dispatch_orders.append(dispatch_order)
            # else:
            #     # 调度失败，记录原因
            #     logger.warning(f"订单 {order.id} 调度失败，剩余量: {order.remaining_quantity}")
            #     break
        
        # 4. 如果没有生成任何调度工单，返回空列表
        # if not dispatch_orders:
        #     return []
        
        # 5. 记录调度结果
        # total_scheduled = undispatched_volume - order.remaining_quantity
        # logger.info(f"订单 {order.id} 调度成功: {total_scheduled}/{undispatched_volume} 吨")
        
        return dispatch_orders
    
    def rolling_schedule(self, orders: List[CustomerOrder], queue:DispatchOrderQueueManager) -> Tuple[List[DispatchOrder], List[CustomerOrder]]:
        """滚动调度多个订单
        
        Args:
            orders: 订单列表
            state: 调度状态
            
        Returns:
            (调度成功的工单列表, 未调度的订单列表)
        """
        all_dispatch_orders = []
        unscheduled_orders = []
        
        # 按优先级排序订单
        sorted_orders = sorted(orders, key=lambda x: x.priority, reverse=True)
        
        for order in sorted_orders:
            if order.is_fully_scheduled():
                continue
                
            # 调用单个订单调度函数
            dispatch_orders = self.schedule_order(order, queue)
            
            if dispatch_orders:
                # 添加到成功调度的工单列表
                all_dispatch_orders.extend(dispatch_orders)
            else:
                # 调度失败，添加到未调度列表
                unscheduled_orders.append(order)
                logger.warning(f"订单 {order.customer_order_id} 无法调度，已加入未调度列表")
        
        return queue.queue
