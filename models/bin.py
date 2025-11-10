
import numpy as np

# 定义油罐类
class Bin:
    def __init__(self, tank_id, capacity, current_level, oil_type):
        self.tank_id = tank_id          # 油罐编号
        self.capacity = capacity        # 油罐容量
        self.current_level = current_level  # 当前油量
        self.oil_type = oil_type        # 油品种类

    def can_transfer(self, amount):
        # 检查油罐是否可以传输一定量的油
        return self.current_level >= amount

    def transfer_oil(self, amount):
        if self.can_transfer(amount):
            self.current_level -= amount
            return True
        return False


