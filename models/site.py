
    def __init__(self, site_id, tanks):
        self.site_id = site_id               # 站点编号
        self.tanks = tanks                   # 站点内的油罐列表

    def get_tank(self, oil_type):
        # 获取站点内某种油品种类的油罐
        for tank in self.tanks:
            if tank.oil_type == oil_type:
                return tank
        return None
