import yaml
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from models.models import Base, Tank, Order, Branch, Plan
from datetime import datetime, timedelta
import uuid

def load_config(config_path='config.yaml'):
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def get_database_url(db_config, include_db=True):

    """生成数据库连接 URL"""

    base = f"{db_config['dialect']}+{db_config['driver']}://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}"

    if include_db:
        return f"{base}/{db_config['database']}"
    else:
        return base


def test_connection():

    config = load_config()
    db_cfg = config['database']
    db_name = db_cfg['database']

    # 第一步：连接 MySQL 服务器（不指定数据库）
    print(f"🔧 正在确保数据库 '{db_name}' 存在...")
    try:

        server_url = get_database_url(db_cfg, include_db=False)
        server_engine = create_engine(server_url, echo=False)
        with server_engine.connect() as conn:
            # 这一步需要配置文件里面的用户有创建权限
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{db_name}` "
                              "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"))
        print(f"✅ 数据库 '{db_name}' 已创建或已存在。")
    except Exception as e:
        print(f"❌ 创建数据库失败: {e}")
        return

    # 第二步：连接到目标数据库
    db_url = get_database_url(db_cfg, include_db=True)
    print(f"🔌 正在连接到: {db_cfg['dialect']}://{db_cfg['host']}:{db_cfg['port']}/{db_name}")
    engine = create_engine(
        db_url,
        echo=db_cfg.get('echo', False),
        pool_size=db_cfg.get('pool_size', 5),
        pool_pre_ping=True  # 建议加上，避免连接失效
    )


    # 第三步：测试连接
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ 数据库连接成功！")
    except Exception as e:
        print(f"❌ 连接目标数据库失败: {e}")
        return

    
    # === 第四步：创建表结构 ===

    print("🛠️ 正在同步表结构...")

    Base.metadata.create_all(engine)

    print("✅ 表结构已同步。")


    # === 第五步：创建 Session ===

    SessionLocal = sessionmaker(bind=engine)

    session = SessionLocal()


    try:

        # ==============================

        # 🧪 开始 CRUD 测试

        # ==============================
        # --- 创建测试数据 ---
        print("\n🧪 开始 CRUD 测试...")

        # 1. 创建 Tank
        tank_id = "T999"
        tank = Tank(
            tank_id=tank_id,
            owner="TestOwner",
            oil_type="MURBAN",
            inventory_m3=10000.0,
            max_capacity_m3=50000.0,
            min_safe_level_m3=2000.0,
            compatible_oils=["MURBAN", "ARABIAN"],
            available_from=datetime.now(),
            status="AVAILABLE"
        )
        session.add(tank)

        # 2. 创建 Branch
        branch_id = "BR-999"
        branch = Branch(
            branch_id=branch_id,
            max_rate_m3h=500.0,
            status="AVAILABLE"
        )

        session.add(branch)

        # 3. 创建 Order
        order_id = "ORD-999"
        order = Order(
            order_id=order_id,
            customer="TestCustomer",
            oil_type="MURBAN",
            required_volume_m3=5000.0,
            earliest_start=datetime.now(),
            deadline=datetime.now() + timedelta(days=3),
            priority=2,
            allow_multi_tank=False
        )
        session.add(order)
        session.commit()
        print("✅ 增（Create）测试通过：Tank、Branch、Order 已插入。")

        # --- 读取测试 ---
        db_tank = session.query(Tank).filter_by(tank_id=tank_id).first()
        db_order = session.query(Order).filter_by(order_id=order_id).first()
        assert db_tank is not None and db_order is not None
        print("✅ 查（Read）测试通过。")


        # --- 更新测试 ---
        db_tank.inventory_m3 = 9000.0
        db_order.status = "ASSIGNED"
        session.commit()
        updated_tank = session.query(Tank).filter_by(tank_id=tank_id).first()
        assert updated_tank.inventory_m3 == 9000.0
        print("✅ 改（Update）测试通过。")


        # --- 创建 Plan（测试外键关系）---
        plan_id = str(uuid.uuid4())[:8]
        plan = Plan(
            plan_id=plan_id,
            order_id=order_id,
            branch_id=branch_id,
            start_time=datetime.now(),
            end_time=datetime.now() + timedelta(hours=10),
            rate_m3h=400.0,
            status="ACTIVE"
        )
        session.add(plan)
        session.commit()


        # 验证 relationship 是否可用
        fetched_plan = session.query(Plan).filter_by(plan_id=plan_id).first()
        assert fetched_plan.order.customer == "TestCustomer"
        assert fetched_plan.branch.branch_id == branch_id
        print("✅ 关系字段（relationship）测试通过。")


        # --- 删除测试 ---
        session.delete(db_tank)
        session.delete(db_order)
        session.delete(branch)
        session.delete(fetched_plan)
        session.commit()

        # 确保已删除
        assert session.query(Tank).filter_by(tank_id=tank_id).count() == 0
        assert session.query(Order).filter_by(order_id=order_id).count() == 0
        print("✅ 删（Delete）测试通过。")
        print("\n🎉 所有 ORM 映射与 CRUD 测试通过！")
    except Exception as e:
        print(f"❌ CRUD 测试失败: {e}")
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    test_connection()
