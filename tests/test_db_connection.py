# test_db_connection.py
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

 if __name__ == "__main__":
    test_connection()
