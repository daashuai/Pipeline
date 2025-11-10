# init_db_and_user.py
import yaml
from sqlalchemy import create_engine, text
import sys

def load_config(config_path='config.yaml'):
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def init_database_and_user():
    config = load_config()
    db_cfg = config['database']

    # 获取配置
    host = db_cfg['host']
    port = db_cfg['port']
    db_name = db_cfg['database']
    app_user = db_cfg['username']
    app_pass = db_cfg['password']
    root_user = db_cfg.get('root_username', 'root')
    root_pass = db_cfg['root_password']  # 必须提供

    # 构建 root 连接 URL（不带数据库）
    server_url = f"mysql+pymysql://{root_user}:{root_pass}@{host}:{port}/"

    print("🔧 正在连接 MySQL 服务器（无数据库）...")
    try:
        engine = create_engine(server_url, echo=False)
        with engine.connect() as conn:
            # 1. 创建数据库
            print(f"📦 创建数据库 '{db_name}' (如果不存在)...")
            conn.execute(text(
                f"CREATE DATABASE IF NOT EXISTS `{db_name}` "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            ))

            # 2. 创建用户（MySQL 5.7+ / 8.0 兼容写法）
            print(f"👤 创建用户 '{app_user}'@'localhost' (如果不存在)...")
            try:
                # 尝试创建用户（如果已存在会报错，但我们可以忽略）
                conn.execute(text(
                    f"CREATE USER IF NOT EXISTS '{app_user}'@'localhost' IDENTIFIED BY '{app_pass}'"
                ))
            except Exception as e:
                # 某些旧版 MySQL 不支持 IF NOT EXISTS，手动检查
                if "exists" in str(e).lower():
                    print("   👤 用户已存在，跳过创建。")
                else:
                    raise

            # 3. 授予权限
            print(f"🔑 授予 '{app_user}'@'localhost' 对数据库 '{db_name}' 的全部权限...")
            conn.execute(text(
                f"GRANT ALL PRIVILEGES ON `{db_name}`.* TO '{app_user}'@'localhost'"
            ))
            conn.execute(text("FLUSH PRIVILEGES"))

        print("✅ 初始化成功！")
        print(f"   数据库: {db_name}")
        print(f"   用户: {app_user}@localhost")

    except Exception as e:
        print(f"❌ 初始化失败: {e}")
        sys.exit(1)

if __name__ == "__main__":
    init_database_and_user()
