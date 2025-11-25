import yaml
from typing import Dict, Any, Type, List
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from data_class import DispatchOrder
from models import OilDB, TankDB, PipelineDB, BranchDB, CustomerDB, CustomerOrderDB, SiteDB, DispatchOrderDB

def load_config(config_path='config.yaml'):

    """加载配置文件"""

    with open(config_path, 'r', encoding='utf-8') as f:

        return yaml.safe_load(f)


def get_database_url(db_config, include_db=True):

    """生成数据库连接 URL"""

    base = f"{db_config['dialect']}+{db_config['driver']}://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}"

    if include_db:

        return f"{base}/{db_config['database']}"

    else:

        return base

def table_2_dict_by_pk(model_class, rows):
    # {
    # 1: {  # tank_id 为 1 的记录
    #     'name': 'Main Tank',
    #     'capacity': 1000,
    #     'location': 'North Zone'
    # },
    # 2: {  # tank_id 为 2 的记录
    #     'name': 'Backup Tank',
    #     'capacity': 500,
    #     'location': 'South Zone'
    # },
    # # ... 其他记录
    # }
    from sqlalchemy import inspect
    mapper = inspect(model_class)
    pk_column = mapper.primary_key[0]  # 获取第一个主键列（假设单主键）
    pk_name = pk_column.name

    result = {}
    for row in rows: 
        # 获取主键值
        pk_value = getattr(row, pk_name)
        # 获取所有列名
        columns = [column.name for column in mapper.columns]
        # 构建行数据（排除主键）
        row_data = {
            col: getattr(row, col)
            for col in columns
            if col != pk_name
        }
        result[pk_value] = row_data

    return result


def load_table_data(table_name: str):
    """
    从数据库加载指定表的数据
    
    Args:
        table_name: 要加载的表名 (例如: "Tank" 或 "Pipeline")
    
    Returns:
        Dict[str, Any]: 以ID为键的对象字典
    """
    # 模型类映射
    model_map = {
        "Tank": TankDB,
        "Pipeline": PipelineDB,
        "Branch": BranchDB
    }
    
    # 验证表名
    if table_name not in model_map:
        raise ValueError(f"不支持的表名: {table_name}. 支持的表: {list(model_map.keys())}")
    
    # 获取对应的模型类
    model_class = model_map[table_name]
    
    # 加载配置
    config = load_config()
    db_cfg = config['database']
    
    # 创建数据库连接
    db_url = get_database_url(db_cfg, include_db=True)
    engine = create_engine(
        db_url,
        echo=db_cfg.get('echo', False),
        pool_size=db_cfg.get('pool_size', 5),
        pool_pre_ping=True
    )
    
    # 创建会话
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    
    try:
        # 使用模型类进行查询
        table = session.query(model_class).all()
        result = table_2_dict_by_pk(model_class, table)
        # session.expunge_all()
        
        # # 根据模型类型构建结果字典
        # for record in db_records:
        #     if table_name == "Tank":
        #         result[record.tank_id] = Tank(
        #             tank_id=record.tank_id,
        #             current_volume=record.inventory_m3,
        #             max_capacity=record.max_capacity_m3,
        #             compatible_oils=record.compatible_oils,
        #             status=record.status
        #         )
        #     elif table_name == "Pipeline":
        #         result[record.pipeline_id] = Pipeline(
        #             pipeline_id=record.pipeline_id,
        #             source=record.source,
        #             destination=record.destination,
        #             max_flow_rate=record.max_flow_rate,
        #             current_flow_rate=record.current_flow_rate
        #         )
        
        return result
    
    finally:
        session.close()

def load_table_as_objects(table_name: str) -> List[Any]:
    """
    从数据库加载指定表的数据并直接返回业务对象列表
    
    Args:
        table_name: 要加载的表名 (例如: "Tank" 或 "Pipeline")
    
    Returns:
        List[Any]: 业务对象列表
    """
    # 加载配置
    config = load_config()
    db_cfg = config['database']
    
    # 创建数据库连接
    db_url = get_database_url(db_cfg, include_db=True)
    engine = create_engine(
        db_url,
        echo=db_cfg.get('echo', False),
        pool_size=db_cfg.get('pool_size', 5),
        pool_pre_ping=True
    )
    
    # 模型类映射
    model_map = {
        "Tank": TankDB,
        "Pipeline": PipelineDB,
        "Branch": BranchDB,
        "Customer": CustomerDB,
        "CustomerOrder": CustomerOrderDB,
        "DispatchOrder": DispatchOrderDB,
        "Site": SiteDB,
        "Oil": OilDB
    }
    
    if table_name not in model_map:
        raise ValueError(f"不支持的表名: {table_name}. 支持的表: {list(model_map.keys())}")
    
    model_class = model_map[table_name]
    
    # 创建会话
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    
    try:
        # 查询所有记录
        db_objects = session.query(model_class).all()
        
        # 转换为业务对象
        business_objects = [obj.to_object() for obj in db_objects]
        return business_objects
    
    finally:
        session.close()
