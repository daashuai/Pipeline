# === ORM Session 工具 ===
def init_db(db_url: str = 'sqlite:///pipeline_batch.db'):
    engine = create_engine(db_url, echo=False, future=True)
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)
    return SessionLocal


def load_data_from_tank_info_json(session, json_file_path: str = "data.json"):
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    # 添加 Tank 数据
    tanks_data = data
    for tank_info in tanks_data:
        tank = TankInfo(**tank_info)
        session.add(tank)
    
    session.commit()
    print("tank_info数据导入成功！")


def load_data_from_tank_detail_json(session, json_file_path: str = "data.json"):
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    # 添加 Tank 数据
    tanks_data = data
    for tank_detail in tanks_data:
        tank = TankDetail(**tank_detail)
        session.add(tank)
    
    session.commit()
    print("tank_detail数据导入成功！")


def load_data_from_pipeline_info_json(session, json_file_path: str = "data.json"):
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    # 添加 pipeline_info 数据
    pipelines_data = data
    for pipeline_info in pipelines_data:
        pipeline = Pipeline_info(**pipeline_info)
        session.add(pipeline)
    
    session.commit()
    print("pipeline_info数据导入成功！")



def load_data_from_pipeline_detail_json(session, json_file_path: str = "data.json"):
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    # 添加 pipeline_detail 数据
    pipelines_data = data
    for pipeline_detail in pipelines_data:
        pipeline = Pipeline_detail(**pipeline_detail)
        session.add(pipeline)
    
    session.commit()
    print("pipeline_detail数据导入成功！")


def load_data_from_oil_json(session, json_file_path: str = "data.json"):
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    # 添加 oil 数据
    oils_data = data
    for oil_data in oils_data:
        oil = Oil(**oil_data)
        session.add(oil)
    
    session.commit()
    print("oils数据导入成功！")
   

def load_data_from_order_json(session, json_file_path: str = "data.json"):
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    # 添加 order 数据
    orders_data = data
    for order_data in orders_data:
        order = Order(**order_data)
        session.add(order)
    
    session.commit()
    print("orders数据导入成功！")


def load_data_from_customer_json(session, json_file_path: str = "data.json"):
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    # 添加 customer 数据
    customers_data = data
    for customer_data in customers_data:
        customer = Customer(**customer_data)
        session.add(customer)
    
    session.commit()
    print("customers数据导入成功！")



if __name__ == "__main__":
    SessionLocal = init_db()
    session = SessionLocal()

    tank_info_path = '/home/lijiehui/project/pipeline/Pipeline/data/json/tank_info.json'
    tank_detail_path = '/home/lijiehui/project/pipeline/Pipeline/data/json/tank_detail.json'
    pipeline_info_path = '/home/lijiehui/project/pipeline/Pipeline/data/json/pipeline_info.json'
    pipeline_detail_path = '/home/lijiehui/project/pipeline/Pipeline/data/json/pipeline_detail.json'
    customer_path = '/home/lijiehui/project/pipeline/Pipeline/data/json/customer.json'
    oil_path = '/home/lijiehui/project/pipeline/Pipeline/data/json/oil.json'
    order_path = '/home/lijiehui/project/pipeline/Pipeline/data/json/order.json'

    load_data_from_tank_info_json(session, tank_info_path)
    load_data_from_tank_detail_json(session, tank_detail_path)
    load_data_from_pipeline_info_json(session, pipeline_info_path)
    load_data_from_pipeline_detail_json(session, pipeline_detail_path)
    load_data_from_oil_json(session, oil_path)
    load_data_from_order_json(session, order_path)
    load_data_from_customer_json(session, customer_path) 

