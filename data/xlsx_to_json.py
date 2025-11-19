import pandas as pd
import json
from datetime import datetime, timedelta

def excel_to_json_with_correct_dates(excel_file, json_file):
    """
    从Excel直接生成正确日期格式的JSON
    """
    # 读取Excel文件
    df = pd.read_excel(excel_file)
    
    # 时间字段列表
    time_fields = [
        'start_time',
        'end_time', 
        'finish_storage_tank_time',
        'branch_start_time',
        'branch_end_time'
    ]
    
    # 转换Excel序列日期为可读格式
    for field in time_fields:
        if field in df.columns:
            # 转换Excel序列号为datetime
            df[field] = df[field].apply(
                lambda x: (datetime(1899, 12, 30) + timedelta(days=x)).strftime('%Y-%m-%d %H:%M:%S') 
                if pd.notnull(x) else None
            )
    
    # 转换为JSON
    json_data = df.to_json(orient='records', indent=4, force_ascii=False)
    
    # 保存JSON文件
    with open(json_file, 'w', encoding='utf-8') as f:
        f.write(json_data)
    
    print(f"转换完成! JSON文件已保存为: {json_file}")

# 使用示例
excel_to_json_with_correct_dates('/home/lijiehui/project/pipeline/Pipeline/data/xlsx/customer_order.xlsx', '/home/lijiehui/project/pipeline/Pipeline/data/json/customer_order.json')