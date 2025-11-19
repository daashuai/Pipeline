import json
from datetime import datetime

with open('/home/lijiehui/project/pipeline/Pipeline/data/json/customer_order.json', encoding='utf-8') as file:
    data = json.load(file)

time_fields = [
        'start_time',
        'end_time', 
        'finish_storage_tank_time',
        'branch_start_time',
        'branch_end_time'
    ]

results = []

for item in data:
    for i in time_fields:
        if item[i] is not None:
            item[i] = datetime.strptime(item[i], "%Y-%m-%d %H:%M:%S")
    results.append(item)

with open('/home/lijiehui/project/pipeline/Pipeline/data/json/customer_order_new.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=4)