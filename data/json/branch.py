import json

with open('/home/lijiehui/project/pipeline/Pipeline/data/json/site.json', encoding='utf-8') as file:
    data = json.load(file)

name_to_id = {}

with open('/home/lijiehui/project/pipeline/Pipeline/data/json/branch_old.json', encoding='utf-8') as file:
    branch = json.load(file)

for item in data:
    name_to_id[item['site_name']] = item['site_id']

results = []
for item in branch:

    item['site_name'] = item['station_name']
    item['site_id'] = name_to_id[item['station_name']]
    del item['station_name']
    results.append(item)

with open('/home/lijiehui/project/pipeline/Pipeline/data/json/branch.json', 'w', encoding='utf-8') as f:
    json.dump(results, f, ensure_ascii=False, indent=4)