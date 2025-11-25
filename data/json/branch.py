import json
import os
script_dir = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(script_dir,'site.json'), encoding='utf-8') as file:
    data = json.load(file)

name_to_id = {}

with open(os.path.join(script_dir,'branch_old.json'), encoding='utf-8') as file:
    branch = json.load(file)

for item in data:
    name_to_id[item['site_name']] = item['site_id']
    item['is_direct_connect'] = False
    item['direct_site_id'] = '-8888'

results = []
for item in branch:

    item['site_name'] = item['station_name']
    item['site_id'] = name_to_id[item['station_name']]
    del item['station_name']
    results.append(item)

with open(os.path.join(script_dir,'branch.json'), 'w', encoding='utf-8') as f:
    json.dump(results, f, ensure_ascii=False, indent=4)
