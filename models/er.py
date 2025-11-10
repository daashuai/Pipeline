from graphviz import Digraph

er = Digraph('PipelineBatchER', format='png')
er.attr(rankdir='LR', size='10,8')

er.node('Tank', '''Tank
----------------------
tank_id (PK)
owner
oil_type
inventory_m3
max_capacity_m3
min_safe_level_m3
compatible_oils
available_from
status''', shape='box')

er.node('Order', '''Order
----------------------
order_id (PK)
customer
oil_type
required_volume_m3
earliest_start
deadline
priority
allow_multi_tank
preferred_branches
status''', shape='box')

er.node('Branch', '''Branch
----------------------
branch_id (PK)
max_rate_m3h
stop_windows
status''', shape='box')

er.node('Trunk', '''Trunk
----------------------
trunk_id (PK)
max_rate_m3h''', shape='box')

er.node('Plan', '''Plan
----------------------
plan_id (PK)
order_id (FK)
branch_id (FK)
start_time
end_time
rate_m3h
status''', shape='box')

er.edge('Order', 'Plan', label='1:N')
er.edge('Branch', 'Plan', label='1:N')
er.edge('Trunk', 'Branch', label='1:N')
er.edge('Tank', 'Order', label='Potential Source')

er.render('管输调度系统_ER图', cleanup=True)

