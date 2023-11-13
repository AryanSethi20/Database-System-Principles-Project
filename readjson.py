import json

# Path to your JSON file
json_file_path = '/Users/tushar19/Downloads/SC3020Project2jsonfile.json'

# Reading the contents of the JSON file
with open(json_file_path, 'r') as file:
    json_reply = file.read()

# Parsing the JSON
data = json.loads(json_reply)

# Initializing attributes for most and least expensive steps
mostexp = None
leastexp = None
totaldiff = 0

# Additional attributes for analysis
total_plans = 0
node_counts = {}
total_actual_time = {}

# Function to traverse the nested structure and extract information
def process_plan(plan, path=[]):
    global mostexp, leastexp, totaldiff, total_plans, node_counts, total_actual_time
    
    if 'Plans' in plan:
        # Recursively process each child plan
        child_plans = plan['Plans']
        for i, child_plan in enumerate(child_plans):
            process_plan(child_plan, path + [i + 1])
    
    total_plans += 1
    
    node_type = plan.get('Node Type', '')
    actual_total_time = round(plan.get('Actual Total Time', 0), 3)
    estimated_total_time = round(plan.get('Total Cost', 0), 3)
    
    current_path = '.'.join(map(str, path))
    print(f"\nNode Type: {node_type} ({current_path}), Estimated Total Time: {estimated_total_time},  Actual Total Time: {actual_total_time} ")
    
    # Update node counts
    node_counts[node_type] = node_counts.get(node_type, 0) + 1
    
    # Update total actual time for each node type
    total_actual_time[node_type] = total_actual_time.get(node_type, 0) + actual_total_time
    
    # The most expensive step
    if mostexp is None or actual_total_time > mostexp[1]:
        mostexp = (node_type, actual_total_time, current_path)
    
    # The least expensive step
    if leastexp is None or actual_total_time < leastexp[1]:
        leastexp = (node_type, actual_total_time, current_path)
    
    # Difference in estimated total time and actual total time as well as and printing the respective output
    diff = round(estimated_total_time - actual_total_time, 3)
    print(f"Difference for {node_type} ({current_path}): {diff}")
    
    # Calculating the total difference
    totaldiff += diff

# Traverse the top-level plans
top_level_plans = data['Plan']['Plans']
for i, top_level_plan in enumerate(top_level_plans):
    process_plan(top_level_plan, [i + 1])

# Most Expensive and Least Expensive Steps
print("\nMost Expensive Steps:")
for node_type, actual_time, current_path in [mostexp]:
    print(f"Node Type: {node_type} ({current_path}), Actual Total Time: {actual_time}")

print("\nLeast Expensive Steps:")
for node_type, actual_time, current_path in [leastexp]:
    print(f"Node Type: {node_type} ({current_path}), Actual Total Time: {actual_time}")

# Print the total difference
print(f"\nTotal Difference between Estimated and Actual Time [Estimated - Actual]: {round(totaldiff, 3)}")
    
# Additional query analysis
print("\nAdditional Query Analysis:\n")
print(f"Total number of plans: {total_plans}")

# Node counts for each node type
print("\nNode Counts:")
for node_type, count in node_counts.items():
    print(f"{node_type}: {count}")

# Average actual total time for each node type
print("\nAverage Actual Total Time:")
for node_type, total_time in total_actual_time.items():
    average_time = total_time / node_counts[node_type]
    print(f"{node_type}: {round(average_time, 3)}")