import json

# Path to your JSON file
json_file_path = '/Users/tushar19/Downloads/SC3020Project2jsonfile.json'

# Reading the contents of the JSON file
with open(json_file_path, 'r') as file:
    json_reply = file.read()

# Parsing the JSON
data = json.loads(json_reply)

# Function to print buffer information
def print_buffer_info(plan):
    buffer_info = {
        "Shared Hit Blocks": plan.get('Shared Hit Blocks', 0),
        "Shared Read Blocks": plan.get('Shared Read Blocks', 0),
        "Shared Dirtied Blocks": plan.get('Shared Dirtied Blocks', 0),
        "Shared Written Blocks": plan.get('Shared Written Blocks', 0),
    }

    print("\nBuffer Information Extraction:\n")
    for key, value in buffer_info.items():
        print(f"{key}: {value}")

# Function to traverse the nested structure and extract information
def process_plan(plan, path=[]):
    if 'Plans' in plan:
        # Recursively process each child plan
        child_plans = plan['Plans']
        for i, child_plan in enumerate(child_plans):
            process_plan(child_plan, path + [i + 1])

    node_type = plan.get('Node Type', '')
    actual_total_time = round(plan.get('Actual Total Time', 0), 3)
    estimated_total_time = round(plan.get('Total Cost', 0), 3)

    current_path = '.'.join(map(str, path))
    print(f"\nNode Type: {node_type} ({current_path}), Estimated Total Time: {estimated_total_time},  Actual Total Time: {actual_total_time} ")

    # Difference in estimated total time and actual total time as well as and printing the respective output
    diff = round(estimated_total_time - actual_total_time, 3)
    print(f"Difference for {node_type} ({current_path}): {diff}")

# Traverse the top-level plans
top_level_plans = data['Plan']['Plans']
for i, top_level_plan in enumerate(top_level_plans):
    process_plan(top_level_plan, [i + 1])

# Print buffer information
print_buffer_info(data['Plan'])

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
