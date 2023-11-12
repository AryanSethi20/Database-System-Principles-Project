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

# Function to traverse the nested structure and extract information
def process_plan(plan):
    global mostexp, leastexp, totaldiff
    
    if 'Plans' in plan:
        # Recursively process each child plan
        child_plans = plan['Plans']
        for child_plan in child_plans:
            process_plan(child_plan)
    
    node_type = plan.get('Node Type', '')
    actual_total_time = round(plan.get('Actual Total Time', 0), 3)
    estimated_total_time = round(plan.get('Total Cost', 0), 3)
    
    print(f"\nNode Type: {node_type}, Estimated Total Time: {estimated_total_time},  Actual Total Time: {actual_total_time} ")
    
    # The most expensive step
    if mostexp is None or actual_total_time > mostexp[1]:
        mostexp = (node_type, actual_total_time)
    
    # The least expensive step
    if leastexp is None or actual_total_time < leastexp[1]:
        leastexp = (node_type, actual_total_time)
    
    # Difference in estimated total time and actual total time as well as and printing the respective output
    diff = round(estimated_total_time - actual_total_time, 3)
    print(f"Difference for {node_type}: {diff}")
    
    # Calculating the total difference
    totaldiff += diff

# Traverse the top-level plans
top_level_plans = data['Plan']['Plans']
for top_level_plan in top_level_plans:
    process_plan(top_level_plan)

# Print the results
print("\nMost Expensive Step:")
print(f"Node Type: {mostexp[0]}, Actual Total Time: {mostexp[1]}")

print("\nLeast Expensive Step:")
print(f"Node Type: {leastexp[0]}, Actual Total Time: {leastexp[1]}")

# Print the total difference
print(f"\nTotal Difference between Estimated and Actual Time [Estimated - Actual]: {round(totaldiff, 3)}")
