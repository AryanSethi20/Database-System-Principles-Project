import json
import psycopg2
import networkx as nx
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from networkx.drawing.nx_pydot import graphviz_layout

operatorSeq = []
parents = []
info = []
time = []
annotations = []
analysisList = []
queryplanjson = "queryplan.json"

def QEPAnnotation():
    deleteQEPAnnotation()
    getQEP(queryplanjson)
    getQEPAnnotation()
    return annotations

def deleteQEPAnnotation():
    operatorSeq.clear()
    parents.clear()
    info.clear()
    time.clear()
    annotations.clear()
    analysisList.clear()

def getQEP(filename):
    with open(f"{filename}") as file:
        data = json.load(file)
        plan = data[0][0][0]["Plan"]
        global executionTime
        global planningTime
        global totalCost
        executionTime = data[0][0][0]["Execution Time"]
        planningTime = data[0][0][0]["Planning Time"]
        totalCost = data[0][0][0]["Plan"]["Total Cost"]  
    iterateOverQEP(plan, -1)
    for i in info:
        if 'Plans' in i:
            i.pop('Plans')

def iterateOverQEP(queryplan, parentNo):
    lastNum = parentNo
    operatorSeq.append(queryplan['Node Type'])
    parents.append(parentNo)
    info.append(queryplan)
    time.append(queryplan["Actual Total Time"])
    lastNum = len(parents) - 1
    if "Plans" in queryplan:
        for plan in queryplan["Plans"]:
            iterateOverQEP(plan, lastNum)
    else:
        return
        
def getQEPAnnotation():
    tables = []
    tableCount = 1
    count = 1
    steps = ''
    i = len(operatorSeq) - 1
    while i >= 0:
        nodeType = operatorSeq[i]
        
        if nodeType == 'Aggregate':
            if info[i]['Strategy'] == "Hashed":
                steps += f'Step {count}: Aggregate performed to hash rows of {tables[-1]} based on key {info[i]["Group Key"]}, and the resulting rows will be returned.\n'
            if info[i]['Strategy'] == "Sorted":
                steps += f'Step {count}: Aggregate performed to sort rows of {tables[-1]} based on key {info[i]["Group Key"]}, and the resulting rows will be returned.\n'
            if info[i]['Strategy'] == "Plain":
                steps += f'Step {count}: Aggregate performed on {tables[-1]} and resulting rows will be returned.\n'

        elif nodeType == 'Append':
            steps += f'Step {count}: Append results from {tables.pop(0)} to {tables.pop(0)} to get intermediate table T{tableCount}.\n'
            tables.append(f'T{tableCount}')
            tableCount += 1

        elif nodeType == 'Bitmap Heap Scan':
            steps += f'Step {count}: Bitmap Heap Scan performed on \'{info[i]["Relation Name"]}\' to decode bitmap'
            if 'Filter' in info[i]:
                steps+= f' with filter {info[i]["Filter"]}'
            tables.append(info[i]["Relation Name"])
            steps += '.\n'

        elif nodeType == 'CTE_Scan':
            steps += f'Step {count}: CTE_Scan performed on relation'
            tables.append(info[i]["Relation Name"])
            # If index cond exist
            if "Index Cond" in info[i]:
                steps += f' with condition(s): {info[i]["Index Cond"]}.\n'

        elif nodeType == 'Gather':
            steps += f'Step {count}: Gather performed on {tables[-1]} to get intermediate table T{tableCount}.\n'
            tables.append(f'T{tableCount}')
            tableCount += 1

        elif nodeType == 'Gather Merge':
            steps += f'Step {count}: Gather Merge performed on {tables[-1]}.\n'

        elif nodeType == 'Hash':
            steps += f'Step {count}: Hashing performed on \'{tables[-1]}\'.\n'

        elif nodeType == 'Hash Join':
            steps += f'Step {count}: Hash {info[i]["Join Type"]} join performed under condition(s) of {info[i]["Hash Cond"]} for {tables.pop(0)} and {tables.pop(0)} to generate intermediate table T{tableCount} as it is ideal to process inputs that are large, unsorted and non-indexed.\n'
            tables.append(f'T{tableCount}')
            tableCount += 1

        elif nodeType == 'Index Only Scan':
            steps += f'Step {count}: Index Only scan performed on \'{info[i]["Index Name"]}\'as data could be access from indexes directly'
            tables.append(info[i]["Relation Name"])
            # If index cond exist
            if "Index Cond" in info[i]:
                steps += f' under condition(s): {info[i]["Index Cond"]}'
            if 'Filter' in info[i]:
                steps+= f' with filter{info[i]["Filter"]}'
            steps += '.\n'

        elif nodeType == 'Index Scan':
            steps += f'Step {count}: Index Scan performed on \'{info[i]["Relation Name"]}\' due to high selectivity of'
            if 'Filter' in info[i]:
                steps+= f' {info[i]["Filter"]}'
            tables.append(info[i]["Relation Name"])
            # If index cond exist
            if "Index Cond" in info[i]:
                steps += f' {info[i]["Index Cond"]}'
            steps += '.\n'

        elif nodeType == 'Incremental Sort':
            steps += f'Step {count}: Incremental Sort performed on {tables[-1]} with Sort Key {info[i]["Sort Key"]} \n'

        elif nodeType == 'Limit':
            steps += f'Step {count}: Perform Limit to limit scanning to {info[i]["Plan Rows"]} rows.\n'

        elif nodeType == 'Materialize':
            steps += f'Step {count}: Materialization performed on {tables[-1]}.\n'

        elif nodeType == 'Memoize':
            steps += f'Step {count}: Perform Memoize of {tables[-1]} with respect to Cache key: {info[i]["Cache Key"]}.\n'

        elif nodeType == 'Merge Append':
            steps += f'Step {count}: Results from table {tables.pop(0)} are appended to {tables.pop(0)} to generate intermediate table T{tableCount}.\n'
            tables.append(f'T{tableCount}')
            tableCount += 1

        elif nodeType == 'Merge Join':
            steps += f'Step {count}: Merge Join performed for {tables.pop(0)} and {tables.pop(0)} under condition(s) of {info[i]["Merge Cond"]} to generate intermediate table T{tableCount}. This is done because join inputs are large and are sorted on their join attributes.\n'
            tables.append(f'T{tableCount}')
            tableCount += 1

        elif nodeType == 'Nested Loop':
            steps += f'Step {count}: Nested Loop operation performed for {tables.pop(0)} and {tables.pop(0)} to generate intermediate table T{tableCount} as one join input is small  (e.g., fewer than 10 rows) and other join input is large and indexed on its join attributes.\n'
            tables.append(f'T{tableCount}')
            tableCount += 1

        elif nodeType == 'Seq Scan':
            steps += f'Step {count}: Sequential Scan performed on \'{info[i]["Relation Name"]}\''
            if 'Filter' in info[i]:
                steps+= f' due to low selectivity of {info[i]["Filter"]}'
            steps += '.\n'
            tables.append(info[i]["Relation Name"])

        elif nodeType == 'SetOp':
            steps += f'Step {count}: Set operation performed on {tables[-1]}\n'

        elif nodeType == 'Sort':
            steps += f'Step {count}: Sort performed on rows in {tables[-1]} based on {info[i]["Sort Key"]})\n'
            if info[i]["Sort Key"][0].find("INC") != -1:
                steps += f'in ascending order.\n'
            if info[i]["Sort Key"][0].find("DESC") != -1:
                steps += f'in descending order.\n'

        else:
            steps += f'Undefined node found!.\n'

        count += 1
        i -= 1

    annotations.append(steps)
    return steps

def QEPAnalysis():
    analysis = ''
    analysis += f"Execution Time = {executionTime} ms, "
    analysis += f"Planning Time = {planningTime} ms, "
    analysis += f"Total Cost = {totalCost}"
    results = analyze_execution_plan(queryplanjson)

    if results['most_expensive'] is not None and results['least_expensive'] is not None:
        # Display the results
        analysis += "\nMost expensive step is "
        analysis += f"{results['most_expensive'][0]} (Path: {results['most_expensive'][2]}) with "
        analysis += f"actual total time of {results['most_expensive'][1]}ms." 

        analysis += "\nLeast expensive step is "
        analysis += f"{results['least_expensive'][0]} (Path: {results['least_expensive'][2]}) with "
        analysis += f"actual total time of {results['least_expensive'][1]}ms."

        # Print the total difference
        analysis += f"\nTotal difference between estimated and actual time is {results['total_difference']}ms"

        # Additional query analysis
        analysis += f"\nTotal number of plans: {results['total_plans']} ["
        for node_type, count in results['node_counts'].items():
            analysis += f"{count} {node_type}, "
        analysis = analysis.rstrip(', ')
        analysis += "]"

        # Average actual total time for each node type
        analysis += "\nAverage actual total time: "
        for node_type, average_time in results['average_actual_time'].items():
            analysis += f"{average_time}ms for {node_type}, "
        analysis = analysis.rstrip(', ')

    #analysisList.append(analysis)    
    return analysis

def analyze_execution_plan(json_file_path):
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
        nonlocal mostexp, leastexp, totaldiff, total_plans, node_counts, total_actual_time

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

        # Difference in estimated total time and actual total time
        diff = round(estimated_total_time - actual_total_time, 3)

        # Calculating the total difference
        totaldiff += diff

    # Traverse the top-level plans
    if "Plans" in data[0][0][0]['Plan']:
        top_level_plans = data[0][0][0]['Plan']['Plans']
        for i, top_level_plan in enumerate(top_level_plans):
            process_plan(top_level_plan, [i + 1])

    # Return the results
    results = {
        'most_expensive': mostexp,
        'least_expensive': leastexp,
        'total_difference': round(totaldiff, 3),
        'total_plans': total_plans,
        'node_counts': node_counts,
        'average_actual_time': {node_type: round(total_time / node_counts[node_type], 3) for node_type, total_time in total_actual_time.items()}
    }

    return results

def executeQuery(text, port_value, host_value, database_value, user_value, password_value):
    try:
        con = psycopg2.connect(
            port=port_value,
            host=host_value,
            database=database_value,
            user=user_value,
            password=password_value
        )
        cursor = con.cursor()
        cursor.execute("EXPLAIN (ANALYZE, BUFFERS, VERBOSE, COSTS, FORMAT JSON) " + text)
        queryOutput = cursor.fetchall()
        queryExecuted = True
        cursor.close()
        with open('queryplan.json', 'w') as f:
            json.dump(queryOutput, f, ensure_ascii=False, indent=2)
    
    except(Exception, psycopg2.DatabaseError) as error:
        queryExecuted = False

    return queryExecuted

def createQEPTree():
    operatorSeq = []
    parents = []
    
    def getQEPforVisualization(filename):
        with open(f"{filename}") as file:
            data = json.load(file)
            plan = data[0][0][0]["Plan"]
        iterateOverQEP(plan, -1)

    def iterateOverQEP(queryplan, parentNo):
        lastNum = parentNo
        operatorSeq.append(queryplan['Node Type'])
        parents.append(parentNo)
        info.append(queryplan)
        lastNum = len(parents) - 1
        if "Plans" in queryplan:
            for plan in queryplan["Plans"]:
                iterateOverQEP(plan, lastNum)
        else:
            return
        
    def create_top_down_tree(node, parent):
        graph = nx.DiGraph()

        for i, node_type in enumerate(node):
            graph.add_node(i, label=node_type)

        for i, p in enumerate(parent):
            if p != -1:
                graph.add_edge(p, i)

        return graph

    def visualize_tree(graph):
        # pos = nx.spring_layout(graph, seed=42)  # Use spring_layout as an alternative
        # pos = nx.nx_pydot.pydot_layout(graph, prog="dot")  # Use spring_layout as an alternative
        root_node = [node for node, in_degree in graph.in_degree() if in_degree == 0]
        pos = graphviz_layout(graph, prog='dot', root=root_node)
        graph = graph.reverse(copy=True)
        #FIXME: this works but it's a bit bugged still
        # For each node, draw a larger box to fit the text
        max_node_size = 0
        for node, (x, y) in pos.items():
            label = graph.nodes[node]['label']
            size = len(label) * 3 
            if size > max_node_size:
                max_node_size = size 
        print(f"max node size: {max_node_size}")
        nx.draw_networkx_edges(graph, pos, node_size=max_node_size)
        for node, (x, y) in pos.items():
            label = graph.nodes[node]['label']
            #TODO: write a function for this code if you have time later
            node_color = "grey"
            if 'Nested Loop Join' in label:
                node_color = "#FFDD32"
            if "Hash Join" in label:
                node_color = "#FFDD32"
            if 'Sequential Scan' in label:
                node_color = "#78C679"
            if 'Index Scan' in label:
                node_color = "#41AB5D"
            if "Hash Buckets" in label:
                node_color = "#6BAED6"
            if label == "Memoize":
                node_color = "#3182BD"
            if 'Sort' in label:
                node_color = "#9E9AC8"
            if 'Aggregate' in label:
                node_color = "#FC9272"
            if 'Gather Merge' in label:
                node_color = "#DE2D26"
            if label == "Gather":
                node_color = "#DE2D26"
            size = len(label) * 0.3 + 50# Estimate the size needed
            nx.draw_networkx_nodes(graph, pos, [node], node_size=size, node_color='none')
            # Draw the label manually to ensure it's placed correctly
            plt.text(x, y, label, fontsize=8, ha='center', va='center', alpha=0.75, 
                    bbox=dict(boxstyle="round,pad=0.3", facecolor=node_color, edgecolor='black'))
        plt.axis('off')
        plt.title("Query Execution Plan")  # Turn off the axis
        plt.show()
    
    getQEPforVisualization(queryplanjson)
    tree = create_top_down_tree(operatorSeq, parents)
    visualize_tree(tree)
