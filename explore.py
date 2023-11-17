from flask import Flask, request, jsonify
import json
import psycopg2
import networkx as nx
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.backends.backend_tkagg import (FigureCanvasTkAgg,
                                               NavigationToolbar2Tk)
from matplotlib.figure import Figure
from networkx.drawing.nx_pydot import graphviz_layout
import customtkinter as ctk
import tkinter as tk
import math

operatorSeq = []
parents = []
info = []
time = []
annotations = []
analysisList = []
queryplanjson = "queryplan.json"
readinfojson = "readinfo.json"

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
    analysis = 'Time Taken For Query Execution:\n'
    analysis += f"Execution Time = {executionTime} ms\n"
    analysis += f"Planning Time = {planningTime} ms\n"
    analysis += f"Total Cost = {totalCost}\n"
    results = analyze_execution_plan(queryplanjson)
    buffers = extract_shared_blocks(queryplanjson)

    if results['most_expensive'] is not None and results['least_expensive'] is not None:
        # Additional query analysis
        analysis += f"\nThis query has a total of {results['total_plans']} steps: ["
        for node_type, count in results['node_counts'].items():
            analysis += f"{count} {node_type}, "
        analysis += "]"
        analysis += "\nThe most expensive step of this query was "
        analysis += f"{results['most_expensive'][0]} (Path: {results['most_expensive'][2]}) with "
        analysis += f"actual total time of {results['most_expensive'][1]}ms." 

        analysis += "\nThe least expensive step of this query was "
        analysis += f"{results['least_expensive'][0]} (Path: {results['least_expensive'][2]}) with "
        analysis += f"actual total time of {results['least_expensive'][1]}ms."

        # Print the total difference
        analysis += f"\nTotal difference between estimated and actual time taken was {results['total_difference']}ms"

        # Display the results
        analysis = analysis.rstrip(', ')

        # Average actual total time for each node type
        analysis += "\nAverage actual total time: "
        for node_type, average_time in results['average_actual_time'].items():
            analysis += f"{average_time}ms for {node_type}, "
        analysis = analysis.rstrip(', ')

        analysis += f"\n\nTotal Shared Hit Blocks: {buffers['Shared Hit Blocks']}"
        analysis += f"\nTotal Shared Read Blocks: {buffers['Shared Read Blocks']}"
        analysis += f"\nTotal Shared Dirtied Blocks: {buffers['Shared Dirtied Blocks']}"
        analysis += f"\nTotal Shared Written Blocks: {buffers['Shared Written Blocks']}"

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

def extract_shared_blocks(plan):
    with open(f"{plan}") as file:
        data = json.load(file)
        plan = data[0][0][0]["Plan"]

    shared_blocks = {
        "Shared Hit Blocks": 0,
        "Shared Read Blocks": 0,
        "Shared Dirtied Blocks": 0,
        "Shared Written Blocks": 0
    }

    def traverse(node):
        nonlocal shared_blocks
        if "Shared Hit Blocks" in node:
            shared_blocks["Shared Hit Blocks"] += node["Shared Hit Blocks"]
        if "Shared Read Blocks" in node:
            shared_blocks["Shared Read Blocks"] += node["Shared Read Blocks"]
        if "Shared Dirtied Blocks" in node:
            shared_blocks["Shared Dirtied Blocks"] += node["Shared Dirtied Blocks"]
        if "Shared Written Blocks" in node:
            shared_blocks["Shared Written Blocks"] += node["Shared Written Blocks"]

        if "Plans" in node:
            for child_plan in node["Plans"]:
                traverse(child_plan)

    traverse(plan)
    return shared_blocks

def executeQuery(text, port_value, host_value, database_value, user_value, password_value):
    try:
        print("trying to create connection")
        con = psycopg2.connect(
            port=port_value,
            host=host_value,
            database=database_value,
            user=user_value,
            password=password_value
        )
        cursor = con.cursor()
        print(cursor)
        cursor.execute("SELECT pg_stat_reset();")
        cursor.execute("EXPLAIN (BUFFERS ON, ANALYZE ON, COSTS ON, VERBOSE ON, SUMMARY ON, TIMING ON, FORMAT JSON) " + text)
        queryOutput = cursor.fetchall()
        print(queryOutput)
        queryExecuted = True
        cursor.execute("SELECT * FROM pg_statio_user_tables;")
        readOutput = cursor.fetchall()
        print(readOutput)
        cursor.close()
        with open(queryplanjson, 'w') as f:
            json.dump(queryOutput, f, ensure_ascii=True, indent=2)
        with open(readinfojson, 'w') as f:
            json.dump(readOutput, f, ensure_ascii=True, indent=2)
        print("Written to queryplan.json") 
    
    except(Exception, psycopg2.DatabaseError) as error:
        queryExecuted = False

    return queryExecuted

def get_hue(reads):
    intensity = min(255, reads * 5)
    return f"#ff{format(255-intensity, '02x')}{format(255-intensity, '02x')}"

def fetch_data_for_ctid(ctid):
    # Replace this with your actual backend function call
    # For example: return backend_function(ctid)
    return [("1", "value1"), ("2", "value2")]  # Dummy data

def update_grid(ctid, scrollable_frame):
    # This is the function that actually updates the new tuples that are read
    # Clear existing grid
    for widget in scrollable_frame.winfo_children():
        widget.destroy()
    print(f"Fetching data for ctid {ctid}")
    # Fetch new data
    data = fetch_data_for_ctid(ctid)
    print(f"Data Fetched: {data}")
    # Populate the grid with new data
    # TODO - rewrite this to work with variable number of columns for each data set
    for row_index, (tuple_value, value) in enumerate(data):
        ctk.CTkLabel(scrollable_frame, text=tuple_value, corner_radius=0, fg_color="transparent", text_color="white", font=("Arial", 12), anchor="w",justify="left", padx=5, pady=5).grid(row=row_index, column=0)
        ctk.CTkLabel(scrollable_frame, text=value, corner_radius=0, fg_color="transparent", text_color="white", font=("Arial", 12), anchor="w",justify="left", padx=5, pady=5).grid(row=row_index, column=1)
        
def get_pie_chart(table,reads_dict):
    print(table)
    sizes = []
    labels = []
    read_information = {i:reads_dict[table][i] for i in reads_dict[table].keys() if reads_dict[table][i]>0}
    print(read_information)
    for key in read_information:
        sizes.append(read_information[key])
        labels.append(key.replace("_"," ").replace("blks","blocks").title())
    fig = Figure(figsize=(8, 8), dpi=100)
    fig.suptitle(f"Profile of Block Reads For Relation {table.title()}")
    subplot = fig.add_subplot(111)
    subplot.pie(sizes, labels=labels, autopct="%1.0f%%")    
    return fig
    
def get_block_number(table):
    # This is where I get the number of blocks in a relation to populate the dropdown on the right side of the block visualization
    # Needs to return a list for it to be accepted by the tkinter dropdown
    return ["1","2","3","4","5","6","7","8","9","10"]


def create_block_visualization():
    # Parsing readinfo.json to get the reads and hits for each type of block
    with open("readinfo.json", 'r') as file:
        data = json.load(file)
    reads_dict = {}
    for item in data:
        key = item[2]
        values = {
            "heap_blks_read": 0 if item[3] is None else item[3],
            "heap_blks_hit": 0 if item[4] is None else item[4],
            "index_blks_read": 0 if item[5] is None else item[5],
            "index_blks_hit": 0 if item[6] is None else item[6],
            "toast_blks_read":0 if item[7] is None else item[7],
            "toast_blks_hit": 0 if item[8] is None else item[8],
            "tidx_blks_read": 0 if item[9] is None else item[9],
            "tidx_blks_hit": 0 if item[10] is None else item[10],
        }
        reads_dict[key] = values
    table_list = list(reads_dict.keys())
    print(reads_dict)
    print(table_list)
    root = tk.Tk()
    table_var = ctk.StringVar()
    root.title("Block Visualization")
    canvas_frame = ctk.CTkFrame(root, bg_color="white")
    canvas_frame.pack(side=ctk.LEFT, expand=True, fill=ctk.BOTH)
    fig = get_pie_chart("lineitem", reads_dict)
    ctid_list = []

    def on_table_select(choice):
        #When you select the dropdown on the left, this is the logic that updates the plot that is drawn and the dropdown on the right for each block id
        nonlocal block_grid
        nonlocal ctid_list
        nonlocal ctid_dropdown
        fig = get_pie_chart(choice, reads_dict)
        block_grid.figure = fig  
        block_grid.draw()
        ctid_list = get_block_number(choice)
        print(ctid_list)
        ctid_dropdown.configure(values = ctid_list)

    table_dropdown = ctk.CTkComboBox(canvas_frame, variable=table_var, values=table_list, bg_color="white", command=on_table_select)
    block_grid = FigureCanvasTkAgg(fig, master=canvas_frame)
    block_grid.draw()
    table_dropdown.pack()
    block_grid.get_tk_widget().pack()

    info_frame = ctk.CTkFrame(root, bg_color="white",fg_color="white")
    info_frame.pack(side=ctk.RIGHT, fill=ctk.BOTH, expand=True)    
    ctid_var = ctk.StringVar()
    
    def on_ctid_select(choice):
        #This is just a wrapper function because tkinter combo box works better if command argument function has only one argument
        update_grid(choice, scrollable_frame)

    ctid_dropdown = ctk.CTkComboBox(info_frame, variable=ctid_var, values=ctid_list, command=on_ctid_select, bg_color="white")
    ctid_dropdown.pack()

    # Table (Treeview) for displaying tuples
    # TODO - modify this to have a better output format
    scrollable_frame = ctk.CTkScrollableFrame(info_frame,bg_color="white")
    scrollable_frame.pack(expand=True, fill='both')
    
    # Run the Tkinter event loop
    root.config(bg='white')
    root.mainloop()

# TODO - comment this out when submitting final code
create_block_visualization()