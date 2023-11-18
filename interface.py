from tkinter import *
import pygments.lexers
from chlorophyll import CodeView
import customtkinter as ctk
import json
import traceback
import networkx as nx
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import (FigureCanvasTkAgg,
                                               NavigationToolbar2Tk)
from matplotlib.figure import Figure
from networkx.drawing.nx_pydot import graphviz_layout
import customtkinter as ctk
import tkinter as tk
import requests
import logging
import math

operatorSeq = []
parents = []
info = []
time = []
annotations = []
analysisList = []
QUERY_PLAN_JSON = "queryplan.json"
READ_INFO_JSON = "readinfo.json"

ctk.set_appearance_mode('light')

def createLoginWindow():

    login_window = ctk.CTk()
    login_window.title("Login to PostgreSQL")
    login_window.geometry('320x480')
    
    frame = ctk.CTkFrame(login_window, fg_color='white')

    title_label = ctk.CTkLabel(frame, text="PostgreSQL Login", font=('Arial', 30, 'bold'), text_color='black')

    port_label = ctk.CTkLabel(frame, text="Port", font=('Arial', 14), text_color='black')
    port_entry = ctk.CTkEntry(frame, fg_color = '#D3D3D3', font=('Arial', 14), text_color='black',placeholder_text="5432")
    
    host_label = ctk.CTkLabel(frame, text="Host", font=('Arial', 14), text_color='black')
    host_entry = ctk.CTkEntry(frame, fg_color = '#D3D3D3', font=('Arial', 14), text_color='black',placeholder_text="localhost")

    database_label = ctk.CTkLabel(frame, text="Database", font=('Arial', 14), text_color='black')
    database_entry = ctk.CTkEntry(frame, fg_color = '#D3D3D3', font=('Arial', 14), text_color='black', placeholder_text="TPC-H")

    user_label = ctk.CTkLabel(frame, text="Username", font=('Arial', 14), text_color='black')
    user_entry = ctk.CTkEntry(frame, fg_color = '#D3D3D3', font=('Arial', 14), text_color='black', placeholder_text="postgres")

    password_label = ctk.CTkLabel(frame, text="Password", font=('Arial', 14), text_color='black')
    password_entry = ctk.CTkEntry(frame, show="*",fg_color = '#D3D3D3', font=('Arial', 14), text_color='black')

    login_button = ctk.CTkButton(frame, text="Login", width=200, font=('Arial', 14), text_color="white", fg_color='#24a0ed', hover_color='#237fb7', command=lambda: login(port_entry, host_entry, database_entry, user_entry, password_entry, error_label, login_window))
    
    error_label = ctk.CTkLabel(frame, text="", text_color="red")

    title_label.grid(row=0, column=0, columnspan=2, sticky="nsew", pady=30)
    port_label.grid(row=1, column=0, sticky="w")
    port_entry.grid(row=1, column=1, padx= 10, pady=10)   
    host_label.grid(row=2, column=0, sticky="w")
    host_entry.grid(row=2, column=1, padx= 10, pady=10)
    database_label.grid(row=3, column=0, sticky="w")
    database_entry.grid(row=3, column=1, padx= 10, pady=10)
    user_label.grid(row=4, column=0, sticky="w")
    user_entry.grid(row=4, column=1, padx= 10, pady=10)
    password_label.grid(row=5, column=0, sticky="w")
    password_entry.grid(row=5, column=1, padx= 10, pady=10)
    login_button.grid(row=6, columnspan=2, pady=20)
    error_label.grid(row=7, columnspan=2)

    frame.pack()
    login_window.mainloop()

def login(port_entry, host_entry, database_entry, user_entry, password_entry, error_label, login_window):
    global token
    
    port = port_entry.get()
    host = host_entry.get()
    database = database_entry.get()
    user = user_entry.get()
    password = password_entry.get()
    
    data = {"dbname": database, "username": user, "password": password, "host": host, "port": port}
    try:
        response = requests.post("http://127.0.0.1:5000/connection", json=data)
        token = response.json().get('token')

        if response.status_code==200:
            login_window.destroy()
            createQueryWindow()
        else:
            raise Exception(f"{response.json().get('error')}")
    except Exception as e:
        error_label.configure(text=f"Error: {e}", fg="red")

def on_close():
    window.destroy()
    quit()

def return_to_main():
        for widget in window.winfo_children():
            widget.destroy()
        createQueryWindow(recall=True)

def createQueryWindow(recall = False):
    global window
    global user_query
    global qep_panel_text
    global analyze_panel_text
    # global error_label

    if not recall:
        window = ctk.CTk()
        #window.geometry("720x720")
        window.state('zoomed')
        window.title("PostgreSQL Database")
        window.configure(bg_color = 'white')
        window.protocol("WM_DELETE_WINDOW", on_close)


    inner_frame = ctk.CTkFrame(window, fg_color='white')

    querypanel = ctk.CTkFrame(inner_frame)
    querypanel_label = ctk.CTkLabel(querypanel, text="Enter SQL Query", font=('Arial', 16, 'bold'), text_color='black')
    user_query = CodeView(querypanel, height=40, lexer=pygments.lexers.SqlLexer, color_scheme="ayu-light")
    querypanel_label.grid(row=0, column=0, sticky="nsew", pady=5)
    user_query.grid(row=1, column=0, sticky="nsew", pady=5)
    querypanel.grid(rowspan=2, column=0, padx=20, pady=10)

    button_frame = ctk.CTkFrame(inner_frame)
    submitButton = ctk.CTkButton(button_frame, text='Submit', text_color = 'white', fg_color = '#04c256', hover_color = '#024d23', font=('Arial', 12), width = 200, command=submitQuery)
    clearbtn = ctk.CTkButton(button_frame, text="Reset", text_color = "white", fg_color = '#c20411', hover_color = '#5c040a',font=('Arial', 12), width = 200, command=deleteQuery)
    submitButton.grid(row=2, column=0, sticky="nsew", pady=5, padx=5)
    clearbtn.grid(row=2, column=1, sticky="nsew", pady=5, padx=5)
    button_frame.grid(row=2, column=0, padx=20, pady=10)

    analyze_panel = ctk.CTkFrame(inner_frame)
    analyze_panel_label = ctk.CTkLabel(analyze_panel, text="Query Analysis", font=('Arial', 16, 'bold'), text_color='black')
    analyze_panel_text = ctk.CTkTextbox(analyze_panel, state='disabled', height=200, width=500, wrap='word', font=('Arial', 14), fg_color = '#FFFFCC', border_color='black', border_width=1)
    analyze_panel_label.grid(row=0, column=0, sticky="nsew", pady=5)
    analyze_panel_text.grid(row=1, column=0, sticky="nsew", pady=5)
    analyze_panel.grid(row=0, column=1, padx=20, pady=10)
     
    qep_panel = ctk.CTkFrame(inner_frame)
    qep_panel_label = ctk.CTkLabel(qep_panel, text="Query Execution Plan In Natural Language", font=('Arial', 16, 'bold'), text_color='black')
    qep_panel_text = ctk.CTkTextbox(qep_panel, state='disabled', height=200, width=500, wrap='word', font=('Arial', 14), fg_color = '#FFFFCC', border_color='black', border_width=1)
    qep_panel_label.grid(row=0, columnspan=2, sticky="nsew", pady=5)
    qep_panel_text.grid(row=1, columnspan=2, sticky="nsew", pady=5)
    qep_panel.grid(row=1, column=1, padx=20, pady=10)

    button_frame1 = ctk.CTkFrame(inner_frame)
    qeptreebtn = ctk.CTkButton(button_frame1, text="View QEP Visualization", text_color = "white", fg_color = '#24a0ed', hover_color = '#237fb7', font=('Arial', 12), width = 200,command=createQEPTree)
    blockvisbtn = ctk.CTkButton(button_frame1, text="View Block Visualization", text_color="white", fg_color="#24a0ed", hover_color='#237fb7', font=('Arial', 12), width = 200, command=create_block_visualization)
    qeptreebtn.grid(row=2, column=0, sticky="nsew", pady=5, padx=5)
    blockvisbtn.grid(row=2, column=1, sticky="nsew", pady=5, padx=5)
    button_frame1.grid(row=2, column=1, padx=20, pady=10)

    inner_frame.pack(expand=True)
    
    if recall:
        user_query.insert(INSERT, restored_query[0])
        qep_panel_text.configure(state='normal')
        qep_panel_text.insert(tk.END, restored_query[1])
        qep_panel_text.configure(state='disabled')
        analyze_panel_text.configure(state='normal')
        analyze_panel_text.insert(tk.END, restored_query[2])
        analyze_panel_text.configure(state='disabled')

    if not recall:
        window.mainloop() 

def submitQuery():
    global restored_query
    query = user_query.get(1.0, 'end-1c')
    qep_panel_text.configure(state='normal')
    qep_panel_text.delete(1.0, 'end-1c')

    if not query:
        analyze_panel_text.insert(tk.END, "Invalid Entry\n")
        analyze_panel_text.configure(text_color='red')
        analyze_panel_text.configure(state='disabled')
        qep_panel_text.insert(tk.END, "Invalid Entry\n")
        qep_panel_text.configure(text_color='red')
        qep_panel_text.configure(state='disabled')

    else:
        resetOutput()
        # x = executeQuery(query, port, host, database, user, password)
        data = {"query": query}
        try:
            logging.debug(f"{query}")
            request = requests.Request("POST", "http://127.0.0.1:5000/query", json=data)

            default_headers = request.headers
            custom_header = {'Authorization': token}
            
            default_headers.update(custom_header)

            # Prepare the request with the updated headers
            prepared_request = request.prepare()

            # Send the request
            session = requests.Session()
            response = session.send(prepared_request)
            logging.debug(response)
            if response.status_code == 200:
                result = response.json()
                queryOutput = result.get('results')
                logging.debug(f"Query Output: {queryOutput}")
                readOutput = result.get('statistics') + result.get('count')
                logging.debug(f"Stats Output: {readOutput}")

                # Write the results of the request in a file for faster processing
                with open(QUERY_PLAN_JSON, 'w') as f:
                    json.dump(queryOutput, f, ensure_ascii=True, indent=2)
                with open(READ_INFO_JSON, 'w') as f:
                    json.dump(readOutput, f, ensure_ascii=True, indent=2)
                logging.info(f"Query results written to the file")
                
                annotated_query = QEPAnnotation()
                qep_panel_text.configure(text_color='black')
                qep_panel_text.insert(tk.END,annotated_query[0])
                analyze_query = QEPAnalysis()
                analyze_panel_text.configure(state='normal', text_color='black')
                analyze_panel_text.insert(tk.END,analyze_query)

                restored_query = [query, annotated_query[0], analyze_query]
                
            else:
                raise Exception(f"{response.json().get('error')}")

        except Exception as e:
            print(traceback.format_exc())
            logging.error(f"Error: {e}")
            analyze_panel_text.configure(state='normal', text_color='red')
            analyze_panel_text.insert(tk.END, "Cannot generate query analysis due to invalid query input")
            analyze_panel_text.configure(state='disabled')
            qep_panel_text.configure(state='normal', text_color='red')
            qep_panel_text.insert(tk.END, "Cannot generate QEP in natural language due to invalid query input")
            qep_panel_text.configure(state='disabled')
        finally:
            qep_panel_text.configure(state='disabled')


def resetOutput():
    analyze_panel_text.configure(state='normal')
    analyze_panel_text.delete(1.0, 'end-1c')
    analyze_panel_text.configure(state='disabled')

def deleteQuery():
    qep_panel_text.configure(state='normal')
    qep_panel_text.delete(1.0, 'end-1c')
    qep_panel_text.configure(state='disabled')
    user_query.delete(1.0,'end-1c')
    analyze_panel_text.configure(state='normal')
    analyze_panel_text.delete(1.0, 'end-1c')
    analyze_panel_text.configure(state='disabled')
    deleteQEPAnnotation()

def createQEPTree():
    for widget in window.winfo_children():
        widget.destroy()
    
    operatorSeq = []
    parents = []
    
    plotFrame = tk.Frame(window)
    plotFrame.pack(fill=tk.BOTH, expand=True)

    def getQEPforVisualization(filename):
        with open(f"{filename}") as file:
            data = json.load(file)
            plan = data[0]["Plan"]
        iterateOverQEP(plan, -1)

    def iterateOverQEP(queryplan, parentNo):
        lastNum = parentNo
        nodeValue = f"{queryplan['Node Type']}"
        #Handling addition of information to node for different sorts
        if(queryplan['Node Type'] == "Sort"):
            nodeValue = f"{queryplan['Sort Method'].title()} Sort On {queryplan['Sort Space Type'].title()}"
            #FIXME: takes too much space
            nodeValue += "\nSort Key - " + ", ".join([s.replace(":", "-") for s in queryplan['Sort Key']])

        #Handling addition of information to node for different joins
        elif(queryplan['Node Type']=="Hash Join"):
            nodeValue = f"{queryplan['Join Type'].title()} Hash Join"
            #FIXME: takes too much space
            nodeValue += f"\nHash Condition - {queryplan['Hash Cond']}"

        elif(queryplan['Node Type']=="Nested Loop"):
            nodeValue = f"{queryplan['Join Type'].title()} Nested Loop Join"

        #Handling addition of information to node for different scans
        elif(queryplan['Node Type']=="Seq Scan"):
            nodeValue = f"Sequential Scan on {queryplan['Relation Name'].title()}"
            try:
                # replaced_filter = queryplan['Filter'].replace("::","-").replace(":","-")
                replaced_filter = queryplan['Filter']
                nodeValue += f"\nFilter - {replaced_filter}"
            except:
                pass

        elif(queryplan['Node Type']=="Index Scan"):
            nodeValue = f"Index Scan on {queryplan['Relation Name'].title()} with {queryplan['Index Name']}"
            #nodeValue += f"\nIndex Name - {queryplan['Index Name'].title()}"
            # nodeValue += f"\nScan Direction - "
            #FIXME - takes too much space
            nodeValue += f"\nIndex Condition - {queryplan['Index Cond'].title()}"

        #Handling addition of information to node for Aggregate
        elif(queryplan['Node Type']=="Aggregate"):
            nodeValue += f"\nStrategy - {queryplan['Strategy'].title()}"
        
        elif(queryplan['Node Type'] == "Hash"):
            nodeValue += f"\nHash Buckets - {queryplan['Hash Buckets']}"
        
        # nodeValue = nodeValue.replace("::","-")
        nodeValue = nodeValue.replace(":","-")
        print(nodeValue)
        operatorSeq.append(nodeValue)
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
        global fig
        fig, ax = plt.subplots()
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
            size = len(label) * 0.3 + 50
            nx.draw_networkx_nodes(graph, pos, [node], node_size=size, node_color='none')
            # Draw the label manually to ensure it's placed correctly
            plt.text(x, y, label, fontsize=8, ha='center', va='center', alpha=0.75, 
                    bbox=dict(boxstyle="round,pad=0.3", facecolor=node_color, edgecolor='black'))
        plt.axis('off')
        plt.title("Visualized Query Execution Plan") 
        canvas = FigureCanvasTkAgg(fig, master=plotFrame)
        toolbar_frame = ctk.CTkFrame(master=plotFrame)
        toolbar_frame.pack(side=tk.BOTTOM, fill=tk.X)
        toolbar = NavigationToolbar2Tk(canvas, toolbar_frame, pack_toolbar=False)
        toolbar.update()
        canvas_widget = canvas.get_tk_widget()
        canvas_widget.pack(fill=tk.BOTH, expand=True)
        toolbar.pack(side=tk.TOP, fill=tk.X)
        #plt.show()
        button_quit = ctk.CTkButton(master=toolbar_frame, text="Quit", command=return_to_main)
        button_quit.pack(side=tk.BOTTOM, fill=tk.X)
    getQEPforVisualization(QUERY_PLAN_JSON)
    tree = create_top_down_tree(operatorSeq, parents)
    visualize_tree(tree)

def QEPAnnotation():
    deleteQEPAnnotation()
    getQEP(QUERY_PLAN_JSON)
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
        plan = data[0]["Plan"]
        global executionTime
        global planningTime
        global totalCost
        executionTime = data[0]["Execution Time"]
        planningTime = data[0]["Planning Time"]
        totalCost = data[0]["Plan"]["Total Cost"]  
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
                steps += f'Step {count}: Aggregate performed to hash rows of {tables[-1]} based on key {info[i]["Group Key"]}, and resulting rows will be returned.\n'
            if info[i]['Strategy'] == "Sorted":
                steps += f'Step {count}: Aggregate performed to sort rows of {tables[-1]} based on key {info[i]["Group Key"]}, and resulting rows will be returned.\n'
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
    results = analyze_execution_plan(QUERY_PLAN_JSON)
    buffers = extract_shared_blocks(QUERY_PLAN_JSON)
    most_expensive_step = results['total_plans'] - results['most_expensive'][2]
    least_expensive_step = results['total_plans'] - results['least_expensive'][2]

    if results['most_expensive'] is not None and results['least_expensive'] is not None:
        # Additional query analysis
        analysis += f"\nThis query has a total of {results['total_plans']} steps: ["
        for node_type, count in results['node_counts'].items():
            analysis += f"{count} {node_type}, "
        analysis = analysis.rstrip(', ')
        analysis += "]"
        analysis += "\nThe most expensive step of this query was "
        analysis += f"Step {most_expensive_step} : {results['most_expensive'][0]} with "
        analysis += f"actual total time of {results['most_expensive'][1]}ms" 

        analysis += "\nThe least expensive step of this query was "
        analysis += f"Step {least_expensive_step} : {results['least_expensive'][0]} with "
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
    def process_plan(plan, cost):
        nonlocal mostexp, leastexp, totaldiff, total_plans, node_counts, total_actual_time

        if 'Plans' in plan:
            # Recursively process each child plan
            child_plans = plan['Plans']
            for i, child_plan in enumerate(child_plans):
                process_plan(child_plan, cost + i+1)

        total_plans += 1

        node_type = plan.get('Node Type', '')
        actual_total_time = round(plan.get('Actual Total Time', 0), 3)
        estimated_total_time = round(plan.get('Total Cost', 0), 3)
        
        # Update node counts
        node_counts[node_type] = node_counts.get(node_type, 0) + 1

        # Update total actual time for each node type
        total_actual_time[node_type] = total_actual_time.get(node_type, 0) + actual_total_time

        # The most expensive step
        if mostexp is None or actual_total_time > mostexp[1]:
            mostexp = (node_type, actual_total_time, cost)

        # The least expensive step
        if leastexp is None or actual_total_time < leastexp[1]:
            leastexp = (node_type, actual_total_time, cost)

        # Difference in estimated total time and actual total time
        diff = round(estimated_total_time - actual_total_time, 3)

        # Calculating the total difference
        totaldiff += diff

    # Traverse the top-level plans
    process_plan(data[0]['Plan'], 0)

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
        plan = data[0]["Plan"]

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
    # FIXME: Needs to return a list for it to be accepted by the tkinter dropdown
    return ["1","2","3","4","5","6","7","8","9","10"]


def create_block_visualization():
    for widget in window.winfo_children():
        widget.destroy()
    # Parsing readinfo.json to get the reads and hits for each type of block
    with open("readinfo.json", 'r') as file:
        data = json.load(file)
    with open("ctid_results.json", 'r') as ctid_file:
        ctid_data = json.load(ctid_file)
    reads_dict = {}
    for item in data[:-1]:
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
        all_values_zero = all(value == 0 for value in values.values())
        if(all_values_zero):
            pass
        else:
            reads_dict[key] = values
    table_list = list(reads_dict.keys())
    print(reads_dict)
    print(table_list)
    # root = tk.Tk()
    table_var = ctk.StringVar()
    # root.title("Block Visualization")
    dropdown_frame = ctk.CTkFrame(window, bg_color="white")
    dropdown_frame.pack(side=ctk.TOP, expand=True, fill=ctk.BOTH)
    canvas_frame = ctk.CTkFrame(window, bg_color="white", border_color="white")
    canvas_frame.pack(side=ctk.LEFT, expand=True, fill=ctk.BOTH)
    fig = get_pie_chart(table_list[1], reads_dict)
    ctid_list = []

    def on_table_select(choice):
        #When you select the dropdown on the left, this is the logic that updates the plot that is drawn and the dropdown on the right for each block id
        nonlocal pie_chart_grid
        nonlocal ctid_list
        fig = get_pie_chart(choice, reads_dict)
        pie_chart_grid.figure = fig  
        pie_chart_grid.draw()
        ctid_list = get_block_number(choice)
        print(ctid_list)

    table_dropdown = ctk.CTkComboBox(dropdown_frame, variable=table_var, values=table_list, bg_color="white", command=on_table_select)
    dropdown_label = ctk.CTkLabel(dropdown_frame, text="Select a Table : ", font=("Arial", 14))
    quit_button = ctk.CTkButton(dropdown_frame, text="Quit", command=return_to_main)
    dropdown_label.pack(side=ctk.LEFT)
    pie_chart_grid = FigureCanvasTkAgg(fig, master=canvas_frame)
    pie_chart_grid.draw()
    table_dropdown.pack(side=ctk.LEFT)
    quit_button.pack(side=ctk.RIGHT)
    pie_chart_grid.get_tk_widget().pack()

    info_frame = ctk.CTkFrame(window, bg_color="white",fg_color="white")
    info_frame.pack(side=ctk.RIGHT, fill=ctk.BOTH, expand=True)    
    
    block_grid = ctk.CTkCanvas(info_frame, width=600, height=600)
    block_grid_label = ctk.CTkLabel(info_frame, text="Block Reads Heatmap", font=("Arial",16))
    block_grid_label.pack()
    block_grid.pack()

    max_blocks = 36
    num_blocks = len(ctid_data)
    if num_blocks > max_blocks:
        n = math.ceil(num_blocks / max_blocks)
        print(f"n: {n}")
        aggregated_data = {}
        for i in range(0, num_blocks, n):
            block_ids = list(ctid_data.keys())[i:i+n]
            total_reads = sum(ctid_data[ctid] for ctid in block_ids if ctid in ctid_data)
            aggregated_data[f"{block_ids[0]}-{block_ids[-1]}"] = total_reads
        ctid_data = aggregated_data
    print(num_blocks)

    block_size = 100
    grid_size = 6
    for ctid, reads in ctid_data.items():
        index = list(ctid_data.keys()).index(ctid)
        x0 = (index % grid_size) * block_size
        y0 = (index // grid_size) * block_size
        x1 = x0 + block_size
        y1 = y0 + block_size
        color = get_hue(reads)
        rect = block_grid.create_rectangle(x0, y0, x1, y1, fill=color, outline="black")
        text_position = (x0 + block_size / 2, y0 + block_size / 2)  # Center of the block
        if(reads < 15):
            text_color = "black"
        else:
            text_color = "white"
        text = block_grid.create_text(text_position, text=f"CTID\n{ctid}\nReads: {reads}", fill=text_color, font=("Arial", 10)) 


def frontend():
    logging.basicConfig(format='%(levelname)s: Line %(lineno)d - %(message)s', level=logging.INFO)
    createLoginWindow()
    return

# TODO - comment this out when submitting final code
# create_block_visualization()