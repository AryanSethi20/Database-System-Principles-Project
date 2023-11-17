from tkinter import *
from tkinter import ttk
import pygments.lexers
from chlorophyll import CodeView
import customtkinter as ctk
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from explore import *

def on_close():
    window.destroy()
    quit()

def createQueryWindow(port_value, host_value, database_value, user_value, password_value, recall = False):
    global port
    global host
    global database
    global user
    global password
    port = port_value
    host = host_value
    database = database_value
    user = user_value
    password = password_value

    global window
    global user_query
    global qep_panel_text
    global analyze_panel_text
    global error_label

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

    """
    qep_tree = ctk.CTkFrame(window)
    qep_tree_label = ctk.CTkLabel(qep_tree, text="QEP Tree", font=('Arial', 16, 'bold'), text_color='black')
    qep_tree_graph = FigureCanvasTkAgg(fig, master=qep_tree)
    qep_tree_graph.draw()
    qep_tree_graph.get_tk_widget().place(relx=0.15, rely=0.15)
    qep_tree_label.grid(row=0, column=0, sticky="nsew", pady=5)
    qep_tree_graph.grid(row=1, column=0, sticky="nsew", pady=5)
    qep_tree.grid(row=0, column=1, padx=20, pady=10, sticky="ne")
    """

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
    
    if not recall:
        window.mainloop() 

def submitQuery():
    query = user_query.get(1.0, 'end-1c')
    qep_panel_text.configure(state='normal')
    qep_panel_text.delete(1.0, 'end-1c')

    if not query:
        qep_panel_text.insert(tk.END, "Invalid Entry\n")
        qep_panel_text.configure(text_color='red')
        qep_panel_text.configure(state='disabled')

    else:
        resetOutput()
        x = executeQuery(query, port, host, database, user, password)
        if not x:
            qep_panel_text.configure(text_color='red')
            qep_panel_text.insert(tk.END, "Check your SQL query")
            qep_panel_text.configure(state='disabled')
        else:
            annotated_query = QEPAnnotation()
            qep_panel_text.configure(text_color='black')
            qep_panel_text.insert(tk.END,annotated_query[0])
            analyze_query = QEPAnalysis()
            analyze_panel_text.configure(state='normal')
            analyze_panel_text.configure(text_color='black')
            analyze_panel_text.insert(tk.END,analyze_query)
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
            plan = data[0][0][0]["Plan"]
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

    def return_to_main():
        for widget in window.winfo_children():
            widget.destroy()
        createQueryWindow(port, host, database, user, password, recall=True)

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
    getQEPforVisualization(queryplanjson)
    tree = create_top_down_tree(operatorSeq, parents)
    visualize_tree(tree)