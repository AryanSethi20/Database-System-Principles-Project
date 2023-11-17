import tkinter as tk
import customtkinter as ctk
from common.utils import *
from chlorophyll import CodeView
import pygments.lexers
import psycopg2
import requests
import logging
from common.consts import *

def createLoginWindow():
    # global login_window
    # global port_entry
    # global host_entry
    # global database_entry
    # global user_entry
    # global password_entry
    # global error_label

    login_window = tk.Tk()
    login_window.title("Login to PostgreSQL")
    login_window.geometry('320x480')
    login_window.config(bg='white')
    
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

        if response.status_code==201:
            login_window.destroy()
            createQueryWindow()
        else:
            raise Exception(f"{response.json().get('error')}")
    except Exception as e:
        error_label.configure(text=e, fg="red")


def createQueryWindow():
    global window
    global user_query
    global qep_panel_text
    global analyze_panel_text
    # global error_label

    window = tk.Tk()
    # window.geometry("720x660")
    window.state('zoomed')
    window.title("PostgreSQL Database")
    window.config(bg = 'white')

    querypanel = tk.PanedWindow(bg='white', height=9)
    querypanel_label = ctk.CTkLabel(querypanel, text="Enter SQL Query", font=('Arial', 16, 'bold'), text_color='black')
    querypanel_label.pack(pady=5)
    querypanel.pack()

    user_query = CodeView(querypanel, height=9, lexer=pygments.lexers.SqlLexer, color_scheme="ayu-light")
    user_query.pack()
    # user_query = tk.Text(querypanel,height=9, relief='solid', wrap='word', bg = '#D3D3D3', font=('Arial',8))
    # user_query.pack()

    div = tk.PanedWindow(bg='white')

    submitButton = ctk.CTkButton(div, text='Submit', text_color = 'white', fg_color = '#04c256', hover_color = '#024d23', font=('Arial', 12), width = 200, command=submitQuery)
    submitButton.pack(side=tk.LEFT, padx=5)
    
    div.pack(pady=5)

    qep_panel = tk.PanedWindow(bg= 'white', height=9)
    qep_panel_label = ctk.CTkLabel(qep_panel, text="Query Execution Plan In Natural Language", font=('Arial', 16, 'bold'), text_color='black')
    qep_panel_label.pack(pady=5)
    qep_panel.pack()

    qep_panel_text = tk.Text(qep_panel, state='disabled', height=9, relief='solid', wrap='word', font=('Arial', 14), bg = '#FFFFCC', width = 80)
    qep_panel_text.pack()

    div1 = tk.PanedWindow(bg='white')

    qeptreebtn = ctk.CTkButton(div1, text="View QEP Visualization", text_color = "white", fg_color = '#24a0ed', hover_color = '#237fb7', font=('Arial', 12), width = 200,command=createQEPTree)
    qeptreebtn.pack(side=tk.LEFT)

    blockvisbtn = ctk.CTkButton(div1, text="View Block Visualization", text_color="white", fg_color="#24a0ed", hover_color='#237fb7', font=('Arial', 12), width = 200, command=create_block_visualization)
    blockvisbtn.pack(side=tk.LEFT, padx=5)
    
    div1.pack(pady=5)

    analyze_panel = tk.PanedWindow(bg= 'white')
    analyze_panel_label = ctk.CTkLabel(analyze_panel, text="Query Analysis", font=('Arial', 16, 'bold'), text_color='black')
    analyze_panel_label.pack(pady=5)
    analyze_panel.pack()

    analyze_panel_text = tk.Text(analyze_panel, state='disabled', height=9, relief='solid', wrap='word', font=('Arial', 14), bg = '#FFFFCC', width = 80)
    analyze_panel_text.pack()

    div2 = tk.PanedWindow(bg='white')

    clearbtn = ctk.CTkButton(div2, text="Reset", text_color = "white", fg_color = '#c20411', hover_color = '#5c040a',font=('Arial', 12), width = 200, command=deleteQuery)
    clearbtn.pack(side= tk.LEFT, padx=5)

    div2.pack(pady=5)
    window.mainloop()

def submitQuery():
    query = user_query.get('1.0', 'end-1c')
    qep_panel_text.configure(state='normal')
    qep_panel_text.delete('1.0', 'end-1c')

    if not query:
        qep_panel_text.insert(tk.END, "Invalid Entry\n")
        qep_panel_text.config(fg='red')
        qep_panel_text.configure(state='disabled')

    else:
        resetOutput()
        # x = executeQuery(query, port, host, database, user, password)
        data = {"query": query}
        try:
            request = requests.Request("POST", "http://127.0.0.1:5000/query", json=data)

            default_headers = request.headers
            custom_header = {'Authorization': token}
            
            default_headers.update(custom_header)

            # Prepare the request with the updated headers
            prepared_request = request.prepare()

            # Send the request
            session = requests.Session()
            response = session.send(prepared_request)
            result = response.json()
            queryOutput = result.get('results')
            # logging.debug(f"Query Output: {queryOutput}")
            readOutput = result.get('statistics')
            # logging.debug(f"Stats Output: {readOutput}")
            # Write the results of the request in a file for faster processing
            with open(QUERY_PLAN_JSON, 'w') as f:
                json.dump(queryOutput, f, ensure_ascii=True, indent=2)
            with open(READ_INFO_JSON, 'w') as f:
                json.dump(readOutput, f, ensure_ascii=True, indent=2)
            logging.info(f"Query results written to the file")

            annotated_query = QEPAnnotation()
            qep_panel_text.config(fg='black')
            qep_panel_text.insert(tk.END,annotated_query[0])
            analyze_query = QEPAnalysis()
            analyze_panel_text.configure(state='normal')
            analyze_panel_text.configure(fg='black')
            analyze_panel_text.insert(tk.END,analyze_query)

        except Exception as e:
            logging.error(f"Error: {e}")
            qep_panel_text.config(fg='red')
            qep_panel_text.insert(tk.END, "Check your SQL query")
            qep_panel_text.configure(state='disabled')
        finally:
            qep_panel_text.configure(state='disabled')

def resetOutput():
    analyze_panel_text.configure(state='normal')
    analyze_panel_text.delete('1.0', 'end-1c')
    analyze_panel_text.configure(state='disabled')

def deleteQuery():
    qep_panel_text.configure(state='normal')
    qep_panel_text.delete('1.0', 'end-1c')
    qep_panel_text.configure(state='disabled')
    user_query.delete('1.0','end-1c')
    analyze_panel_text.configure(state='normal')
    analyze_panel_text.delete('1.0', 'end-1c')
    analyze_panel_text.configure(state='disabled')
    deleteQEPAnnotation()

def frontend():
    logging.basicConfig(level=logging.INFO)
    createLoginWindow()