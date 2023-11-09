import tkinter as tk
import customtkinter as ctk
from explore import *
import psycopg2
import json

def login():
    global port
    global host
    global database
    global user
    global password
    
    port = port_entry.get()
    host = host_entry.get()
    database = database_entry.get()
    user = user_entry.get()
    password = password_entry.get()

    try:
        conn = psycopg2.connect(
            port=port,
            host=host,
            database=database,
            user=user,
            password=password
        )
        conn.close()
        login_window.destroy()
        create_query_window()

    except psycopg2.Error as e:
        error_label.config(text=f"Error: {e}", fg="red")

def create_login_window():
    global login_window
    global port_entry
    global host_entry
    global database_entry
    global user_entry
    global password_entry
    global error_label

    login_window = tk.Tk()
    login_window.title("Login to PostgreSQL")
    login_window.geometry('330x480')
    login_window.config(bg='white')
    
    frame = ctk.CTkFrame(login_window, fg_color='white')

    title_label = ctk.CTkLabel(frame, text="PostgreSQL", font=('Arial', 30, 'bold'), text_color='black')

    port_label = ctk.CTkLabel(frame, text="Port", font=('Arial', 14), text_color='black')
    port_entry = ctk.CTkEntry(frame, fg_color = '#D3D3D3', font=('Arial', 14), text_color='black')
    
    host_label = ctk.CTkLabel(frame, text="Host", font=('Arial', 14), text_color='black')
    host_entry = ctk.CTkEntry(frame, fg_color = '#D3D3D3', font=('Arial', 14), text_color='black')

    database_label = ctk.CTkLabel(frame, text="Database", font=('Arial', 14), text_color='black')
    database_entry = ctk.CTkEntry(frame, fg_color = '#D3D3D3', font=('Arial', 14), text_color='black')

    user_label = ctk.CTkLabel(frame, text="Username", font=('Arial', 14), text_color='black')
    user_entry = ctk.CTkEntry(frame, fg_color = '#D3D3D3', font=('Arial', 14), text_color='black')

    password_label = ctk.CTkLabel(frame, text="Password", font=('Arial', 14), text_color='black')
    password_entry = ctk.CTkEntry(frame, show="*",fg_color = '#D3D3D3', font=('Arial', 14), text_color='black')

    login_button = ctk.CTkButton(frame, text="Login", width=200, font=('Arial', 14), text_color="white", fg_color='#24a0ed', hover_color='#237fb7', command=login)
    
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

def submit_query():
    query = user_query.get('1.0', 'end-1c')
    qep_panel_text.configure(state='normal')
    qep_panel_text.delete('1.0', 'end-1c')

    if not query:
        qep_panel_text.insert(tk.END, "Please submit SQL query\n")
        qep_panel_text.config(fg='red')
        qep_panel_text.configure(state='disabled')

    else:
        x = runQuery(query, port, host, database, user, password)
        if not x:
            qep_panel_text.config(fg='red')
            qep_panel_text.insert(tk.END, "Please check your SQL query")
            qep_panel_text.configure(state='disabled')
        else:
            annotated_query = startAnnotation()
            qep_panel_text.config(fg='black')
            qep_panel_text.insert(tk.END,annotated_query[0])
        qep_panel_text.configure(state='disabled')

def clearQueries():
    qep_panel_text.configure(state='normal')
    qep_panel_text.delete('1.0', 'end-1c')
    qep_panel_text.configure(state='disabled')
    user_query.delete('1.0','end-1c')
    clearAnnotation()

def create_query_window():
    global window
    global user_query
    global qep_panel_text
    global leftPanel_text
    global centrePanel_text
    global rightPanel_text
    global error_label

    window = tk.Tk()
    window.geometry("700x580")
    window.title("Database")
    window.config(bg = 'white')

    querypanel = tk.PanedWindow(bg='white')
    querypanel_label = ctk.CTkLabel(querypanel, text="SQL Query", font=('Arial', 20, 'bold'), text_color='black')
    querypanel_label.pack(pady=5)
    querypanel.pack()

    user_query = tk.Text(querypanel,height=9, relief='solid', wrap='word', bg = '#D3D3D3', font=('Arial',10))
    user_query.pack()

    div = tk.PanedWindow(bg='white')

    submitButton = ctk.CTkButton(div, text='Submit', text_color = 'white', fg_color = '#04c256', hover_color = '#024d23', font=('Arial', 12), width = 200, command=submit_query)
    submitButton.pack(side=tk.LEFT, padx=5)
    
    div.pack(pady=5)

    annotatedPanel = tk.PanedWindow(bg= 'white')
    annotatedPanel_label = ctk.CTkLabel(annotatedPanel, text="Query Execution Plan In Natural Language", font=('Arial', 20, 'bold'), text_color='black')
    annotatedPanel_label.pack(pady=5)
    annotatedPanel.pack()

    qep_panel_text = tk.Text(annotatedPanel, state='disabled', height=14, relief='solid', wrap='word', font=('Arial', 10), bg = '#D3D3D3', width = 80)
    qep_panel_text.pack()

    div1 = tk.PanedWindow(bg='white')

    qeptreebtn = ctk.CTkButton(div1, text="View QEP Tree", text_color = "white", fg_color = '#24a0ed', hover_color = '#237fb7', font=('Arial', 12), width = 200,command=createQEPTreeDiagram)
    qeptreebtn.pack(side=tk.LEFT)

    clearbtn = ctk.CTkButton(div1, text="Reset", text_color = "white", fg_color = '#c20411', hover_color = '#5c040a',font=('Arial', 12), width = 200, command=clearQueries)
    clearbtn.pack(side= tk.LEFT, padx=5)
    
    div1.pack(pady=5)

    window.mainloop()

if __name__ == "__main__":
    create_login_window()