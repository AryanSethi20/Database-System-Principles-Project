import tkinter as tk
import customtkinter as ctk
from explore import *
from chlorophyll import CodeView
import pygments.lexers
import psycopg2

ctk.set_appearance_mode('light')

def createLoginWindow():
    global login_window
    global port_entry
    global host_entry
    global database_entry
    global user_entry
    global password_entry
    global error_label

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
        print("trying to make connection")
        conn = psycopg2.connect(
            port=port,
            host=host,
            database=database,
            user=user,
            password=password
        )
        print(conn)
        conn.close()
        login_window.destroy()
        createQueryWindow()

    except psycopg2.Error as e:
        error_label.configure(text=f"Error: {e}", fg="red")

def createQueryWindow():
    global window
    global user_query
    global qep_panel_text
    global analyze_panel_text
    global error_label

    window = ctk.CTk()
    window.geometry("720x720")
    #window.state('zoomed')
    window.title("PostgreSQL Database")
    window.configure(bg_color = 'white')

    querypanel = ctk.CTkFrame(window)
    querypanel_label = ctk.CTkLabel(querypanel, text="Enter SQL Query", font=('Arial', 16, 'bold'), text_color='black')
    querypanel_label.pack(pady=5)
    querypanel.pack()
    user_query = CodeView(querypanel, height=9, lexer=pygments.lexers.SqlLexer, color_scheme="ayu-light")
    user_query.pack()
    # user_query = tk.Text(querypanel,height=9, relief='solid', wrap='word', bg = '#D3D3D3', font=('Arial',8))
    # user_query.pack()
    
    div = ctk.CTkFrame(window)
    submitButton = ctk.CTkButton(div, text='Submit', text_color = 'white', fg_color = '#04c256', hover_color = '#024d23', font=('Arial', 12), width = 200, command=submitQuery)
    submitButton.pack(side=ctk.LEFT, padx=5)
    div.pack(pady=5)

    qep_panel = ctk.CTkFrame(window)
    qep_panel_label = ctk.CTkLabel(qep_panel, text="Query Execution Plan In Natural Language", font=('Arial', 16, 'bold'), text_color='black')
    qep_panel_label.pack(pady=5)
    qep_panel.pack()
    qep_panel_text = ctk.CTkTextbox(qep_panel, state='disabled', height=140, width = 600, wrap='word', font=('Arial', 14), fg_color = '#FFFFCC', border_color='black', border_width=1)
    qep_panel_text.pack()
   
    div1 = ctk.CTkFrame(window)
    qeptreebtn = ctk.CTkButton(div1, text="View QEP Visualization", text_color = "white", fg_color = '#24a0ed', hover_color = '#237fb7', font=('Arial', 12), width = 200,command=createQEPTree)
    qeptreebtn.pack(side=ctk.LEFT)
    blockvisbtn = ctk.CTkButton(div1, text="View Block Visualization", text_color="white", fg_color="#24a0ed", hover_color='#237fb7', font=('Arial', 12), width = 200, command=create_block_visualization)
    blockvisbtn.pack(side=ctk.LEFT, padx=5)  
    div1.pack(pady=5)

    analyze_panel = ctk.CTkFrame(window)
    analyze_panel_label = ctk.CTkLabel(analyze_panel, text="Query Analysis", font=('Arial', 16, 'bold'), text_color='black')
    analyze_panel_label.pack(pady=5)
    analyze_panel.pack()
    analyze_panel_text = ctk.CTkTextbox(analyze_panel, state='disabled', height=140, width=600, wrap='word', font=('Arial', 14), fg_color = '#FFFFCC', border_color='black', border_width=1)
    analyze_panel_text.pack()

    div2 = ctk.CTkFrame(window)
    clearbtn = ctk.CTkButton(div2, text="Reset", text_color = "white", fg_color = '#c20411', hover_color = '#5c040a',font=('Arial', 12), width = 200, command=deleteQuery)
    clearbtn.pack(side= ctk.LEFT, padx=5)
    div2.pack(pady=5)
    
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

if __name__ == "__main__":
    createLoginWindow()