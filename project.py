import tkinter as tk
import psycopg2

class HoverButton(tk.Button):
    def __init__(self, master, **kw):
        tk.Button.__init__(self, master=master, **kw)
        self.defaultBackground = self["background"]
        self.bind("<Enter>", self.on_enter)
        self.bind("<Leave>", self.on_leave)

    def on_enter(self, e):
        self['background'] = self['activebackground']

    def on_leave(self, e):
        self['background'] = self.defaultBackground

def create_login_window():
    def login():
        # Retrieve database connection information from the input fields
        host = host_entry.get()
        database = database_entry.get()
        user = user_entry.get()
        password = password_entry.get()

        try:
            # Connect to the PostgreSQL database
            conn = psycopg2.connect(
                host=host,
                database=database,
                user=user,
                password=password
            )
            conn.close()  # Close the connection

            # Close the login window and open the query window
            login_window.destroy()
            create_query_window(host, database, user, password)

        except psycopg2.Error as e:
            # Display an error message if there's a connection issue
            error_label.config(text=f"Error: {e}", fg="red")

    login_window = tk.Tk()
    login_window.title("Login to PostgreSQL")
    login_window.geometry('330x480')
    login_window.config(bg='white')
    
    frame = tk.Frame(login_window, bg='white')

    title_label = tk.Label(frame, text="PostgreSQL", font=('Arial', 30))

    host_label = tk.Label(frame, text="Host", bg='white', font=('Arial', 14))
    host_entry = tk.Entry(frame, bg = '#D3D3D3', relief='solid', font=('Arial', 14))

    database_label = tk.Label(frame, text="Database", bg='white', font=('Arial', 14))
    database_entry = tk.Entry(frame, bg = '#D3D3D3', relief='solid', font=('Arial', 14))

    user_label = tk.Label(frame, text="Username", bg='white', font=('Arial', 14))
    user_entry = tk.Entry(frame, bg = '#D3D3D3', relief='solid', font=('Arial', 14))

    password_label = tk.Label(frame, text="Password", bg='white', font=('Arial', 14))
    password_entry = tk.Entry(frame, show="*",bg = '#D3D3D3', relief='solid', font=('Arial', 14))

    login_button = HoverButton(frame, text="Login", width='25', font=('Arial', 14), fg="white", bg='#24a0ed',
                activebackground='#237fb7', command=login)

    error_label = tk.Label(frame, text="", fg="red")

    title_label.grid(row=0, column=0, columnspan=2, sticky="nsew", pady=30)
    host_label.grid(row=1, column=0, sticky="w")
    host_entry.grid(row=1, column=1, pady=10)
    database_label.grid(row=2, column=0, sticky="w")
    database_entry.grid(row=2, column=1, pady=10)
    user_label.grid(row=3, column=0, sticky="w")
    user_entry.grid(row=3, column=1, pady=10)
    password_label.grid(row=4, column=0, sticky="w")
    password_entry.grid(row=4, column=1, pady=10)
    login_button.grid(row=5, columnspan=2, pady=20)
    error_label.grid(row=6, columnspan=2)

    frame.pack()

    login_window.mainloop()
    

def create_query_window(host, database, user, password):
    def execute_query_and_display_plan():
        # Retrieve the SQL query from the input field
        query = user_query.get("1.0", tk.END)

        try:
            # Connect to the PostgreSQL database
            conn = psycopg2.connect(
                host=host,
                database=database,
                user=user,
                password=password
            )

            # Create a cursor and execute the query
            cursor = conn.cursor()
            cursor.execute("EXPLAIN ANALYZE " + query)
            query_plan = cursor.fetchall()

            # Display the query plan in the output text widget
            query_plan_output.delete("1.0", tk.END)
            for row in query_plan:
                query_plan_output.insert(tk.END, f"{row[0]}\n")

            conn.close()  # Close the connection

        except psycopg2.Error as e:
            # Display an error message if there's a connection issue
            query_plan_output.delete("1.0", tk.END)
            query_plan_output.insert(tk.END, f"Error: {e}")

    def reset_query():
        # Clear the SQL query input field
        user_query.delete("1.0", tk.END)
        query_plan_output.delete("1.0", tk.END)
    
    query_window = tk.Tk()
    query_window.geometry('720x600')
    query_window.title("PostgreSQL Query Plan")
    query_window.config(bg='white')

    # SQL Query Window display settings
    querypanel = tk.PanedWindow(bg='white')
    querypanel_label = tk.Label(querypanel, text="Input SQL Query Here", bg='white', font=("Arial", 12, 'bold'))
    querypanel_label.pack(pady=5)
    querypanel.pack()

    # Textbox for SQL Statement
    user_query = tk.Text(querypanel,height=9, relief='solid', wrap='word', bg = '#D3D3D3', font=('Arial',10))
    user_query.pack()

    # Panel for button
    div = tk.PanedWindow(bg='white')

    # Call preprocessing.py to execute query
    submitButton = HoverButton(div, text="Submit Query", fg="white", bg = '#024d23', activebackground = '#04c256', relief=tk.RAISED, font=("Arial", 12), width=20, command=execute_query_and_display_plan)
    submitButton.pack(side=tk.LEFT, padx=5)

    #Clear user query
    clearbtn = HoverButton(div, text="Clear", relief=tk.RAISED, fg="white", bg = '#5c040a', activebackground = '#c20411',font=("Arial", 12), width=20,
                     command=reset_query)
    clearbtn.pack(side= tk.LEFT, padx=5)

    div.pack(pady=5)
    
    # Query Plan Label display settings
    query_plan = tk.PanedWindow(bg='white')
    query_plan_label = tk.Label(query_plan, text="Query Execution Plan", bg='white', font=("Arial", 12, 'bold'))
    query_plan_label.pack(pady=5)
    query_plan.pack()
    
    # Query Plan Window display settings
    query_plan_output = tk.Text(query_window, height=20, width=80, font=('Arial', 10), bg = '#D3D3D3', relief='solid', wrap='word')
    query_plan_output.pack()

    query_window.mainloop()

# Start with the login window
create_login_window()