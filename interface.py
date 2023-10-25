from tkinter import *
from tkinter import ttk
import pygments.lexers
from chlorophyll import CodeView


# Create a Tkinter root window
root = Tk()
codeview = CodeView(root, lexer=pygments.lexers.SqlLexer, color_scheme="monokai")
codeview.pack(fill="both", expand=True)
root.mainloop()