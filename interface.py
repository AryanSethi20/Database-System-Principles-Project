import tkinter as tk

root = tk.Tk()
root.title("3x2 Grid of Frames")
root.geometry("800x600")

# Create a 3x2 grid of frames using the pack geometry manager

# First Row
frame1 = tk.Frame(root, bg="lightblue", width=200, height=100)
frame1.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

frame2 = tk.Frame(root, bg="lightgreen", width=200, height=100)
frame2.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

frame3 = tk.Frame(root, bg="lightcoral", width=200, height=100)
frame3.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

# Second Row
frame4 = tk.Frame(root, bg="lightpink", width=200, height=100)
frame4.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

frame5 = tk.Frame(root, bg="lightyellow", width=200, height=100)
frame5.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

frame6 = tk.Frame(root, bg="lightcyan", width=200, height=100)
frame6.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

root.mainloop()
