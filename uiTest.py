import tkinter as tk
from orderer import Orderer
from gathererNew import Gatherer

def gather_data():
    global storage_list, orders_list
    gatherer = Gatherer()
    gatherer.gather_data()
    storage_list = gatherer.storage_list
    orders_list = gatherer.orders_list
    gatherer.driver.quit()
    log_output("Data gathered successfully!")

def combine_lists():
    orderer = Orderer()
    orderer.login()
    orderer.lists_combiner(storage_list, orders_list)
    orderer.driver.quit()
    log_output("Lists combined successfully!")

def log_output(message):
    log_text.insert(tk.END, f"{message}\n")
    log_text.see(tk.END)

# Create the main application window
root = tk.Tk()
root.title("Orderer UI")

# Add buttons
gather_button = tk.Button(root, text="Gather Data", command=gather_data)
gather_button.pack(pady=10)

combine_button = tk.Button(root, text="Combine Lists", command=combine_lists)
combine_button.pack(pady=10)

# Add a text box to display logs
log_text = tk.Text(root, height=10, width=50)
log_text.pack(pady=10)

# Start the main event loop
root.mainloop()