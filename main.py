from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import time
import math
from datetime import datetime
from dotenv import load_dotenv
import os
import pandas as pd
from selenium.common.exceptions import UnexpectedAlertPresentException


def clean_and_convert(values):  # Clean the numbers (remove commas and convert to int) 
    cleaned_values = []
    for value in values:
        # If the value contains a decimal different from ',00', skip the entire row (outer loop iteration)
        if ',' in value and not value.endswith(',00'):
            return None  # This signals that the article must be skipped
            
        # Clean and convert
        cleaned_value = int(value.replace(',00', '').replace('.', ''))  # Remove commas, convert to int
        cleaned_values.append(cleaned_value)
        
    return cleaned_values

def current_stock(Stock_period):
    if (Stock_period <= len(final_array_sold)):        
        soldS = sum(final_array_sold[:Stock_period])
        boughtS = sum(final_array_bought[:Stock_period])
        resultS = boughtS - soldS
        if (resultS >= 1):
            stock_list.append(resultS)
            return current_stock(Stock_period + 1)
        else:       
            return current_stock(Stock_period + 1)
    else:
        if (len(stock_list) > 0):
            average_value = sum(stock_list) / len(stock_list)
            rounded_up_average = math.ceil(average_value)
            stock_list.clear()
            return rounded_up_average
        else:
            return 0

def custom_round(value):
    # Get the integer part and the decimal part
    integer_part = int(value)
    decimal_part = value - integer_part
    
    # Apply the rounding logic
    if decimal_part <= 0.3:  # TODO Could be made user editable
        return integer_part  # Round down
    else:
        return integer_part + 1  # Round up    
    
def next_article():
    print("Will NOT order this: " + str(part1) + "." + str(part2) + "." + str(part3) + "!")  
    driver.back()
    time.sleep(0.3)

orders_list = [] # it will contain all the lists of orders
storage_list = [] # it will contain the name of all the storages gathered from the filename of the tables

# Get the current month
current_month = datetime.now().month

#  Statistics
number_of_orders = 0
stock_list = []

# Calculate how many months until November
if current_month < 12:
    months_to_discard = 12 - current_month
else:
    months_to_discard = 0

# Path to the folder containing all the spreadsheets
folder_path = './Database/'

# Columns you are interested in
column1_name = 'Cod.'
column2_name = 'Dif.'
column3_name = 'Pz x collo'

# Load credentials from .env file
load_dotenv()
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")

# Set up the Selenium WebDriver (Ensure to have the correct browser driver installed)
driver = webdriver.Chrome()

actions = ActionChains(driver)

# Load the webpage
driver.get('https://www.pac2000a.it/PacApplicationUserPanel/faces/home.jsf')

# Wait for the page to fully load
time.sleep(2)

# Log in by entering the username and password, then clicking the login button
username_field = driver.find_element(By.ID, "username")
password_field = driver.find_element(By.ID, "Password")
login_button = driver.find_element(By.CLASS_NAME, "btn-primary")

username_field.send_keys(USERNAME)
password_field.send_keys(PASSWORD)
login_button.click()

# Wait for the page to load after login
time.sleep(3)

# Locate the "eMarket" link by its text
emarket_link = driver.find_element(By.XPATH, '//a[contains(text(), "eMarket")]')
emarket_link.click()

time.sleep(1)

stat_link = driver.find_element(By.XPATH, '//a[@title="Statistiche Articolo"]')
stat_link.click()

time.sleep(3)  # Adjust as necessary

# Loop through all files in the folder
for file_name in os.listdir(folder_path):
    if  file_name.endswith('.ods'):  # Adjust for your spreadsheet extension

        storage_name = os.path.splitext(file_name)[0] # Filename without extension
        storage_list.append(storage_name) 
        
        file_path = os.path.join(folder_path, file_name) # Full path to the current spreadsheet file
        
        df = pd.read_excel(file_path) # Load the spreadsheet

        current_list = []  # Create a new empty list for the current outer loop iteration
        orders_list.append(current_list)

        # Process each row in the spreadsheet
        print(f"Processing file: {file_name}")
        for index, row in df.iterrows():
            part1 = row[column1_name]  # Cod Article
            part2 = row[column2_name]  # Var Article
            part3 = row[column3_name]  # Package size
            
            if part1 == 32052:
                time.sleep(0.3) #TODO this could implement a black list


            # Now, switch to the iframe that contains the required script
            iframe = driver.find_element(By.ID, "ifStatistiche Articolo")
            driver.switch_to.frame(iframe)

            try:
                # Locate the input fields, clean and fill them
                cod_art_field = driver.find_element(By.NAME, 'cod_art')  
                var_art_field = driver.find_element(By.NAME, 'var_art') 
                cod_art_field.clear()
                var_art_field.clear()
                cod_art_field.send_keys(part1)
                var_art_field.send_keys(part2)
                actions.send_keys(Keys.ENTER)
                actions.perform()
                time.sleep(0.7)

                 # Now that you're inside the iframe, attempt to extract the data
                sold_quantities = driver.execute_script("return window.str_qta_vend;")
                bought_quantities = driver.execute_script("return window.str_qta_acq;")

            except UnexpectedAlertPresentException:
                print("Alert present: Codice Articolo Non Valido. Going back and continuing.")
                continue  # Skip to the next iteration of the loop

            # Wait for the search results to load
            
            # Split into two lists: one for 2024 and one for 2023
            sold_quantities_2024 = sold_quantities[::2]  # Values for 2024 (January, February, etc.)
            sold_quantities_2023 = sold_quantities[1::2]  # Values for 2023 (January, February, etc.)
            bought_quantities_2024 = bought_quantities[::2]  # Values for 2024 (January, February, etc.)
            bought_quantities_2023 = bought_quantities[1::2]  # Values for 2023 (January, February, etc.)

            cleaned_2024_sold = clean_and_convert(sold_quantities_2024)
            cleaned_2023_sold = clean_and_convert(sold_quantities_2023)
            cleaned_2024_bought = clean_and_convert(bought_quantities_2024)
            cleaned_2023_bought = clean_and_convert(bought_quantities_2023)

            # If any of the cleaned lists is None (indicating invalid decimal), skip this article (outer loop iteration)
            if cleaned_2024_sold is None or cleaned_2023_sold is None or cleaned_2024_bought is None or cleaned_2023_bought is None:
                print(f"Skipping article at index {index} due to invalid decimal in data")
                next_article()
                continue  # Skip to the next row in df.iterrows()

            # Reverse the order of both lists
            cleaned_2024_sold.reverse()
            cleaned_2023_sold.reverse()
            cleaned_2024_bought.reverse()
            cleaned_2023_bought.reverse()

            # Combine both lists (2024 values first, then 2023)
            final_array_sold = cleaned_2024_sold + cleaned_2023_sold
            final_array_bought = cleaned_2024_bought + cleaned_2023_bought

            # Remove the first elements based on current month
            i = 0
            while len(final_array_bought) > 1 and i < months_to_discard:
                final_array_sold.pop(0) 
                final_array_bought.pop(0)
                i += 1  

            # Remove the last elements from both lists if the bought-list has a zero as last element
            while len(final_array_bought) > 0 and final_array_bought[-1] == 0:
                final_array_bought.pop() 
                final_array_sold.pop() 

            if len(final_array_bought) <= 1:
                next_article()
                continue 

            # Print the results  TODO Can be eresed
            print("Sold Quantities:", final_array_sold)
            print("Bought Quantities:", final_array_bought)

            # Get the Variables form the Env
            Sales_period = os.getenv("Periodo")
            Sales_period = int(Sales_period)
            Stock_period = os.getenv("Giacenza")
            Stock_period = int(Stock_period)
            Coverage = os.getenv("Copertura")
            Coverage = float(Coverage)
            current_day = datetime.now().day

            # Calculate Giacenza
            Stock_period = min(Stock_period, len(final_array_sold))  # Use the smaller of Giacenza or the length of the array
            Stock = current_stock(Stock_period)
            print("Giacenza = ", Stock)
            
            # Calculate avg. daily sales          
            Sales_period = min(Sales_period, len(final_array_sold))  # Use the smaller of Periodo or the length of the array
            soldD = sum(final_array_sold[:Sales_period])
            cleaned_2023_sold.reverse()
            last_year_current_month = cleaned_2023_sold[current_month-1]
            if (last_year_current_month != 0):
                soldD += last_year_current_month
            else:
                Sales_period -= 1
            current_day -= 1            
            daily_sales = soldD / ((Sales_period*30)+(current_day)) 
            print("Daily Sales = ", daily_sales)
            if (daily_sales == 0): # Skip order of articles that aren't currently being sold
                next_article()
                continue

            # Calculate Yearly Average Sales & Deviation
            if (len(final_array_sold) >= 4):
                recent_months = sum(final_array_sold[1:4]) / 3  # Take the last 3 months
                print("Recent months Average Sales = ", recent_months)
                this_month = final_array_sold[0]
                last_month = final_array_sold[1]
                days_to_recover = 30 - (datetime.now().day -1)
                if (days_to_recover > 0):
                    last_month = (days_to_recover/30)*last_month
                    this_month += last_month
                if recent_months != 0:
                    deviation = ((this_month - recent_months)/recent_months)*100
                    deviation = round(deviation, 2)
                else:
                    deviation = 0
                print("Deviation = ", deviation, "%")

            daily_sales = daily_sales * (1 + deviation / 100)
            
            # Calculate if a new order must be done
            restock = daily_sales*Coverage
            if (restock) > Stock:

                if (Stock > 0):
                   restock -= Stock
                

                restock = custom_round(restock / part3)


                if  restock == 0:
                    next_article()
                    continue
                combined_string = '.'.join(map(str, [part1, part2, restock]))
                current_list.append(combined_string)
                print("ORDER THIS: " + combined_string + "!")
                number_of_orders += restock
            else:
                print("Will NOT order this: " + str(part1) + "." + str(part2) + "." + str(part3) + "!")            
                
            driver.back()
            time.sleep(0.3)

driver.get('https://dropzone.pac2000a.it/')

# Wait for the page to fully load
time.sleep(2)

# Login
username_field = driver.find_element(By.ID, "username")
password_field = driver.find_element(By.ID, "password")
username_field.send_keys(USERNAME)
password_field.send_keys(PASSWORD)
actions.send_keys(Keys.ENTER)
actions.perform()

time.sleep(3)

orders_menu = driver.find_element(By.ID, "carta31")
orders_menu.click()

time.sleep(2)

orders_menu1 = driver.find_element(By.ID, "carta32")
orders_menu1.click()

time.sleep(3)

driver.switch_to.window(driver.window_handles[-1])  # Switch to the new tab

actions.click()
actions.send_keys(Keys.ESCAPE)
actions.perform()

time.sleep(2)


def make_orders(storage: str, order_list: list):

    desired_value = storage

    order_button = driver.find_element(By.ID, "addButtonT")
    order_button.click()

    time.sleep(2)

    dropdown1 = driver.find_element(By.ID, "dropdownlistArrowclienteCombo")
    dropdown1.click()

    time.sleep(1)

    actions.send_keys(Keys.ARROW_DOWN)
    actions.send_keys(Keys.ENTER)
    actions.perform()

    time.sleep(1)

    dropdown1 = driver.find_element(By.ID, "dropdownlistArrowmagazzini")
    dropdown1.click()

    time.sleep(1)

    # Locate the input field using XPath
    input_field = driver.find_element(By.XPATH, '//*[@id="dropdownlistContentmagazzini"]/input')

    while True :
        actions.send_keys(Keys.ARROW_DOWN)
        actions.perform()
        time.sleep(1)
        # Get the value of the 'value' attribute
        input_value = input_field.get_attribute("value") # Arrivato alla fine della dropdown list non torna su, bisogna fixare forse, dipende da come vengono processati i file dalla cartella in cui sono salvate le liste
        if input_value == desired_value:
            actions.send_keys(Keys.ENTER)
            actions.perform()
            break
        

    time.sleep(1)

    # Locate the button using its ID
    confirm_button = driver.find_element(By.ID, "okModifica")

    # Click the button
    confirm_button.click()

    time.sleep(3)

    driver.switch_to.window(driver.window_handles[-1])  # Switch to the new tab

    new_order_button = driver.find_element(By.ID, "addButtonT")
    new_order_button.click()

    time.sleep(1)
    
    for element in order_list:
        # Split the element by the dot character
        parts = element.split('.')
        
        # Assign each part to part1, part2, and part3
        part1, part2, part3 = map(int, parts)  # Convert parts to integers

        # if part1 == 32052:
            # time.sleep(0.3) #TODO testing only DELETE
        # Locate the parent div element by its ID
        parent_div1 = driver.find_element(By.ID, "codArt")
        parent_div2 = driver.find_element(By.ID, "varArt")

        # Find the input element within the parent div
        cod_art_field = parent_div1.find_element(By.TAG_NAME, "input")
        var_art_field = parent_div2.find_element(By.TAG_NAME, "input")

        # Clear the input field and insert the desired number
        cod_art_field.clear()  # If you need to clear any existing value
        var_art_field.clear() 

        cod_art_field.send_keys(part1)        
        var_art_field.send_keys(part2)

        time.sleep(1)

        parent_div3 = driver.find_element(By.ID, "w_Quantita")
        stock_size = parent_div3.find_element(By.TAG_NAME, "input")
        stock_size.clear() 
        stock_size.send_keys(part3)

        time.sleep(1)

        # Locate the button using its ID
        confirm_button_order = driver.find_element(By.ID, "okModificaRiga")

        # Click the button
        confirm_button_order.click()

        time.sleep(1)

    driver.switch_to.window(driver.window_handles[-2])  # Switch to the previous tab

for storage, order_list in zip(storage_list, orders_list):
    make_orders(storage, order_list)

print("This order consists of " + str(number_of_orders) + " packages")
# Close the browser
driver.quit()