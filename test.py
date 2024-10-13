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

load_dotenv()
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
Client = os.getenv("Client")

threshold_low_volume = 50
high_deviation_threshold = 15


part1 = 26361
part2 = 1
part3 = 12

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
    Stock_period = min(Stock_period, len(final_array_sold))  # Use the smaller of Giacenza or the length of the array
    soldS = sum(final_array_sold[:Stock_period])
    boughtS = sum(final_array_bought[:Stock_period])
    resultS = boughtS - soldS
    if (resultS >= 1):
        return resultS
    elif (resultS < 1):
        if (Stock_period < len(final_array_sold)):
            return current_stock(Stock_period + 1)
        elif (Stock_period == len(final_array_sold)):
            return 0

def custom_round(value):
    # Get the integer part and the decimal part
    integer_part = int(value)
    decimal_part = value - integer_part
    
    

    rounding_threshold = 0.3 # TODO Test of test

    # Apply the adjusted rounding logic
    if decimal_part <= rounding_threshold:
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
    

# Print the results  TODO Can be eresed
print("Sold Quantities:", final_array_sold)
print("Bought Quantities:", final_array_bought)

# Get the Variables form the Env
Sales_period = os.getenv("Periodo")
Sales_period = int(Sales_period)
Stock_period = os.getenv("Giacenza")
Stock_period = int(Stock_period)
Copertura = os.getenv("Copertura")
Copertura = float(Copertura)
current_day = datetime.now().day

# Calculate Giacenza
resultS = current_stock(Stock_period)
print("Giacenza = ", resultS)


# Calculate avg. daily sales
# if (current_day > 15):
    # Sales_period -= 1
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
if (daily_sales*Copertura) > resultS:

    order = daily_sales * Copertura
    if (resultS > 0):
        order -= resultS
    

    order = custom_round(order / part3)

    if  order == 0:
        next_article()
        
    combined_string = '.'.join(map(str, [part1, part2, order]))
    
    print("ORDER THIS: " + combined_string + "!")
else:
    print("Will NOT order this: " + str(part1) + "." + str(part2) + "." + str(part3) + "!")



  
'''  elif (resultS < 0):
                    resultS = resultS * -1 # Absolute value of the negative stock qty
                    resultS = min(resultS, part3/2) # Cap for negative stock equal to the size of package
                    order += resultS'''



# This is to be re-implemented!
'''   if restock >= part3:
                    restock = custom_round(restock / part3)
                else:
                    if restock > 3/part3:   
                        restock = 1
                    else:
                        restock = 0 '''

'''# Adjust thresholds based on sales volume and consistency
    if yearSales < threshold_low_volume:  # Sporadic sales
        rounding_threshold = 0.2  # More lenient
    elif deviation > high_deviation_threshold:  # High deviation
        rounding_threshold = 0.4  # For highly seasonal products
    else:  # For high-volume products
        rounding_threshold = 0.6  # Stricter rounding '''