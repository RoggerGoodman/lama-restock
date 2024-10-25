from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import time
import math
from datetime import datetime
import os
import pandas as pd
from selenium.common.exceptions import UnexpectedAlertPresentException
from selenium.webdriver.chrome.options import Options
from consts import COLLUMN1_NAME, COLLUMN2_NAME, COLLUMN3_NAME, SPREADSHEETS_FOLDER
from credentials import PASSWORD, USERNAME
from helpers import custom_round, custom_round2, clean_and_convert
from logger import logger


class Gatherer:

    def __init__(self) -> None:
        # Set up the Selenium WebDriver (Ensure to have the correct browser driver installed)
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # Run Chrome in headless mode
        chrome_options.add_argument("--no-sandbox")  # Required for some environments
        chrome_options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems
        chrome_options.add_argument("--disable-gpu")  # Applicable only if you are running on Windows

        self.driver = webdriver.Chrome(options=chrome_options)


        self.actions = ActionChains(self.driver)
        self.orders_list = []  # it will contain all the lists of orders
        # it will contain the name of all the storages gathered from the filename of the tables
        self.storage_list = []

        # Get the current month
        self.current_month = datetime.now().month

        #  Statistics
        self.number_of_orders = 0
        self.stock_list = []

        # Calculate how many months until November
        if self.current_month < 12:
            self.months_to_discard = 12 - self.current_month
        else:
            self.months_to_discard = 0

        # months_to_discard = 12 - current_month if current_month < 12 else 0



    # Load the webpage
    def login(self):
        try:
            # TODO Get rid of the constant  
            self.driver.get('https://www.pac2000a.it/PacApplicationUserPanel/faces/home.jsf')
        except Exception as exc:
            logger.info('Somwething went wrong, smartie')
        # Wait for the page to fully load
        time.sleep(2)

        # Log in by entering the username and password, then clicking the login button
        username_field = self.driver.find_element(By.ID, "username")
        password_field = self.driver.find_element(By.ID, "Password")
        login_button = self.driver.find_element(By.CLASS_NAME, "btn-primary")

        username_field.send_keys(USERNAME)
        password_field.send_keys(PASSWORD)
        login_button.click()

    def next_article(self, part1, part2, part3):
        logger.info("Will NOT order this: " + str(part1) +
                    "." + str(part2) + "." + str(part3) + "!")
        self.driver.back()
        time.sleep(0.3)

    def calculate_avg_stock(self, stock_period, final_array_sold, final_array_bought, package_size):
        if (stock_period <= len(final_array_sold)):
            soldS = final_array_sold[stock_period-1]
            boughtS = final_array_bought[stock_period-1]
            resultS = boughtS - soldS
            if resultS >= 1:
                self.stock_list.append(resultS)
                return self.calculate_avg_stock(stock_period + 1, final_array_sold, final_array_bought, package_size)
            else:
                self.stock_list.append(package_size/2)
                return self.calculate_avg_stock(stock_period + 1, final_array_sold, final_array_bought, package_size)
        else:
            if (len(self.stock_list) > 0):
                average_value = sum(self.stock_list) / len(self.stock_list)
                rounded_up_average = math.ceil(average_value)
                self.stock_list.clear()
                return rounded_up_average
            else:
                return 0
            
    def calculate_last_stock(self, final_array_sold, final_array_bought):
        for index, value in enumerate(final_array_bought):
            if value > 0:
                stock = value
                first_index = index
                break  # Stop after finding the first positive value 
        sold_since_last_restock = sum(final_array_sold[:first_index+1])
        current_stock = stock - sold_since_last_restock
        return current_stock       

    def gather_data(self):
        self.login()
        # Wait for the page to load after login
        time.sleep(3)

        # Locate the "eMarket" link by its text
        emarket_link = self.driver.find_element(
            By.XPATH, '//a[contains(text(), "eMarket")]')
        emarket_link.click()

        time.sleep(1)

        stat_link = self.driver.find_element(By.XPATH, '//a[@title="Statistiche Articolo"]')
        stat_link.click()

        time.sleep(3)  # Adjust as necessary

        # Loop through all files in the folder
        for file_name in os.listdir(SPREADSHEETS_FOLDER):
            
            if not file_name.endswith('.ods'):  # Adjust for your spreadsheet extension
                raise ValueError('filkename must be ods')

            storage_name = os.path.splitext(
                file_name)[0]  # Filename without extension
            self.storage_list.append(storage_name)

            # Full path to the current spreadsheet file
            file_path = os.path.join(SPREADSHEETS_FOLDER, file_name)

            df = pd.read_excel(file_path)  # Load the spreadsheet

            current_list = []  # Create a new empty list for the current outer loop iteration
            self.orders_list.append(current_list)

            # TODO Even though i see the logic why did you use iterrows() here, but i want you to know that using it
            #  in pandas dataframe is overall not a good practice

            # Process each row in the spreadsheet
            logger.info(f"Processing file: {file_name}")
            for index, row in df.iterrows():
                product_cod = row[COLLUMN1_NAME]  # Cod Article
                product_var = row[COLLUMN2_NAME]  # Var Article
                package_size = row[COLLUMN3_NAME]  # Package size

                if product_cod == 32052:
                    time.sleep(0.3)  # TODO this could implement a black list
                # Now, switch to the iframe that contains the required script
                iframe = self.driver.find_element(By.ID, "ifStatistiche Articolo")
                self.driver.switch_to.frame(iframe)

                try:
                    # Locate the input fields, clean and fill them
                    cod_art_field = self.driver.find_element(By.NAME, 'cod_art')
                    var_art_field = self.driver.find_element(By.NAME, 'var_art')
                    cod_art_field.clear()
                    var_art_field.clear()
                    cod_art_field.send_keys(product_cod)
                    var_art_field.send_keys(product_var)
                    self.actions.send_keys(Keys.ENTER)
                    self.actions.perform()
                    time.sleep(1)

                    # Now that you're inside the iframe, attempt to extract the data
                    sold_quantities = self.driver.execute_script(
                        "return window.str_qta_vend;")
                    bought_quantities = self.driver.execute_script(
                        "return window.str_qta_acq;")

                except UnexpectedAlertPresentException:
                    logger.info(
                        "Alert present: Codice Articolo Non Valido. Going back and continuing.")
                    continue  # Skip to the next iteration of the loop

                # Wait for the search results to load

                # Split into two lists: one for 2024 and one for 2023
                # Values for 2024 (January, February, etc.)
                sold_quantities_2024 = sold_quantities[::2]
                # Values for 2023 (January, February, etc.)
                sold_quantities_2023 = sold_quantities[1::2]
                # Values for 2024 (January, February, etc.)
                bought_quantities_2024 = bought_quantities[::2]
                # Values for 2023 (January, February, etc.)
                bought_quantities_2023 = bought_quantities[1::2]

                cleaned_2024_sold = clean_and_convert(sold_quantities_2024)
                cleaned_2023_sold = clean_and_convert(sold_quantities_2023)
                cleaned_2024_bought = clean_and_convert(bought_quantities_2024)
                cleaned_2023_bought = clean_and_convert(bought_quantities_2023)

                # If any of the cleaned lists is None (indicating invalid decimal), skip this article (outer loop iteration)
                if not cleaned_2024_sold or not cleaned_2023_sold or not cleaned_2024_bought or not cleaned_2023_bought:
                
                    logger.info(f"Skipping article at index: {index} due to invalid decimal in data")

                    self.next_article(product_cod, product_var, package_size)
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
                while len(final_array_bought) > 1 and i < self.months_to_discard:
                    final_array_sold.pop(0)
                    final_array_bought.pop(0)
                    i += 1

                # Remove the last elements from both lists if the bought-list has a zero as last element
                while len(final_array_bought) > 0 and final_array_bought[-1] == 0:
                    final_array_bought.pop()
                    final_array_sold.pop()

                if len(final_array_bought) <= 1:
                    self.next_article(product_cod, product_var, package_size)
                    continue

                # logger.info the results  TODO Can be eresed
                logger.info(f"Sold Quantities: {final_array_sold}")
                logger.info(f"Bought Quantities: {final_array_bought}")

                # Get the Variables form the Env
                sales_period = os.getenv("Periodo")
                sales_period = int(sales_period)
                stock_period = os.getenv("Giacenza")
                stock_period = int(stock_period)
                coverage = os.getenv("Copertura")
                coverage = float(coverage)
                current_day = datetime.now().day

                # Calculate Stock
                stock_period = min(stock_period, len(final_array_sold))

                stock = self.calculate_avg_stock(
                    stock_period=stock_period,
                    final_array_sold=final_array_sold,
                    final_array_bought=final_array_bought,
                    package_size=package_size
                )
                logger.info(f"Avg. Stock = {stock}")

                # Calculate stock since last restock
                current_stock = self.calculate_last_stock(final_array_sold=final_array_sold,final_array_bought=final_array_bought)
                logger.info(f"Supposed Stock = {current_stock}")
                
                # Calculate avg. daily sales
                sales_period = min(sales_period, len(final_array_sold))
                sold_daily = sum(final_array_sold[:sales_period])
                cleaned_2023_sold.reverse()
                last_year_current_month = cleaned_2023_sold[self.current_month-1]
                if (last_year_current_month != 0):
                    sold_daily += last_year_current_month
                else:
                    sales_period -= 1
                current_day -= 1
                avg_daily_sales = sold_daily / ((sales_period*30)+(current_day))
                logger.info(f"Avg. Daily Sales = {avg_daily_sales}")
                if (avg_daily_sales == 0):  # Skip order of articles that aren't currently being sold
                    self.next_article(product_cod, product_var, package_size)
                    continue
                
                # Average Monthly Sales
                if (len(final_array_sold) >= 6):
                    sales_period_yearly = min(12, len(final_array_sold))
                    sold_yearly = sum(final_array_sold[:sales_period_yearly])
                    avg_monthly_sales = sold_yearly / sales_period_yearly
                    logger.info(f"Avg. Monthly Sales = {avg_monthly_sales}")
                else:
                    avg_monthly_sales = -1
                    logger.info(f"Avg. Monthly Sales are not available for this article")


                # Calculate recent months Average Sales & Deviation
                if len(final_array_sold) >= 4:
                    # Take the last 3 months
                    recent_months = sum(final_array_sold[1:4])/3
                    logger.info(f"Average Sales in recent months = {recent_months}")
                    this_month = final_array_sold[0]
                    last_month = final_array_sold[1]
                    days_to_recover = 30 - (datetime.now().day - 1)
                    if (days_to_recover > 0):
                        last_month = (days_to_recover/30)*last_month
                        this_month += last_month
                    if recent_months != 0:
                        deviation = ((this_month - recent_months) /
                                    recent_months)*100
                        deviation = round(deviation, 2)
                    else:
                        deviation = 0
                    logger.info(f"Deviation = {deviation} %")
                    # deviation /= 2
                    avg_daily_sales = avg_daily_sales * (1 + deviation / 100)
                else:
                    logger.info(f"Deviation is not available for this article") 

                # Calculate if a new order must be done
                restock = avg_daily_sales*coverage
                if restock > stock:
                    
                    restock -= stock
                    
                    if restock >= package_size:
                        restock = custom_round(restock / package_size) # At least 1 order will be made
                    else:
                        restock = custom_round2(restock / package_size, deviation, current_stock) # Additional evaluations required

                    if restock == 0:
                        self.next_article(product_cod, product_var, package_size)
                        continue
                    combined_string = '.'.join(map(str, [product_cod, product_var, restock]))
                    current_list.append(combined_string)
                    logger.info("ORDER THIS: " + combined_string + "!")
                    self.number_of_orders += restock
                elif(0 < avg_monthly_sales <= 10):                    
                    if current_stock <= 1:
                        combined_string = '.'.join(map(str, [product_cod, product_var, 1]))
                        current_list.append(combined_string)
                        logger.info("ORDER THIS: " + combined_string + "!")
                    else:
                        logger.info("Will NOT order this: " + str(product_cod) +
                                "." + str(product_var) + "." + str(package_size) + "!")                        
                else:
                    logger.info("Will NOT order this: " + str(product_cod) +
                                "." + str(product_var) + "." + str(package_size) + "!")
                logger.info(f"=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/")
                self.driver.back()
                time.sleep(0.3)