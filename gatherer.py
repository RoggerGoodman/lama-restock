from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import time
import math
import os
import pandas as pd
from selenium.common.exceptions import UnexpectedAlertPresentException
from selenium.webdriver.chrome.options import Options
from consts import COLLUMN1_NAME, COLLUMN2_NAME, COLLUMN3_NAME, SPREADSHEETS_FOLDER
from credentials import PASSWORD, USERNAME
from helpers import Helper
from logger import logger
from analyzer import Analyzer

analyzer = Analyzer()
helpers = Helper()

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
        self.storage_list = [] # it will contain the name of all the storages gathered from the filename of the tables
        
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
    
    def next_article(self, part1, part2, part3, reason):
        logger.info("Will NOT order this: " + str(part1) +
                    "." + str(part2) + "." + str(part3) + "!")
        logger.info(f"Reason : {reason}")
        logger.info(f"=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/")
        self.driver.back()
        time.sleep(0.3)
         
    def calculate_last_stock(self, final_array_sold, final_array_bought, values_to_pick):
        stock = 0
        for index, value in enumerate(final_array_bought):
            if value > 0:
                stock += value
                last_index = index
                values_to_pick -= 1
                if values_to_pick == 0:
                    break  # Stop after finding the first positive value 
        sold_since_last_restock = sum(final_array_sold[:last_index+1])
        current_stock = stock - sold_since_last_restock
        return current_stock

    def order_this(self, current_list, product_cod, product_var, qty, reason):
        combined_string = '.'.join(map(str, [product_cod, product_var, qty]))
        current_list.append(combined_string)
        logger.info("ORDER THIS: " + combined_string + "!")
        logger.info(f"Reason : {reason}")
        logger.info(f"=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/")        
        self.driver.back()
        time.sleep(0.3)
                           
    def gather_data(self):
        self.login()
        # Wait for the page to load after login
        time.sleep(3)

        # Locate the "eMarket" link by its text
        emarket_link = self.driver.find_element(By.XPATH, '//a[contains(text(), "eMarket")]')
        emarket_link.click()

        time.sleep(1)

        stat_link = self.driver.find_element(By.XPATH, '//a[@title="Statistiche Articolo"]')
        stat_link.click()

        time.sleep(3)  # Adjust as necessary

        # Loop through all files in the folder
        for file_name in os.listdir(SPREADSHEETS_FOLDER):
            
            if not file_name.endswith('.ods'):  # Adjust for your spreadsheet extension
                raise ValueError('filkename must be ods')

            storage_name = os.path.splitext(file_name)[0]  # Filename without extension
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

                # if product_cod == 32052:
                    # time.sleep(0.3)  # TODO this could implement a black list

                # Now, switch to the iframe that contains the required script
                iframe = self.driver.find_element(By.ID, "ifStatistiche Articolo")
                self.driver.switch_to.frame(iframe)

                # Locate the input fields, clean and fill them
                try:                    
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
                    logger.info("Alert present: Invalid product code. Going back and continuing.")
                    self.actions.send_keys(Keys.ENTER)
                    continue  # Skip to the next iteration of the loop

                sold_quantities_current_year = sold_quantities[::2]
                sold_quantities_last_year = sold_quantities[1::2]
                bought_quantities_current_year = bought_quantities[::2]
                bought_quantities_last_year = bought_quantities[1::2]

                cleaned_current_year_sold = helpers.clean_and_convert(sold_quantities_current_year)
                cleaned_last_year_sold = helpers.clean_and_convert(sold_quantities_last_year)
                cleaned_current_year_bought = helpers.clean_and_convert(bought_quantities_current_year)
                cleaned_last_year_bought = helpers.clean_and_convert(bought_quantities_last_year)

                # If any of the cleaned lists is None (indicating invalid decimal), skip this article (outer loop iteration)
                if not cleaned_current_year_sold or not cleaned_last_year_sold or not cleaned_current_year_bought or not cleaned_last_year_bought:
                    logger.info(f"Skipping article at index: {index} due to invalid decimal in data")
                    reason = "The article is sold in kilos, and for now we do not manage this kind"
                    self.next_article(product_cod, product_var, package_size, reason)
                    continue  # Skip to the next row in df.iterrows()

                # Reverse the order of both lists
                cleaned_current_year_sold.reverse()
                cleaned_last_year_sold.reverse()
                cleaned_current_year_bought.reverse()
                cleaned_last_year_bought.reverse()

                # Combine both lists (current year values first, then last year)
                final_array_sold = cleaned_current_year_sold + cleaned_last_year_sold
                final_array_bought = cleaned_current_year_bought + cleaned_last_year_bought

                final_array_bought, final_array_sold = helpers.prepare_array(final_array_bought, final_array_sold)
                

                if len(final_array_bought) <= 1:
                    reason = "The prduct has been in the system for too little"
                    self.next_article(product_cod, product_var, package_size, reason)
                    continue

                # logger.info the results  TODO Can be eresed
                logger.info(f"Sold Quantities: {final_array_sold}")
                logger.info(f"Bought Quantities: {final_array_bought}")
                #endregion

                #region Get the Variables form the Env
                sales_period = os.getenv("Periodo")
                sales_period = int(sales_period)
                stock_period = os.getenv("Giacenza")
                stock_period = int(stock_period)
                coverage = os.getenv("Copertura")
                coverage = float(coverage)
                
                #endregion

                #region Calculate Stock
                stock_period = min(stock_period, len(final_array_sold))

                avg_stock = helpers.calculate_avg_stock(
                    stock_period=stock_period,
                    final_array_sold=final_array_sold,
                    final_array_bought=final_array_bought,
                    package_size=package_size
                )
                
                supposed_stock = helpers.calculate_supposed_stock(
                    final_array_bought=final_array_bought,
                    final_array_sold=final_array_sold,
                    avg_stock=avg_stock
                )

                #endregion

                #region Calculate stocks types
                current_stock = self.calculate_last_stock(final_array_sold=final_array_sold,final_array_bought=final_array_bought, values_to_pick=1)
                logger.info(f"Supposed Stock = {current_stock}")
                
                avg_daily_sales = helpers.calculate_weighted_avg_sales(sales_period, final_array_sold, cleaned_last_year_sold)                            
                if (avg_daily_sales == 0):  # Skip order of articles that aren't currently being sold
                    reason = "Avg. dayly sales = 0, no reason to continue"
                    self.next_article(product_cod, product_var, package_size, reason)
                    continue

                if len(final_array_bought) <= 10:
                    use_true_stock = True
                    true_stock = helpers.calculate_true_stock(final_array_sold=final_array_sold,final_array_bought=final_array_bought)
                else:
                    use_true_stock = False
                #endregion

                #region Average Monthly Sales
                if (len(final_array_sold) >= 6):
                    avg_monthly_sales = helpers.calculate_avg_monthly_sales(final_array_sold)
                else:
                    avg_monthly_sales = -1
                    logger.info(f"Avg. Monthly Sales are not available for this article")
                #endregion

                #region Calculate recent months Average Sales & Deviation
                if len(final_array_sold) >= 4:                    
                    recent_months = helpers.calculate_avg_sales_recent_months(final_array_sold, 3) # Take the last 3 months
                    deviation_corrected = helpers.calculate_deviation(final_array_sold, recent_months)
                    avg_daily_sales_corrected = avg_daily_sales * (1 + deviation_corrected / 100)
                else:
                    logger.info(f"Deviation is not available for this article") 
                #endregion

                #region Calculate if a new order must be done
                req_stock = avg_daily_sales_corrected*coverage
                                                                          
                                        
                if(use_true_stock):
                    if true_stock <= math.ceil(package_size/2):
                        reason = "The prduct is relativly new, and true_stock is low enough"
                        analyzer.stat_recorder(1, "new_article_success")
                        self.order_this(current_list, product_cod, product_var, 1, reason)
                    else:
                        reason = "The prduct is relativly new, but true_stock not low enough"
                        analyzer.stat_recorder(0, "new_article_fail")
                        self.next_article(product_cod, product_var, package_size, reason)
                        continue  
                elif(0 < avg_monthly_sales <= 10): #TODO parametrize
                    new_current_stock = self.calculate_last_stock(final_array_sold=final_array_sold,final_array_bought=final_array_bought, values_to_pick=2)
                    current_stock = max(current_stock, new_current_stock)
                    logger.info(f"New best supposed Stock = {current_stock}")                    
                    if current_stock <= math.floor(package_size*-1/4):
                        reason = "Avg. monthly sales < 10, and current stock is low enough"
                        analyzer.stat_recorder(1, "low_success")
                        self.order_this(current_list, product_cod, product_var, 1, reason)
                    else:
                        reason = "Avg. monthly sales < 10, but current stock not low enough"
                        analyzer.stat_recorder(0, "low_fail")
                        self.next_article(product_cod, product_var, package_size, reason)
                        continue  
                elif (avg_daily_sales >= 1):
                    req_stock -= max(avg_stock, current_stock)
                    if req_stock >= package_size:
                        req_stock = helpers.custom_round(req_stock / package_size, 0.3) # At least 1 order will be made
                        reason = "Avg. dayly sales >= 1, and current stock is low enough"                     
                        analyzer.stat_recorder(req_stock, "high_success")
                        self.order_this(current_list, product_cod, product_var, req_stock, reason)
                    elif (current_stock < math.ceil(package_size/5)):
                        reason = "Avg. dayly sales >= 1, and current stock is low enough"
                        analyzer.stat_recorder(1, "high_success")
                        self.order_this(current_list, product_cod, product_var, 1, reason)
                    elif (math.ceil(req_stock) >= package_size/2 and deviation_corrected > 10): #TODO in need of judgment
                        reason = "Avg. dayly sales >= 1, and restock need is high enough also deviation is positive"
                        analyzer.stat_recorder(1, "high_success")
                        self.order_this(current_list, product_cod, product_var, 1, reason)
                    else:
                        reason = "Avg. dayly sales >= 1, but current stock not low enough"
                        analyzer.stat_recorder(0, "high_fail")
                        self.next_article(product_cod, product_var, package_size, reason)
                        continue
                elif(avg_daily_sales < 1):
                    new_current_stock = self.calculate_last_stock(final_array_sold=final_array_sold,final_array_bought=final_array_bought, values_to_pick=2)
                    def_current_stock = max(current_stock, new_current_stock)
                    req_stock = helpers.custom_round(req_stock, 0.4) - max(def_current_stock, 1) #TODO if negative set it to 1??? If it's negative it gets added otherwise
                    logger.info(f"New best supposed Stock = {def_current_stock}")
                    if def_current_stock <= max(math.floor(package_size*-3/4), -7):
                        reason = "Avg. dayly sales < 1, and current stock is low enough"
                        analyzer.stat_recorder(1, "mid_success")
                        self.order_this(current_list, product_cod, product_var, 1, reason)
                    elif (req_stock >= 3):
                        reason = "Avg. dayly sales < 1, and restock need is high enough"
                        analyzer.stat_recorder(1, "mid_success")
                        self.order_this(current_list, product_cod, product_var, 1, reason)
                    elif (current_stock < 0 and new_current_stock < 0):
                        if deviation_corrected >= 0: threshold = package_size/2 
                        else: threshold = package_size
                        if new_current_stock*-1 > threshold:
                            reason = "Avg. dayly sales < 1, and both stocks are negative"
                            analyzer.stat_recorder(1, "mid_success")
                            self.order_this(current_list, product_cod, product_var, 1, reason)
                        else:
                            reason = "Avg. dayly sales < 1, but current stock not low enough to pass the threshold"
                            analyzer.stat_recorder(0, "mid_fail")
                            self.next_article(product_cod, product_var, package_size, reason)
                            continue                                   
                    else:
                        reason = "Avg. dayly sales < 1, but current stock not low enough"
                        analyzer.stat_recorder(0, "mid_fail")
                        self.next_article(product_cod, product_var, package_size, reason)
                        continue                     
                else:
                    reason = "This is not good, there is a bug"
                    self.next_article(product_cod, product_var, package_size, reason)
                    continue
                #endregion
                        
            analyzer.log_statistics()
            

    