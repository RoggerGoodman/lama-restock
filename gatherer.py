from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import math
import os
import pandas as pd
from selenium.common.exceptions import UnexpectedAlertPresentException
from selenium.webdriver.chrome.options import Options
from consts import COLLUMN1_NAME, COLLUMN2_NAME, COLLUMN3_NAME, COLLUMN4_NAME, SPREADSHEETS_FOLDER
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
        # chrome_options.add_argument("--headless")  # Run Chrome in headless mode
        # chrome_options.add_argument("--no-sandbox")  # Required for some environments
        # chrome_options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems
        # chrome_options.add_argument("--disable-gpu")  # Applicable only if you are running on Windows

        self.driver = webdriver.Chrome(options=chrome_options)
        self.actions = ActionChains(self.driver)
        self.orders_list = []  # it will contain all the lists of orders
        self.storage_list = [] # it will contain the name of all the storages gathered from the filename of the tables
        self.blacklist = {"21820", "21822", "21823", "21824", "26590"}
        
    def login(self):
        try:
            # TODO Get rid of the constant  
            self.driver.get('https://www.pac2000a.it/PacApplicationUserPanel/faces/home.jsf')
        except Exception as exc:
            logger.info('Somwething went wrong, smartie')

        # Wait for the username field to be present, indicating that the page has loaded
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.ID, "username"))
        )

        # Log in by entering the username and password, then clicking the login button
        username_field = self.driver.find_element(By.ID, "username")
        password_field = self.driver.find_element(By.ID, "Password")
        login_button = self.driver.find_element(By.CLASS_NAME, "btn-primary")

        username_field.send_keys(USERNAME)
        password_field.send_keys(PASSWORD)
        login_button.click()
    
    def next_article(self, part1, part2, part3, name, reason):
        logger.info(f"Will NOT order {name}: {part1}.{part2}.{part3}!")
        logger.info(f"Reason : {reason}")
        logger.info(f"=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/")

    def order_this(self, current_list, product_cod, product_var, qty, name, reason):
        combined_string = '.'.join(map(str, [product_cod, product_var, qty]))
        current_list.append(combined_string)
        logger.info(f"ORDER {name}: " + combined_string + "!")
        logger.info(f"Reason : {reason}")
        logger.info(f"=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/=/")        
                           
    def gather_data(self):
        self.login()
        # Wait for the page to load after login
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//a[contains(text(), "eMarket")]'))
        )

        # Locate the "eMarket" link by its text
        emarket_link = self.driver.find_element(By.XPATH, '//a[contains(text(), "eMarket")]')
        emarket_link.click()

        WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, '//a[@title="Statistiche Articolo"]'))
        )

        stat_link = self.driver.find_element(By.XPATH, '//a[@title="Statistiche Articolo"]')
        stat_link.click()

        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.ID, "ifStatistiche Articolo"))
        )

        # Loop through all files in the folder
        for file_name in os.listdir(SPREADSHEETS_FOLDER):
            
            if not file_name.endswith('.ods'):  # Adjust for your spreadsheet extension
                raise ValueError('filename must be ods')

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
                package_size = row[COLLUMN3_NAME] # Package size
                product_name = row[COLLUMN4_NAME] # Codice Name

                if product_cod in self.blacklist:
                    logger.info(f"Skipping blacklisted Cod Article: {product_cod}")
                    continue  # Skip to the next iteration

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
                     # Wait until the script variables are defined and available
                    WebDriverWait(self.driver, 10).until(
                        lambda driver: driver.execute_script("return typeof window.str_qta_vend !== 'undefined'")
                    )

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

                self.driver.back()
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, "ifStatistiche Articolo"))
                )

                cleaned_current_year_sold = helpers.clean_convert_reverse(sold_quantities_current_year)
                cleaned_last_year_sold = helpers.clean_convert_reverse(sold_quantities_last_year)
                cleaned_current_year_bought = helpers.clean_convert_reverse(bought_quantities_current_year)
                cleaned_last_year_bought = helpers.clean_convert_reverse(bought_quantities_last_year)

                # If any of the cleaned lists is None (indicating invalid decimal), skip this article (outer loop iteration)
                if not cleaned_current_year_sold or not cleaned_last_year_sold or not cleaned_current_year_bought or not cleaned_last_year_bought:
                    logger.info(f"Skipping article at index: {index} due to invalid decimal in data")
                    reason = "The article is sold in kilos, and for now we do not manage this kind"
                    self.next_article(product_cod, product_var, package_size, product_name, reason)
                    continue  # Skip to the next row in df.iterrows()


                # Combine both lists (current year values first, then last year)
                final_array_sold = cleaned_current_year_sold + cleaned_last_year_sold
                final_array_bought = cleaned_current_year_bought + cleaned_last_year_bought

                final_array_bought, final_array_sold = helpers.prepare_array(final_array_bought, final_array_sold)

                final_array_bought, final_array_sold = helpers.detect_dead_periods(final_array_bought, final_array_sold)
                
                if len(final_array_bought) <= 1:
                    reason = "The prduct has been in the system for too little"
                    analyzer.news_recorder(f"Article {product_name}, with code {product_cod}.{product_var}")
                    self.next_article(product_cod, product_var, package_size, product_name, reason)
                    continue

                # logger.info the results  TODO Can be eresed
                logger.info(f"  Sold Quantities: {final_array_sold}")
                logger.info(f"Bought Quantities: {final_array_bought}")
                                
                sales_period = os.getenv("Periodo")
                sales_period = int(sales_period)
                # stock_period = os.getenv("Giacenza")
                # stock_period = int(stock_period)
                coverage = os.getenv("Copertura")
                coverage = float(coverage)
                                
                avg_daily_sales = helpers.calculate_weighted_avg_sales(sales_period, final_array_sold, cleaned_last_year_sold) 

                if len(final_array_bought) <= 10:
                    use_stock = True
                    stock = helpers.calculate_stock(final_array_sold=final_array_sold,final_array_bought=final_array_bought)
                else:
                    stock = 0
                    use_stock = False
                                
                if (len(final_array_sold) >= 13):
                    avg_monthly_sales = helpers.calculate_avg_monthly_sales(final_array_sold)
                else:
                    avg_monthly_sales = -1
                    logger.info(f"Avg. Monthly Sales are not available for this article")
                                
                if len(final_array_sold) >= 4:                    
                    recent_months_sales = helpers.calculate_data_recent_months(final_array_sold, 3, "sales") 
                    expected_packages = helpers.calculate_expectd_packages(final_array_bought, package_size)
                    deviation_corrected = helpers.calculate_deviation(final_array_sold, recent_months_sales)
                    avg_daily_sales_corrected = avg_daily_sales * (1 + deviation_corrected / 100)
                else:
                    recent_months_sales = -1
                    deviation_corrected = 0
                    avg_daily_sales_corrected = avg_daily_sales
                    logger.info(f"Deviation and recent months sales are not available for this article") 
                
                if (recent_months_sales == 0):  # Skip order of articles that aren't currently being sold
                    reason = "No sales in recent months, no reason to continue"
                    self.next_article(product_cod, product_var, package_size, product_name, reason)
                    continue

                stock_oscillation = helpers.calculate_stock_oscillation(final_array_bought, final_array_sold, avg_daily_sales, package_size)

                #region Calculate if a new order must be done
                req_stock = avg_daily_sales_corrected*coverage
                logger.info(f"Required stock = {req_stock:.2f}")
                restock = req_stock
                restock_corrected = req_stock

                if stock_oscillation > 0:
                    restock -= stock_oscillation
                    restock_corrected -= stock_oscillation
                else:
                    restock_corrected -= stock_oscillation    

                logger.info(f"Restock = {restock:.2f}")
                
                
                if avg_daily_sales >= 1:
                    restock, reason, stat = self.process_A_sales(stock_oscillation, package_size, deviation_corrected, restock, expected_packages, req_stock, use_stock, stock)
                elif 0 < recent_months_sales <= 14:
                    restock, reason, stat = self.process_C_sales(stock_oscillation, package_size, restock, deviation_corrected, use_stock, stock)
                elif avg_daily_sales < 1:
                    restock, reason, stat = self.process_B_sales(stock_oscillation, package_size, restock_corrected, expected_packages, use_stock, stock)
                else:
                    restock, reason, stat = None, "This is not good, there is a bug", None

                # Take Action
                if restock:
                    if avg_daily_sales <= 0.2 or avg_daily_sales_corrected <= 0.2:
                        analyzer.note_recorder(f"Article {product_name}, with code {product_cod}.{product_var}")
                    analyzer.stat_recorder(restock, stat)
                    self.order_this(current_list, product_cod, product_var, restock, product_name, reason)
                else:
                    analyzer.stat_recorder(0, stat)
                    self.next_article(product_cod, product_var, package_size, product_name, reason)
                        
            analyzer.log_statistics()
            

    '''elif restock >= math.ceil(package_size/2) and stock_oscillation <= 0:
                        reason = "Avg. dayly sales < 1, and restock is needed also oscillation is negative"
                        analyzer.stat_recorder(1, "B_success")
                        self.order_this(current_list, product_cod, product_var, 1, reason)  
                        
                        if package_size == 1:
                    '''
    
    def process_A_sales(self, stock_oscillation, package_size, deviation_corrected, restock, expected_packages, req_stock, use_stock, stock):
        """Handles cases for avg_daily_sales >= 1."""
        if restock >= package_size:
            restock = helpers.custom_round(restock / package_size, 0.7)
            if stock_oscillation <= -package_size:
                restock += 1
            return restock, "A1", "A_success"
        
        if use_stock and stock <= math.floor(package_size / 2): #TODO If they remain equal for all category, make only 1 check before categorization
            return 1, "A2", "A_success"

        if restock > math.ceil(package_size / 2) and (deviation_corrected > 20 or stock_oscillation <= math.floor(-package_size / 3)):
            order = 2 if stock_oscillation <= -package_size else 1
            return order, "A3", "A_success"

        if stock_oscillation <= math.floor(-package_size / 2):
            return 1, "A4", "A_success"

        if expected_packages >= 1 and stock_oscillation < package_size / 2:
            return 1, "A5", "A_success"

        if package_size >= 20 and restock >= math.ceil(package_size / 4):
            return 1, "A6", "A_success"

        if stock_oscillation <= math.ceil(req_stock / 2) and expected_packages > 0.3:
            return 1, "A7", "A_success"
        
        return None, "A0", "A_fail"


    def process_C_sales(self, stock_oscillation, package_size, restock, deviation_corrected, use_stock, stock):
        """Handles cases for 0 < recent_months_sales <= 14."""
        if use_stock and stock <= math.floor(package_size / 2):
            return 1, "C1", "C_success"

        if stock_oscillation <= math.floor(-package_size / 3):
            return 1, "C2", "C_success"

        if package_size <= 8 and stock_oscillation < math.ceil(-package_size / 4):
            return 1, "C3", "C_success"

        if restock >= 1.8 and deviation_corrected >= 10 and stock_oscillation < 0:
            return 1, "C4", "C_success"

        return None, "C0", "C_fail"


    def process_B_sales(self, stock_oscillation, package_size, restock_corrected, expected_packages, use_stock, stock):
        """Handles cases for avg_daily_sales < 1."""
        if use_stock and stock <= math.floor(package_size / 2):
            return 1, "B1", "B_success"
        
        if restock_corrected > package_size:
            return 1, "B2", "B_success"

        if expected_packages >= 1 and stock_oscillation <= 0:
            return 1, "B3", "B_success"

        if expected_packages >= 0.5 and stock_oscillation <= math.ceil(-package_size / 3):
            return 1, "B4", "B_success"

        if stock_oscillation <= math.floor(-package_size / 3):
            return 1, "B5", "B_success"
        
        if package_size <= 8 and stock_oscillation <= 0:
            return 1, "B6", "B_success"

        return None, "B0", "B_fail"
