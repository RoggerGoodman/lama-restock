from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
from datetime import datetime
import pandas as pd
from selenium.common.exceptions import UnexpectedAlertPresentException
from selenium.webdriver.chrome.options import Options
from consts import COLLUMN1_NAME, COLLUMN2_NAME, COLLUMN3_NAME, COLLUMN4_NAME, COLLUMN5_NAME, COLLUMN6_NAME, SPREADSHEETS_FOLDER
from credentials import PASSWORD, USERNAME
from logger import logger
from helpers import Helper
from analyzer import analyzer
from blacklists import blacklists
from processor_A import process_A_sales
from processor_B import process_B_sales
from processor_C import process_C_sales
from processor_N import process_N_sales

class Gatherer:

    def __init__(self, helper: Helper) -> None:
        # Use the shared Helper instance passed from main
        self.helper = helper
        
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

    def login(self):
        try:  
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

            if storage_name in blacklists:
                blacklist_granular = blacklists[storage_name]["blacklist_granular"].copy()

            # Full path to the current spreadsheet file
            file_path = os.path.join(SPREADSHEETS_FOLDER, file_name)

            df = pd.read_excel(file_path)  # Load the spreadsheet
            analyzer.get_original_list(df)
            order_list = []

            # Process each row in the spreadsheet
            logger.info(f"Processing file: {file_name}")
            for index, row in df.iterrows():
                product_cod = row[COLLUMN1_NAME]  # Cod Article
                product_var = row[COLLUMN2_NAME]  # Var Article
                package_size = row[COLLUMN3_NAME] # Package size
                product_name = row[COLLUMN4_NAME] # Cod Name
                package_multi = row[COLLUMN5_NAME] # Package Multiplier
                product_availability = row[COLLUMN6_NAME] # Yes/No

                if (product_cod, product_var) in blacklist_granular:
                    logger.info(f"Skipping blacklisted Cod Article and Var: {product_cod}.{product_var}")
                    blacklist_granular.remove((product_cod, product_var))  # Remove from runtime copy
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
                    
                    # Wait until the script variables are defined and available
                    WebDriverWait(self.driver, 10).until(
                        lambda driver: driver.execute_script("return typeof window.str_qta_acq !== 'undefined'")
                    )

                    bought_quantities = self.driver.execute_script(
                        "return window.str_qta_acq;")

                except UnexpectedAlertPresentException:
                    logger.info("Alert present: Invalid product code. Going back and continuing.")
                    self.actions.send_keys(Keys.ENTER)
                    continue  # Skip to the next iteration of the loop
                

                package_size = int(package_size)
                # package_multi = int(package_multi)
                package_multi = int(float(package_multi.replace(',', '.'))) # TODO Needs to work in both cases
                package_size *= package_multi

                sold_quantities_current_year = sold_quantities[::2]
                sold_quantities_last_year = sold_quantities[1::2]
                bought_quantities_current_year = bought_quantities[::2]
                bought_quantities_last_year = bought_quantities[1::2]

                self.driver.back()
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, "ifStatistiche Articolo"))
                )

                cleaned_current_year_sold = self.helper.clean_convert_reverse(sold_quantities_current_year)
                cleaned_last_year_sold = self.helper.clean_convert_reverse(sold_quantities_last_year)
                cleaned_current_year_bought = self.helper.clean_convert_reverse(bought_quantities_current_year)
                cleaned_last_year_bought = self.helper.clean_convert_reverse(bought_quantities_last_year)

                # If any of the cleaned lists is None (indicating invalid decimal), skip this article (outer loop iteration)
                if not cleaned_current_year_sold or not cleaned_last_year_sold or not cleaned_current_year_bought or not cleaned_last_year_bought:
                    logger.info(f"Skipping article at index: {index} due to invalid decimal in data")
                    reason = "The article is sold in kilos, and for now we do not manage this kind"
                    self.helper.next_article(product_cod, product_var, package_size, product_name, reason)
                    self.helper.line_breaker()
                    continue  # Skip to the next row in df.iterrows()

                # Combine both lists (current year values first, then last year)
                final_array_sold = cleaned_current_year_sold + cleaned_last_year_sold
                final_array_bought = cleaned_current_year_bought + cleaned_last_year_bought

                final_array_bought, final_array_sold = self.helper.prepare_array(final_array_bought, final_array_sold)
                
                if len(final_array_bought) == 0 and product_availability == "Si":
                    reason = "The prduct has never been in the system"
                    analyzer.brand_new_recorder(f"Article {product_name}, with code {product_cod}.{product_var}")
                    self.helper.next_article(product_cod, product_var, package_size, product_name, reason)
                    self.helper.line_breaker()
                    continue
                if len(final_array_bought) > 3 and len(final_array_sold) > 3:
                    final_array_bought, final_array_sold = self.helper.detect_dead_periods(final_array_bought, final_array_sold)
                if len(final_array_bought)  <= 0:
                    if product_availability == "No":
                        reason = "The article is NOT available for restocking and hasn't been bought or sold for the last 3 months" 
                        self.helper.next_article(product_cod, product_var, package_size, product_name, reason)
                        self.helper.line_breaker()
                        continue
                    else :
                        reason = "The article is available once more for restocking but hasn't been bought or sold for the last 3 months"
                        analyzer.brand_new_recorder(f"Article {product_name}, with code {product_cod}.{product_var}")
                        self.helper.next_article(product_cod, product_var, package_size, product_name, reason)
                        self.helper.line_breaker()
                        continue

                # log the results
                logger.info(f"Processing {product_cod}.{product_var}")
                logger.info(f"  Sold Quantities: {final_array_sold}")
                logger.info(f"Bought Quantities: {final_array_bought}")
                
                                
                sales_period = os.getenv("Periodo")
                sales_period = int(sales_period)
                coverage = os.getenv("Copertura")
                coverage = float(coverage)
                                
                avg_daily_sales = self.helper.calculate_weighted_avg_sales(sales_period, final_array_sold, cleaned_last_year_sold) 
                if len(final_array_bought) <= 10:
                    use_stock = True
                    stock = self.helper.calculate_stock(final_array_sold=final_array_sold,final_array_bought=final_array_bought)
                else:
                    stock = 0
                    use_stock = False

                logger.info(f"Package size = {package_size}")

                if len(final_array_sold) >= 4:                    
                    recent_months_sales = self.helper.calculate_data_recent_months(final_array_sold, 3)
                    expected_packages = self.helper.calculate_expectd_packages(final_array_bought, package_size)
                    deviation_corrected = self.helper.calculate_deviation(final_array_sold, recent_months_sales)
                    avg_daily_sales_corrected = avg_daily_sales * (1 + deviation_corrected / 100)
                else:
                    recent_months_sales = -1
                    expected_packages = 0
                    deviation_corrected = 0
                    avg_daily_sales_corrected = avg_daily_sales
                    logger.info(f"Deviation and recent months sales are not available for this article") 
                
                if (recent_months_sales == 0):  # Skip order of articles that aren't currently being sold
                    reason = "No sales in recent months, no reason to continue"
                    self.helper.next_article(product_cod, product_var, package_size, product_name, reason)
                    self.helper.line_breaker()
                    continue

                if not use_stock:
                    stock_oscillation = self.helper.calculate_stock_oscillation(final_array_bought, final_array_sold, avg_daily_sales)

                req_stock = avg_daily_sales_corrected*coverage
                logger.info(f"Required stock = {req_stock:.2f}")
                self.helper.line_breaker()

                package_consumption = req_stock / package_size 
                real_need = req_stock

                if use_stock:
                    real_need -= stock
                    category = "N"
                    result, check, status = process_N_sales(package_size, deviation_corrected, real_need, expected_packages, req_stock, stock, self.helper)
                elif package_consumption >= 1:
                    if stock_oscillation > 0:
                        real_need -= stock_oscillation
                    category = "A"
                    result, check, status = process_A_sales(stock_oscillation, package_size, deviation_corrected, real_need, expected_packages, req_stock, package_consumption, self.helper)
                elif package_consumption >= 0.3:
                    if stock_oscillation > 0:
                        real_need -= stock_oscillation
                    category = "B"
                    result, check, status = process_B_sales(stock_oscillation, package_size, deviation_corrected, real_need, expected_packages, use_stock, stock)
                else :
                    if stock_oscillation > 0:
                        real_need -= stock_oscillation
                    category = "C"
                    result, check, status = process_C_sales(stock_oscillation, package_size, real_need, deviation_corrected, use_stock, stock)

                if result:
                # Log the restock action
                    if avg_daily_sales <= 0.2 or avg_daily_sales_corrected <= 0.2:
                        analyzer.note_recorder(product_name, product_cod, product_var)
                    analyzer.stat_recorder(result, status)
                    self.helper.order_this(order_list, product_cod, product_var, result, product_name, category, check)
                    self.helper.line_breaker()
                else:
                    # Log that no action was taken
                    analyzer.stat_recorder(0, status)
                    self.helper.order_denied(product_cod, product_var, package_size, product_name, category, check)
                    self.helper.line_breaker()

            self.orders_list.append(order_list)
            analyzer.log_statistics()

