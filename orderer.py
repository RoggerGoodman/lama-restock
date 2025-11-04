from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.actions.wheel_input import ScrollOrigin
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import time
import re
from selenium.common.exceptions import TimeoutException
from credentials import PASSWORD, USERNAME
from logger import logger


class Orderer:

    def __init__(self) -> None:
        #Set up the Selenium WebDriver (Ensure to have the correct browser driver installed)
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # Run Chrome in headless mode
        chrome_options.add_argument("--no-sandbox")  # Required for some environments
        chrome_options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems
        chrome_options.add_argument("--disable-gpu")  # Applicable only if you are running on Windows

        self.driver = webdriver.Chrome()
        self.wait = WebDriverWait(self.driver, 10)
        self.actions = ActionChains(self.driver)
        
    # Load the webpage
    def login(self):
        self.driver.get('https://dropzone.pac2000a.it/')

        # Wait for the page to fully load
        self.wait.until(
            EC.presence_of_element_located((By.ID, "username"))
        )

        # Login
        username_field = self.driver.find_element(By.ID, "username")
        password_field = self.driver.find_element(By.ID, "password")
        username_field.send_keys(USERNAME)
        password_field.send_keys(PASSWORD)
        self.actions.send_keys(Keys.ENTER)
        self.actions.perform()

        self.wait.until(
            EC.presence_of_element_located((By.ID, "carta31"))
        )

        orders_menu = self.driver.find_element(By.ID, "carta31")
        orders_menu.click()

        self.wait.until(
            EC.presence_of_element_located((By.ID, "carta139"))
        )

        orders_menu1 = self.driver.find_element(By.ID, "carta139")
        orders_menu1.click()

        self.wait.until(
            lambda driver: len(driver.window_handles) > 1
        )

        self.driver.switch_to.window(self.driver.window_handles[-1])  # Switch to the new tab

        self.wait.until(
            EC.presence_of_element_located((By.ID, "Ordini"))
        )

        time.sleep(1) 

        orders_menu1 = self.driver.find_element(By.ID, "Ordini")
        orders_menu1.click()

        lista_link = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, "//a[@href='./lista']"))
        )

        # Click the "Lista" link
        lista_link.click()

        # Wait for the modal to be present
        modal = self.wait.until(
            EC.presence_of_element_located((By.CLASS_NAME, "modal-content"))
)

        self.wait.until(
            EC.presence_of_element_located((By.XPATH, "//button[text()='Chiudi']"))
        )

        time.sleep(1)

        close_button = self.driver.find_element(By.XPATH, "//button[text()='Chiudi']")
        close_button.click()


        time.sleep(1)

    def make_orders(self, storage: str, order_list: tuple):

        desired_value = re.sub(r'^\d+\s+', '', storage)

        self.wait.until(
            EC.presence_of_element_located((By.ID, "newRowButtonMenuSopra"))
        )

        order_button = self.driver.find_element(By.ID, "newRowButtonMenuSopra")
        order_button.click()

        time.sleep(2)

        # Step 1: Wait for the modal to appear
        modal_container = self.wait.until(
            EC.visibility_of_element_located((By.ID, "finestraInsertOrdini"))  # Ensure the modal is visible
        )

        # Step 2: Wait for the button inside the modal using XPath
        button_inside_modal = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, "//*[@id='IDCodiceClienteBis']/div[1]/div/div[1]/span[2]"))
        )

        # Step 3: Click the button
        button_inside_modal.click()

        # Full XPath to locate the element
        xpath = "/html/body/div[2]/div[2]/div[5]/div/div/div/div[2]/div/form/div[1]/div/smart-combo-box/div[1]/div/div[2]/smart-list-box/div[1]/div[2]/div[2]/smart-list-item"

        # Step 1: Wait for the element to be clickable using full XPath
        element = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, xpath))
        )

        # Step 2: Click the element 
      
        element.click()

        time.sleep(1)

        # Step 2: Wait for the button inside the modal using XPath 
        button_inside_modal2 = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, "//*[@id='magazziniInsert']/div[1]/div/div[1]/span[2]"))
        )

        # Step 3: Click the button
        button_inside_modal2.click()

        time.sleep(1)

        # Find all smart-list-item elements inside the dropdown
        combo_box_element = self.driver.find_element(By.ID, "magazziniInsert")
        loop = True

        self.actions.send_keys(Keys.ARROW_DOWN)
        self.actions.perform()
        time.sleep(0.5)
        self.actions.send_keys(Keys.ARROW_DOWN)
        self.actions.perform()
        time.sleep(0.5)
        self.actions.send_keys(Keys.ARROW_UP)
        self.actions.perform()
        time.sleep(0.5)
        self.actions.send_keys(Keys.ARROW_UP)
        self.actions.perform()
        time.sleep(0.5)
        # Get the value of the 'value' attribute
        # Arrivato alla fine della dropdown list non torna su, bisogna fixare forse, dipende da come vengono processati i file dalla cartella in cui sono salvate le liste
        input_value = combo_box_element.get_attribute("value")
        if input_value == desired_value:
            self.actions.send_keys(Keys.ESCAPE)
            self.actions.perform()
            loop = False
        # Loop through all the items and check for the matching label
        while loop:
            self.actions.send_keys(Keys.ARROW_DOWN)
            self.actions.perform()
            time.sleep(0.5)
            # Get the value of the 'value' attribute
            # Arrivato alla fine della dropdown list non torna su, bisogna fixare forse, dipende da come vengono processati i file dalla cartella in cui sono salvate le liste
            input_value = combo_box_element.get_attribute("value")
            if input_value == desired_value:
                self.actions.send_keys(Keys.ESCAPE)
                self.actions.perform()
                break
        time.sleep(1)

        # Find the 'Conferma' button using its class or smart-id
        confirm_button = self.driver.find_element(By.XPATH, '//*[@id="confermaInsertTestata"]')

        # Click the 'Conferma' button
        confirm_button.click()

        self.wait.until(
            lambda driver: len(driver.window_handles) > 2
        )

        self.driver.switch_to.window(self.driver.window_handles[-1])  # Switch to the new tab

        self.wait.until(
            EC.presence_of_element_located((By.ID, "addCatalogoButtonTNuovo"))
        )

        new_order_button = self.driver.find_element(By.ID, "addCatalogoButtonTNuovo")
        new_order_button.click()

        time.sleep(1)

        self.wait.until(
            lambda driver: len(driver.window_handles) > 3
        )

        self.driver.switch_to.window(self.driver.window_handles[-1])  # Switch to the new tab

        button_inside_modal3 = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, "//*[@id='confermaFooterFiltro']/button"))
        )

        time.sleep(1)

        # Step 3: Click the button
        button_inside_modal3.click()

        time.sleep(120)
        self.driver.maximize_window()	
        time.sleep(1)

        hover_target1 = self.driver.find_element(By.XPATH, "//*[@id='gridListino']/div[1]/div[5]/div[1]/div[1]/smart-grid-column[4]")
        hover_target2 = self.driver.find_element(By.XPATH, "//*[@id='gridListino']/div[1]/div[5]/div[1]/div[1]/smart-grid-column[5]")

        self.actions.move_to_element(hover_target1).perform()
        time.sleep(1)
        button_xpath1 = "//*[@id='gridListino']/div[1]/div[5]/div[1]/div[1]/smart-grid-column[4]/div[4]/div[5]"
        button1 = self.wait.until(
                EC.element_to_be_clickable((By.XPATH, button_xpath1))
        )
        button1.click()
        time.sleep(0.2)
        options1 = self.driver.find_element(By.XPATH, "/html/body/div[4]/div/smart-filter-panel/div/div[2]/smart-input[1]/div/input")
        options1.click()
        time.sleep(0.2)
        scrollbar = self.driver.find_element(By.XPATH, "/html/body/smart-scroll-viewer/div/smart-scroll-bar[1]/div[1]/div[2]")
        self.actions.move_to_element(scrollbar).perform()
        time.sleep(0.2)
        origin = ScrollOrigin.from_element(scrollbar)
        self.actions.scroll_from_origin(origin, 0, 500).perform()
        filter = self.driver.find_element(By.XPATH, "/html/body/smart-scroll-viewer/div/div/div/ul/li[7]")
        filter.click()
        time.sleep(0.2)
        filterB = self.driver.find_element(By.XPATH, "/html/body/div[4]/div/smart-filter-panel/div/div[3]/smart-button[1]/button")
        filterB.click()
        time.sleep(0.2)

        for cod_part, var_part, qty_part in order_list:

            self.actions.move_to_element(hover_target1).perform()
            button_xpath1 = "//*[@id='gridListino']/div[1]/div[5]/div[1]/div[1]/smart-grid-column[4]/div[4]/div[5]"
            button1 = self.wait.until(
                EC.element_to_be_clickable((By.XPATH, button_xpath1))
            )
            button1.click()
            time.sleep(0.2)
            self.actions.key_down(Keys.CONTROL).send_keys('a').key_up(Keys.CONTROL).send_keys(Keys.BACKSPACE)
            self.actions.perform()
            self.actions.send_keys(cod_part)
            self.actions.send_keys(Keys.ENTER)
            self.actions.perform()
            time.sleep(0.2)
            self.actions.move_to_element(hover_target2).perform()
            button_xpath2 = "//*[@id='gridListino']/div[1]/div[5]/div[1]/div[1]/smart-grid-column[5]/div[4]/div[5]"
            button2 = self.wait.until(
                EC.element_to_be_clickable((By.XPATH, button_xpath2))
            )
            button2.click()
            time.sleep(0.2)
            self.actions.key_down(Keys.CONTROL).send_keys('a').key_up(Keys.CONTROL).send_keys(Keys.BACKSPACE)
            self.actions.perform()
            self.actions.send_keys(var_part)
            self.actions.send_keys(Keys.ENTER)
            self.actions.perform()
            time.sleep(0.2)
            try:
                quantita_element = self.wait.until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "div[data-field='Quantita'].align-right.smart-label"))
                )
            except TimeoutException:
                logger.info(f'{cod_part}.{var_part} failed because it was not found')
                continue  # Skip to the next loop iteration if element is not found in 10 seconds
            quantita_element.click()
            self.actions.double_click(quantita_element).perform()
            time.sleep(0.2)
            self.actions.send_keys(qty_part)
            self.actions.send_keys(Keys.ENTER)
            self.actions.perform()
            time.sleep(0.2)
        
        # Wait until the button is clickable
        send_button = self.wait.until(
            EC.element_to_be_clickable((By.ID, "invioOrdineButton"))
        )

        # Click the button
        # send_button.click()

        # Switch to the previous tab
        self.driver.switch_to.window(self.driver.window_handles[-2])

'''self.wait.until(
                EC.text_to_be_present_in_element(
                    (By.ID, "jqxNotificationDefaultContainer-top-right"),
                    "Articolo"
                )
            )'''           