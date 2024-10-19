from gatherer import Gatherer


gatherer = Gatherer()
gatherer.gather_data()



# TODO create a class OrdersPlacer and hide all the functionality in there
# from selenium import webdriver
# from selenium.webdriver.common.by import By
# from selenium.webdriver.common.action_chains import ActionChains
# from selenium.webdriver.common.keys import Keys
# import time
# import math
# from datetime import datetime
# from dotenv import load_dotenv
# import os
# import pandas as pd
# from selenium.common.exceptions import UnexpectedAlertPresentException

# from config import TEST_MODE
# from helpers import custom_round, clean_and_convert
# from logger import logger

# driver.get('https://dropzone.pac2000a.it/')

# # Wait for the page to fully load
# time.sleep(2)

# # Login
# username_field = driver.find_element(By.ID, "username")
# password_field = driver.find_element(By.ID, "password")
# username_field.send_keys(USERNAME)
# password_field.send_keys(PASSWORD)
# actions.send_keys(Keys.ENTER)
# actions.perform()

# time.sleep(3)

# orders_menu = driver.find_element(By.ID, "carta31")
# orders_menu.click()

# time.sleep(2)

# orders_menu1 = driver.find_element(By.ID, "carta32")
# orders_menu1.click()

# time.sleep(3)

# driver.switch_to.window(driver.window_handles[-1])  # Switch to the new tab

# actions.click()
# actions.send_keys(Keys.ESCAPE)
# actions.perform()

# time.sleep(2)


# def make_orders(storage: str, order_list: list):

#     desired_value = storage

#     order_button = driver.find_element(By.ID, "addButtonT")
#     order_button.click()

#     time.sleep(2)

#     dropdown1 = driver.find_element(By.ID, "dropdownlistArrowclienteCombo")
#     dropdown1.click()

#     time.sleep(1)

#     actions.send_keys(Keys.ARROW_DOWN)
#     actions.send_keys(Keys.ENTER)
#     actions.perform()

#     time.sleep(1)

#     dropdown1 = driver.find_element(By.ID, "dropdownlistArrowmagazzini")
#     dropdown1.click()

#     time.sleep(1)

#     # Locate the input field using XPath
#     input_field = driver.find_element(
#         By.XPATH, '//*[@id="dropdownlistContentmagazzini"]/input')

#     while True:
#         actions.send_keys(Keys.ARROW_DOWN)
#         actions.perform()
#         time.sleep(1)
#         # Get the value of the 'value' attribute
#         # Arrivato alla fine della dropdown list non torna su, bisogna fixare forse, dipende da come vengono processati i file dalla cartella in cui sono salvate le liste
#         input_value = input_field.get_attribute("value")
#         if input_value == desired_value:
#             actions.send_keys(Keys.ENTER)
#             actions.perform()
#             break

#     time.sleep(1)

#     # Locate the button using its ID
#     confirm_button = driver.find_element(By.ID, "okModifica")

#     # Click the button
#     confirm_button.click()

#     time.sleep(3)

#     driver.switch_to.window(driver.window_handles[-1])  # Switch to the new tab

#     new_order_button = driver.find_element(By.ID, "addButtonT")
#     new_order_button.click()

#     time.sleep(1)

#     for element in order_list:
#         # Split the element by the dot character
#         parts = element.split('.')

#         # Assign each part to part1, part2, and part3
#         part1, part2, part3 = map(int, parts)  # Convert parts to integers

#         # if part1 == 32052:
#         # time.sleep(0.3) #TODO testing only DELETE
#         # Locate the parent div element by its ID
#         parent_div1 = driver.find_element(By.ID, "codArt")
#         parent_div2 = driver.find_element(By.ID, "varArt")

#         # Find the input element within the parent div
#         cod_art_field = parent_div1.find_element(By.TAG_NAME, "input")
#         var_art_field = parent_div2.find_element(By.TAG_NAME, "input")

#         # Clear the input field and insert the desired number
#         cod_art_field.clear()  # If you need to clear any existing value
#         var_art_field.clear()

#         cod_art_field.send_keys(part1)
#         var_art_field.send_keys(part2)

#         time.sleep(0.8)

#         parent_div3 = driver.find_element(By.ID, "w_Quantita")
#         stock_size = parent_div3.find_element(By.TAG_NAME, "input")
#         stock_size.clear()
#         stock_size.send_keys(part3)

#         time.sleep(0.5)

#         # Locate the button using its ID
#         confirm_button_order = driver.find_element(By.ID, "okModificaRiga")

#         # Click the button
#         confirm_button_order.click()

#         time.sleep(1)

#     # Switch to the previous tab
#     driver.switch_to.window(driver.window_handles[-2])


# for storage, order_list in zip(storage_list, orders_list):
#     if not TEST_MODE:
#         make_orders(storage, order_list)
#     else:
#         logger.info(f'We made orders')

# logger.info("This order consists of " + str(number_of_orders) + " packages")
# # Close the browser
# driver.quit()
