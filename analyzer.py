from logger import logger
import pandas as pd
from consts import COLLUMN1_NAME, COLLUMN2_NAME

class Analyzer:

    def __init__(self) -> None:
        self.reset_statistics()
    
    def get_original_list(self, original_list):
        self.df = original_list 

    def stat_recorder(self, qty, category_name):
        self.number_of_packages += qty
        setattr(self, category_name, getattr(self, category_name) + 1)

    def note_recorder(self, product_name: str, product_cod: int, product_var: int):
        self.notes_list.append((product_name, product_cod, product_var))

    def new_entry_recorder(self, note):
        self.new_entry_list.append(note)

    def brand_new_recorder(self, note):
        self.brand_new_list.append(note)

    def filter_notes(self):
        # Convert notes_list into a dictionary for faster lookup and removal
        notes_dict = {(note[1], note[2]): note for note in self.notes_list}

        filtered_list = []
        for _, row in self.df.iterrows():
            key = (row[COLLUMN1_NAME], row[COLLUMN2_NAME])
            if key in notes_dict:
                filtered_list.append(notes_dict.pop(key))  # Append and remove to shrink search space

        return filtered_list

    def log_statistics(self):
        """Logs the statistics to a predefined logger and resets them."""
        totA = self.A_success + self.A_fail
        totB = self.B_success + self.B_fail
        totC = self.C_success + self.C_fail
        total = totA + totB + totC
        logger.info(f"A orders : {self.A_success}")
        logger.info(f"A fails : {self.A_fail}")
        logger.info(f"A class percentage = {(totA/total)*100:.2f}%")
        logger.info(f"A class success rate = {(self.A_success/totA)*100:.2f}%")
        logger.info(f"B orders : {self.B_success}")
        logger.info(f"B fails : {self.B_fail}")
        logger.info(f"B class percentage = {(totB/total)*100:.2f}%")
        logger.info(f"B class success rate = {(self.B_success/totB)*100:.2f}%")
        logger.info(f"C orders : {self.C_success}")
        logger.info(f"C fails : {self.C_fail}")
        logger.info(f"C class percentage = {(totC/total)*100:.2f}%")
        logger.info(f"C class success rate = {(self.C_success/totC)*100:.2f}%")
        logger.info(f"Total packages : {self.number_of_packages}")
        totalSuccess = (self.A_success + self.B_success + self.C_success)
        logger.info(f"Total products types ordered : {totalSuccess}")
        self.low_list = self.filter_notes()
        logger.info("The following products are not being processed by the program because they have been in the system for too little:\n" + "\n".join(self.new_entry_list))
        logger.info("The following products are brand new or made available once more:\n" + "\n".join(self.brand_new_list))
        logger.info("Very low daily sales products order list:\n" + "\n".join([", ".join(map(str, item)) for item in self.low_list]))

     
        dataframe = pd.DataFrame()
        # Reset statistics for the next use
        self.reset_statistics()

    def reset_statistics(self):
        """Resets all statistical fields to their initial values."""
        self.notes_list = []
        self.low_list = []
        self.new_entry_list = []
        self.brand_new_list = []
        self.number_of_packages = 0
        self.number_of_products = 0
        self.A_success = 0
        self.A_fail = 0
        self.B_success = 0
        self.B_fail = 0
        self.C_success = 0
        self.C_fail = 0

analyzer = Analyzer()