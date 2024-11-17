from logger import logger
import pandas as pd

class Analyzer:

    def __init__(self) -> None:
        self.reset_statistics()

    def stat_recorder(self, qty, category_name):
        self.number_of_packages += qty
        setattr(self, category_name, getattr(self, category_name) + 1)

    def note_recorder(self, note):
        self.notes_list.append(note)

    def news_recorder(self, note):
        self.news_list.append(note)

    def log_statistics(self):
        """Logs the statistics to a predefined logger and resets them."""
        logger.info(f"A orders : {self.A_success}")
        logger.info(f"A fails : {self.A_fail}")
        logger.info(f"B orders : {self.B_success}")
        logger.info(f"B fails : {self.B_fail}")
        logger.info(f"C orders : {self.C_success}")
        logger.info(f"C fails : {self.C_fail}")
        logger.info(f"Total packages : {self.number_of_packages}")
        self.number_of_products = (
            self.A_success + self.B_success + self.C_success
        )
        logger.info(f"Total products types ordered : {self.number_of_products}")
        logger.info("New or never bought products list:\n" + "\n".join(self.news_list))
        logger.info("Very low daily sales products order list:\n" + "\n".join(self.notes_list))
        
        
        
        dataframe = pd.DataFrame()
        # Reset statistics for the next use
        self.reset_statistics()

    def reset_statistics(self):
        """Resets all statistical fields to their initial values."""
        self.notes_list = [] #is it right?
        self.news_list = []
        self.number_of_packages = 0
        self.number_of_products = 0
        self.A_success = 0
        self.A_fail = 0
        self.B_success = 0
        self.B_fail = 0
        self.C_success = 0
        self.C_fail = 0