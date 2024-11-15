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

    def log_statistics(self):
        """Logs the statistics to a predefined logger and resets them."""
        logger.info(f"High orders : {self.high_success}")
        logger.info(f"High fails : {self.high_fail}")
        logger.info(f"Medium orders : {self.mid_success}")
        logger.info(f"Medium fails : {self.mid_fail}")
        logger.info(f"Low orders : {self.low_success}")
        logger.info(f"Low fails : {self.low_fail}")
        logger.info(f"New products orders : {self.new_article_success}")
        logger.info(f"New products fails : {self.new_article_fail}")
        logger.info(f"Total packages : {self.number_of_packages}")
        logger.info("Vert low daily sales products list:\n" + "\n".join(self.notes_list))
        
        self.number_of_products = (
            self.high_success + self.mid_success + self.low_success + self.new_article_success
        )
        logger.info(f"Total products types ordered : {self.number_of_products}")

        dataframe = pd.DataFrame()
        # Reset statistics for the next use
        self.reset_statistics()

    def reset_statistics(self):
        """Resets all statistical fields to their initial values."""
        self.notes_list = [] #is it right?
        self.number_of_packages = 0
        self.number_of_products = 0
        self.high_success = 0
        self.high_fail = 0
        self.mid_success = 0
        self.mid_fail = 0
        self.low_success = 0
        self.low_fail = 0
        self.new_article_success = 0
        self.new_article_fail = 0