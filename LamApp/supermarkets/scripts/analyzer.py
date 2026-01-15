import logging

# Use Django's logging system
logger = logging.getLogger(__name__)

class Analyzer:
    CLASS_NAME = "N"

    def __init__(self) -> None:
        self.reset_statistics()

    def get_original_list(self, original_list):
        self.df = original_list

    # ----------------------------
    # Recorders
    # ----------------------------
    def stat_recorder(self, qty: int, success: bool):
        self.number_of_packages += qty
        if success:
            self.success += 1
        else:
            self.fail += 1

    def low_sale_recorder(self, product_name: str, product_cod: int, product_var: int):
        self.low_list.append((product_name, product_cod, product_var))

    def brand_new_recorder(self, note: str):
        self.brand_new_list.append(note)

    def anomalous_stock_recorder(self, note: str):
        self.anomalous_stock_list.append(note)

    # ----------------------------
    # Helpers
    # ----------------------------
    @staticmethod
    def safe_div(numerator: float, denominator: float) -> float:
        """Safely divide two numbers and return percentage."""
        return (numerator / denominator * 100) if denominator > 0 else 0.0

    # ----------------------------
    # Logging
    # ----------------------------
    def log_statistics(self):
        total = self.success + self.fail

        logger.info(f"{self.CLASS_NAME} orders : {self.success}")
        logger.info(f"{self.CLASS_NAME} fails : {self.fail}")
        logger.info(
            f"{self.CLASS_NAME} class success rate = "
            f"{self.safe_div(self.success, total):.2f}%"
        )

        logger.info(f"Total packages : {self.number_of_packages}")
        logger.info(f"Total product types ordered : {self.success}")

        if self.brand_new_list:
            logger.info(
                "The following products are brand new or made available once more:\n"
                + "\n".join(self.brand_new_list)
            )

        if self.low_list:
            logger.info(
                "Very low daily sales products order list:\n"
                + "\n".join(", ".join(map(str, item)) for item in self.low_list)
            )

        if self.anomalous_stock_list:
            logger.info(
                "The following products have an anomalous negative stock oscillation:\n"
                + "\n".join(self.anomalous_stock_list)
            )

        # Reset statistics for next run
        self.reset_statistics()

    # ----------------------------
    # Reset
    # ----------------------------
    def reset_statistics(self):
        self.low_list = []
        self.brand_new_list = []
        self.anomalous_stock_list = []

        self.number_of_packages = 0
        self.success = 0
        self.fail = 0


analyzer = Analyzer()
