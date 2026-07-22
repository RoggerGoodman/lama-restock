import logging

# Use Django's logging system
logger = logging.getLogger(__name__)

class Analyzer:
    CLASS_NAME = "N"

    def __init__(self) -> None:
        self.reset_statistics()

    # ----------------------------
    # Recorders
    # ----------------------------
    def stat_recorder(self, qty: int, success: bool, check: int = None):
        self.number_of_packages += qty
        if success:
            self.success += 1
        else:
            self.fail += 1
        if check is not None:
            self.check_counts[check] = self.check_counts.get(check, 0) + 1

    def low_sale_recorder(self, product_name: str, product_cod: int, product_var: int):
        self.low_list.append((product_name, product_cod, product_var))

    # ----------------------------
    # Logging
    # ----------------------------
    def log_statistics(self):
        logger.info(f"{self.CLASS_NAME} orders : {self.success}")
        logger.info(f"{self.CLASS_NAME} fails : {self.fail}")
        logger.info(
            f"{self.CLASS_NAME} order breakdown: "
            f"{self.CLASS_NAME}1(formula)={self.check_counts.get(1, 0)}, "
            f"{self.CLASS_NAME}2(forced-leftover)={self.check_counts.get(2, 0)}, "
            f"{self.CLASS_NAME}3(forced-low-stock)={self.check_counts.get(3, 0)}"
        )

        logger.info(f"Total packages : {self.number_of_packages}")
        logger.info(f"Total product types ordered : {self.success}")

        if self.low_list:
            logger.info(
                "Very low daily sales products order list:\n"
                + "\n".join(", ".join(map(str, item)) for item in self.low_list)
            )

        # Reset statistics for next run
        self.reset_statistics()

    # ----------------------------
    # Reset
    # ----------------------------
    def reset_statistics(self):
        self.low_list = []

        self.number_of_packages = 0
        self.success = 0
        self.fail = 0
        self.check_counts = {}


analyzer = Analyzer()
