from gatherer import Gatherer
from orderer import Orderer


gatherer = Gatherer()
gatherer.gather_data()
storage_list = gatherer.storage_list
orders_list = gatherer.orders_list
gatherer.driver.quit()
orderer = Orderer()
orderer.login()
orderer.lists_combiner(storage_list, orders_list)

# Close the browser
orderer.driver.quit()
