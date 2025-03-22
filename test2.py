from orderer import Orderer
orders_list = [ '122.1.2' ]
storage_list = ['23 S.PALOMBA SURGELATI']
orderer = Orderer()
orderer.login()
orderer.lists_combiner(storage_list, orders_list)