from orderer import Orderer
orders_list = [[("GRAN SUGO ALLE COZZE  350G", 2), ("PISELLI FINISSIMI BUSTA 450G", 3)]]
storage_list = ['S.PALOMBA SURGELATI']
orderer = Orderer()
orderer.login()
orderer.lists_combiner(storage_list, orders_list)