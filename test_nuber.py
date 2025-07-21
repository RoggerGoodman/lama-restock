sold_a =[5, 21, 28, 25, 20, 14, 16, 11, 23, 20, 18, 9, 15, 30, 30, 40, 31, 20, 17]
bought_a = [0, 14, 42, 28, 14, 14, 14, 14, 28, 14, 14, 14, 14, 28, 42, 28, 42, 14, 28]
sold = sum(sold_a)
bought = sum(bought_a)
tot = sold - bought
print(tot)