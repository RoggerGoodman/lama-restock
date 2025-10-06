import math
avg_daily_sales = 0.42
final_array_sold =   [19, 8, 13, 18, 17, 10, 19, 11, 18, 16, 18, 24, 20, 12, 26, 11, 30, 20, 19, 20, 14]
final_array_bought = [20, 10, 10, 20, 10, 20, 10, 10, 20, 10, 20, 30, 10, 20, 30, 10, 30, 10, 10, 40, 10]

def gap_balance(final_array_sold, final_array_bought, avg_daily_sales):

        gaps = [b - s for b, s in zip(final_array_bought, final_array_sold)]
        max_abs_gap = max(abs(g) for g in gaps[1:23])          
        candidate_indexes = [i for i, g in enumerate(gaps[1:23], start=1) if abs(g) == max_abs_gap]
        print(f"indexs = {candidate_indexes}")

        best_stock = 0

        for idx in candidate_indexes:
                signed_gap = gaps[idx]
                end = idx + 1 if signed_gap > 0 else idx

                Stot = sum(final_array_sold[:end])
                Btot = sum(final_array_bought[:end])
                stock = Btot - Stot
                if stock > best_stock:
                        best_stock = stock

        best_stock -= math.ceil(avg_daily_sales)
        print(f"Biggest gap Stock = {best_stock}")
        return best_stock

stock = gap_balance(final_array_sold, final_array_bought, avg_daily_sales)