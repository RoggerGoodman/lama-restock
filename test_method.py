import math
avg_daily_sales = 2.32
final_array_sold = [38, 55, 43, 27, 16, 15, 11, 5, 5, 12, 32, 68, 63, 44, 42, 20, 18, 6, 5]
final_array_bought = [30, 66, 42, 24, 12, 12, 12, 6, 6, 6, 24, 72, 54, 60, 48, 24, 12, 12, 6]

def gap_balance(final_array_sold, final_array_bought, avg_daily_sales):

        gaps = [b - s for b, s in zip(final_array_bought, final_array_sold)]
        max_abs_gap = max(abs(g) for g in gaps[1:12])          
        candidate_indexes = [i for i, g in enumerate(gaps[1:12], start=1) if abs(g) == max_abs_gap]
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