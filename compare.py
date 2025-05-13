import pandas as pd

def compare_csv(human_order_path, autom_order_path, output_file="comparison_result.xlsx"):
    # Load CSV files
    df1 = pd.read_csv(human_order_path, dtype=str)
    df2 = pd.read_csv(autom_order_path, dtype=str)

    # Select relevant columns and clean column names
    columns_to_keep = ["Cod.", "Diff.", "N.imb."]
    df1 = df1.rename(columns=lambda x: x.strip())
    df2 = df2.rename(columns=lambda x: x.strip())

    # Filter only the required columns and drop empty values
    df1_filtered = df1[columns_to_keep].dropna()
    df2_filtered = df2[columns_to_keep].dropna()

    # Merge based on 'Cod.' and 'Diff.' to find matches
    merged_df = df1_filtered.merge(df2_filtered, on=["Cod.", "Diff."], suffixes=("_human", "_auto"))

    # Convert "N.imb." to numeric for comparison
    merged_df["N.imb._human"] = merged_df["N.imb._human"].str.replace(",", ".").astype(float)
    merged_df["N.imb._auto"] = merged_df["N.imb._auto"].str.replace(",", ".").astype(float)

    # Calculate differences
    merged_df["Difference"] = merged_df["N.imb._auto"] - merged_df["N.imb._human"]

    # Categorize changes
    def highlight_difference(value):
        if value > 0:
            return "Increase"
        elif value < 0:
            return "Decrease"
        return "Same"

    merged_df["Change Type"] = merged_df["Difference"].apply(highlight_difference)

    # Find items missing from each list
    human_only = df1_filtered[~df1_filtered.set_index(["Cod.", "Diff."]).index.isin(df2_filtered.set_index(["Cod.", "Diff."]).index)]
    auto_only = df2_filtered[~df2_filtered.set_index(["Cod.", "Diff."]).index.isin(df1_filtered.set_index(["Cod.", "Diff."]).index)]

    # Save results to an Excel file with two sheets
    with pd.ExcelWriter(output_file) as writer:
        merged_df.to_excel(writer, sheet_name="Comparison", index=False)
        human_only.to_excel(writer, sheet_name="Only_in_Human_List", index=False)
        auto_only.to_excel(writer, sheet_name="Only_in_Auto_List", index=False)

    print(f"Comparison completed. Results saved in '{output_file}'.")

# Example usage:
human_order_path = "./Orders/h.csv"
autom_order_path = "./Orders/a.csv"
compare_csv(human_order_path, autom_order_path)
