import pandas as pd

def check_column_uniqueness(csv_path, columns_to_check=None):
  
    if columns_to_check is None:
        columns_to_check = ['product_id', 'transaction_id', 'inventory_id', 'discount_id', 'customer_id']

    # Load CSV
    df = pd.read_csv(csv_path, dtype=str)  

    for col in columns_to_check:
        if col in df.columns:           
            clean_col = df[col].dropna().map(str).str.strip()
            clean_col = clean_col[clean_col != '']
            if clean_col.duplicated().any():
                print(f"Duplicate found in column: {col}")
                return False
        else:
            print(f"Column '{col}' not found in file. Skipped.")

    return True

# # Example usage
# if __name__ == "__main__":
#     filename = "test.csv"
#     result = check_column_uniqueness(filename)
#     print("All columns are unique:", result)
