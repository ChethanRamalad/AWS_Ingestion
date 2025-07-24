import pandas as pd
from datetime import datetime

def ensure_audit_trail(csv_path, output_path, user='system', force_update=False, date_format='%Y-%m-%d %H:%M:%S'):
    df = pd.read_csv(csv_path, dtype=str) # load all columns as str for compatibility

    now_str = datetime.now().strftime(date_format)

    for col in ['created_at', 'updated_at', 'created_by']:
        if col not in df.columns:
            df[col] = ""  # create column if missing

    if force_update:
        df['created_at'] = now_str
        df['created_by'] = user
    else:
        df['created_at'] = df['created_at'].replace(['', None, 'nan', 'NaN'], pd.NA)
        df['created_by'] = df['created_by'].replace(['', None, 'nan', 'NaN'], pd.NA)
        df['created_at'] = df['created_at'].fillna(now_str)
        df['created_by'] = df['created_by'].fillna(user)

    df['updated_at'] = now_str

    df.to_csv(output_path, index=False)
    print(f"Audit trail fields ensured. Output written to '{output_path}'.")

# # Example usage:
# if __name__ == "__main__":
#     input_file = "test.csv"  # <-- Replace with your input CSV filename
#     output_file = "with_audit.csv"    # <-- Output file with audit fields
#     ensure_audit_trail(input_file, output_file, user='batch_user')
