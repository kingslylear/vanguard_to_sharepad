import pandas as pd

SHEET_NAME = 1
SKIP_ROWS = 4
HEADER = 0
DATA_PATH = "data/LoadDocstore.xlsx"
EXPORT_PATH = "export/merged_df.csv"

def read_data():
    return pd.read_excel(DATA_PATH, sheet_name=SHEET_NAME, skiprows=SKIP_ROWS, header=HEADER)

def split_data(data):
    mask = pd.isna(data["Amount"])
    tx = data.loc[: mask.idxmax() - 1]
    inv_tx = data.loc[mask.idxmax() :]
    mask = data["Date"].eq("Date")
    inv_tx = inv_tx.loc[mask.idxmax() :]
    return tx, inv_tx

def clean_inv_tx(inv_tx):
    inv_tx.rename(columns=inv_tx.iloc[0], inplace=True)
    inv_tx.drop(inv_tx.index[0], inplace=True)
    inv_tx.drop(inv_tx.index[-1], inplace=True)
    return inv_tx

def rename_columns(import_file_inv_tx):
    column_mapping = {
        "InvestmentName": "SharepadID",
        "Quantity": "Shares",
        "Cost": "Total",
        "TransactionDetails": "Note",
    }
    import_file_inv_tx.rename(columns=column_mapping, inplace=True)
    return import_file_inv_tx

def process_type_column(import_file_inv_tx):
    import_file_inv_tx["Type"] = [
        "buy" if x.startswith("Bought") else "sell" if x.startswith("Sold") else x
        for x in import_file_inv_tx["TransactionDetails"]
    ]
    import_file_inv_tx.rename(columns={"TransactionDetails": "Note"}, inplace=True)
    return import_file_inv_tx

def clean_tx(tx):
    tx = tx[~tx["Details"].str.startswith(("Bought", "Sold"))]
    tx = tx[~tx["Details"].str.contains("DIV:")]
    tx["Note"] = tx["Details"]
    return tx

def process_details_column(import_file_tx):
    import_file_tx.loc[import_file_tx["Note"] == "Regular Deposit", "Details"] = "credit"
    import_file_tx.loc[import_file_tx["Note"].str.startswith("Account Fee"), "Details"] = "debit"
    import_file_tx.loc[import_file_tx["Note"].str.startswith("Cash Account Interest"), "Details"] = "credit"
    import_file_tx.loc[import_file_tx["Note"].str.contains("One-off withdrawal"), "Details"] = "debit"
    return import_file_tx

def rename_and_remove_cols(import_file_tx):
    import_file_tx.rename(columns={"Details": "Type", "Amount": "Total"}, inplace=True)
    import_file_tx = import_file_tx[["Date", "Type", "Total"]]
    return import_file_tx

def merge_dataframes(import_file_tx, import_file_inv_tx):
    merged_df = pd.concat([import_file_tx, import_file_inv_tx], ignore_index=True)
    merged_df["charges"] = 0
    merged_df["stamp"] = 0
    return merged_df

def export_data(merged_df):
    merged_df.to_csv(EXPORT_PATH, index=False)

def main():
    data = read_data()
    tx, inv_tx = split_data(data)
    inv_tx = clean_inv_tx(inv_tx)
    import_file_inv_tx = process_type_column(inv_tx.copy())
    import_file_inv_tx = rename_columns(import_file_inv_tx)
    tx = clean_tx(tx.copy())
    import_file_tx = process_details_column(tx)
    import_file_tx = rename_and_remove_cols(import_file_tx)
    merged_df = merge_dataframes(import_file_tx, import_file_inv_tx)
    export_data(merged_df)

if __name__ == "__main__":
    main()