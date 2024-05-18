import pandas as pd

# Cut off the first 3 rows and split into two dfs: balance + transactions
data = pd.read_excel("data/LoadDocstore.xlsx", sheet_name=1, skiprows=4, header=0)

# Split the data into two dataframes: balance + transactions based on first NaN row in Amount Col

# Find first blank row in Amount column
mask = pd.isna(data["Amount"])
tx = data.loc[: mask.idxmax() - 1]

# Check each row of df
# if value = 'Regular Deposit',

import_file_tx = tx.copy()


def extract_details(df, col):
    export_array = []
    total = len(df)
    print(f"There are {total} rows in the table")

    # Initialize counters
    deposits = interest_payments = account_fee_charges = share_purchases = (
        share_sales
    ) = withdrawals = unknown = 0

    for index, row in df.iterrows():
        if row[col] == "Regular Deposit":
            new_values = {
                "Date": row["Date"],
                "SharePadID": "",
                "Type": "credit",
                "Shares": "",
                "Price": "",
                "Broker": "",
                "Stamp": "",
                "Noncash": "",
                "Total": row["Amount"],
                "Note": row["Details"],
            }
            export_array.append(new_values)
            deposits += 1  # Increment counter
        elif row[col] == "Cash Account Interest":
            new_values = {
                "Date": row["Date"],
                "SharePadID": "",
                "Type": "credit",
                "Shares": "",
                "Price": "",
                "Broker": "",
                "Stamp": "",
                "Noncash": "",
                "Total": row["Amount"],
                "Note": row["Details"],
            }
            export_array.append(new_values)
            interest_payments += 1  # Increment counter
        elif row[col].startswith("Account Fee"):
            new_values = {
                "Date": row["Date"],
                "SharePadID": "",
                "Type": "debit",
                "Shares": "",
                "Price": "",
                "Broker": "",
                "Stamp": "",
                "Noncash": "",
                "Total": row["Amount"],
                "Note": row["Details"],
            }
            export_array.append(new_values)
            account_fee_charges += 1  # Increment counter
        elif row[col].startswith("Bought "):
            type = "buy"
            split_string = row[col].split(" ", 2)
            shares = float(split_string[1])  # Convert to float
            sharepad_id = split_string[2]
            price = abs(row["Amount"]) / shares  # Use the correct variable name
            new_values = {
                "Date": row["Date"],
                "SharePadID": sharepad_id,
                "Type": type,
                "Shares": shares,
                "Price": price,
                "Broker": "",
                "Stamp": "",
                "Noncash": "",
                "Total": row["Amount"],
                "Note": row["Details"],
            }
            export_array.append(new_values)
            share_purchases += 1  # Increment counter
        elif row[col].startswith("Sold "):
            type = "sell"
            split_string = row[col].split(" ", 2)
            shares = float(split_string[1])  # Convert to float
            sharepad_id = split_string[2]
            price = abs(row["Amount"]) / shares  # Use the correct variable name
            new_values = {
                "Date": row["Date"],
                "SharePadID": sharepad_id,
                "Type": type,
                "Shares": shares,
                "Price": price,
                "Broker": "",
                "Stamp": "",
                "Noncash": "",
                "Total": row["Amount"],
                "Note": row["Details"],
            }
            export_array.append(new_values)
            share_sales += 1  # Increment counter
        elif row[col].startswith("Payment by"):
            type = "debit"
            new_values = {
                "Date": row["Date"],
                "SharePadID": "",
                "Type": type,
                "Shares": "",
                "Price": "",
                "Broker": "",
                "Stamp": "",
                "Noncash": "",
                "Total": row["Amount"],
                "Note": row["Details"],
            }
            export_array.append(new_values)
            withdrawals += 1  # Increment counter
        elif "DIV:" in row[col]:
            continue
        else:
            print(f"Unknown transaction type: {row[col]}")
            unknown += 1  # Increment counter

    # Print the counts
    print(f"There were {deposits} deposits")
    print(f"There were {interest_payments} interest payments")
    print(f"There were {account_fee_charges} account fee charges")
    print(f"There were {share_purchases} share purchases")
    print(f"There were {share_sales} share sales")
    print(f"There were {withdrawals} withdrawals")

    if (
        len(export_array)
        == deposits
        + interest_payments
        + account_fee_charges
        + share_purchases
        + share_sales
        + withdrawals
    ):
        print("All transactions have been processed")
    else:
        print(f"There are {unknown} number of unknown transactions")

    # Convert the array to a DataFrame
    export_df = pd.DataFrame(export_array)
    # Export the DataFrame to a CSV file
    export_df.to_csv("export/export.csv", index=False)


# Call the function
export = extract_details(tx, "Details")
