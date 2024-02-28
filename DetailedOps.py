import duckdb
import pandas as pd
import requests
from io import BytesIO


# Function to download Parquet file from URL with error handling
def download_parquet(url):
    response = requests.get(url)

    if response.status_code == 200:
        try:
            return pd.read_parquet(BytesIO(response.content))
        except FileNotFoundError:
            print(f"File not found at {url}.")
            return None
        except pd.errors.ParserError:
            print(f"Error parsing Parquet file at {url}.")
            return None
    else:
        print(f"Failed to download file from {url}. Status code: {response.status_code}")
        return None


# Function to display the first few lines of data
def display_data():
    print("\nFirst few lines of data:")
    print(df.head())


# Function to edit data in DataFrame
def edit_data():
    print("\nCurrent DataFrame:")
    print(df)

    edit_choice = input("Enter 'column' to edit a column name or 'data' to modify specific data: ")

    if edit_choice.lower() == 'column':
        column_to_modify = input("Enter the column name to modify: ")
        new_column_name = input(f"Enter the new name for column '{column_to_modify}': ")

        df.rename(columns={column_to_modify: new_column_name}, inplace=True)
    elif edit_choice.lower() == 'data':
        column_to_modify = input("Enter the column name containing the data to modify: ")
        value_to_modify = input(f"Enter the value to change in '{column_to_modify}': ")
        new_value = input(f"Enter the new value for '{value_to_modify}': ")

        df.loc[df[column_to_modify] == value_to_modify, column_to_modify] = new_value

    update_duckdb()

    print("\nModified DataFrame:")
    print(df)


# Function to perform calculations on data
def perform_calculations():
    print("\nAvailable columns:")
    print(df.columns.tolist())

    choice = input("Enter 'row' or 'column' for operation on rows or columns: ")

    if choice.lower() == 'row':
        row_index = int(input("Enter the row index (starting from 0): "))
        print(df.iloc[row_index])
    elif choice.lower() == 'column':
        column_name = input("Enter the column name for operation: ")
        operation = input("Enter operation (add, average, median): ")

        if operation == 'add':
            value_to_add = float(input("Enter the value to add: "))
            df[column_name] = df[column_name] + value_to_add
        elif operation == 'average':
            result = df[column_name].mean()
            print(f"Average of '{column_name}': {result}")
        elif operation == 'median':
            result = df[column_name].median()
            print(f"Median of '{column_name}': {result}")
        else:
            print("Invalid operation.")

    update_duckdb()

    if operation != 'add':
        print(f"{operation.capitalize()} result: {result}")


# Function to update DuckDB with the modified DataFrame (corrected line)
def update_duckdb():
    # Unregister the old DataFrame if it exists
    if 'parquet_data' in con.list_tables():
        con.unregister('parquet_data')

    # Register the modified DataFrame (explicitly providing index, columns, values)
    con.register("parquet_data", df.index.tolist(), df.columns.tolist(), df.values)


# Download the Parquet file (enhanced error handling)
parquet_file_url = "https://github.com/varunmk1a/DuckDBsample/raw/main/amdev1.parquet"
df = download_parquet(parquet_file_url)

# Check if download was successful (skip further steps if not)
if df.empty:
    print("Exiting due to download error.")
    exit()

# Create a DuckDB connection
con = duckdb.connect(database=':memory:', read_only=False)

con.register('parquet_data', df)

while True:
    print("\nOptions:")
    print("1. View Data")
    print("2. Edit Data")
    print("3. Perform Calculations")
    print("4. Exit")

    choice = input("Enter your choice (1/2/3/4): ")

    if choice == '1':
        display_data()
    elif choice == '2':
        edit_data()
    elif choice == '3':
        perform_calculations()
    elif choice == '4':
        print("Exiting the program.")
        break
    else:
        print("Invalid choice. Please enter 1, 2, 3, or 4.")

# Close the connection to DuckDB before exiting the program
con.close()
