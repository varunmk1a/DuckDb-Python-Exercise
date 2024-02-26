import duckdb
import pandas as pd

def display_data():
    print("\nAvailable columns:")
    print(df.columns.tolist())
    column_name = input("Enter the column name you want to analyze: ")
    operation = input("Enter the operation (e.g., AVG, SUM): ")

    # Execute SQL query dynamically
    query = f"SELECT {operation}({column_name}) FROM parquet_data"
    result = con.execute(query)

    # Fetch and display the result
    result_df = result.fetchdf()
    print(result_df)

def edit_data():
    print("\nCurrent DataFrame:")
    print(df)

    column_to_modify = input("Enter the column name to modify: ")
    new_value = input("Enter the new value: ")

    # Modify the DataFrame
    df[column_to_modify] = new_value

    # Update DuckDB with the modified DataFrame
    con.unregister('parquet_data')  # Unregister the old DataFrame
    con.register('parquet_data', df)  # Register the modified DataFrame

    print("\nModified DataFrame:")
    print(df)

# Read Parquet data with Pandas
parquet_file = input("Enter the path to your Parquet file: ")
df = pd.read_parquet(parquet_file)

# Create a DuckDB connection
con = duckdb.connect(database=':memory:', read_only=False)

# Store the Pandas DataFrame into DuckDB
con.register('parquet_data', df)

while True:
    print("\nOptions:")
    print("1. View Data")
    print("2. Edit Data")
    print("3. Exit")

    choice = input("Enter your choice (1/2/3): ")

    if choice == '1':
        display_data()
    elif choice == '2':
        edit_data()
    elif choice == '3':
        print("Exiting the program.")
        break
    else:
        print("Invalid choice. Please enter 1, 2, or 3.")
