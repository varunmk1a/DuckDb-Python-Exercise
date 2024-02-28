import duckdb
import pandas as pd

def display_data():
    print("\nAvailable columns:")
    print(df.columns.tolist())
    column_name = input("Enter the column name you want to analyze: ")

    # Check if the selected column is numeric
    if pd.api.types.is_numeric_dtype(df[column_name]):
        operation = input("Enter the operation (e.g., AVG, SUM): ")

        # Execute SQL query dynamically
        query = f"SELECT {operation}({column_name}) FROM parquet_data"
        result = con.execute(query)

        # Fetch and display the result
        try:
            result_df = result.fetchdf()
            print(result_df)
        except duckdb.DuckDBError as e:
            print(f"Error fetching result: {e}")
    else:
        print(f"Non-numeric column '{column_name}' selected.")

        # Additional logic for non-numeric columns (you can customize this part)
        unique_values = df[column_name].unique()
        print(f"Unique values in '{column_name}': {unique_values}")
        print(f"Count of occurrences in '{column_name}': {len(df[column_name])}")

def edit_data():
    print("\nCurrent DataFrame:")
    print(df)

    column_to_modify = input("Enter the column name to modify: ")

    # Ask for the specific value to modify in the selected column
    value_to_modify = input(f"Enter the value in '{column_to_modify}' to modify: ")

    # Check if the value exists in the selected column
    if value_to_modify in df[column_to_modify].values:
        new_value = input(f"Enter the new value for '{value_to_modify}': ")

        # Selectively modify the DataFrame based on the condition
        df.loc[df[column_to_modify] == value_to_modify, column_to_modify] = new_value

        # Update DuckDB with the modified DataFrame
        update_duckdb()

        print("\nModified DataFrame:")
        print(df)
    else:
        print(f"The value '{value_to_modify}' does not exist in column '{column_to_modify}'.")

def update_duckdb():
    # Unregister the old DataFrame if it exists
    if 'parquet_data' in con.list_tables():
        con.unregister('parquet_data')

    # Register the modified DataFrame
    con.register('parquet_data', df)
# Provide the local path to the Parquet file
parquet_file_path = r"D:\Training\DuckDb-Python-Exercise\Par\amdev1.parquet"

# Read Parquet data with Pandas from the provided file path
df = pd.read_parquet(parquet_file_path)

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
