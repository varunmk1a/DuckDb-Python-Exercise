import duckdb

# Connect to DuckDB
conn = duckdb.connect(database=':memory:', read_only=False)

# Load the Parquet file into a DuckDB table
parquet_file = 'D:/Training/DuckDb-Python-Exercise/Par/train.pqt'
table_name = 'train_table'
conn.execute(f'CREATE TABLE {table_name} AS SELECT * FROM parquet_scan(\'{parquet_file}\')')

# Retrieve column names using system tables, limit to first 10 columns
column_names = conn.execute(f'SELECT column_name FROM information_schema.columns WHERE table_name = \'{table_name}\' LIMIT 10').fetchall()
print("First 10 Columns in the Parquet file:")
for col in column_names:
    print(col[0])

# Ask user what they want to do
option = input("\nWhat would you like to do? (1: Edit column names, 2: Calculate average/sum of a column, 3: Modify data): ")

# Edit column names
if option == '1':
    edit_columns = input("Do you want to edit column names? (y/n): ").lower()
    if edit_columns == 'y':
        for i, col in enumerate(column_names):
            new_name = input(f"Enter new name for column '{col[0]}' (press Enter to keep the same): ")
            if new_name:
                conn.execute(f'ALTER TABLE {table_name} RENAME COLUMN {col[0]} TO {new_name}')

# Calculate average/sum of a column
elif option == '2':
    column_to_calculate = input("Enter the name of the column to calculate average/sum: ")
    operation = input("Enter the operation (1: Average, 2: Sum): ")
    result = conn.execute(f'SELECT {operation}(CAST({column_to_calculate} AS FLOAT)) FROM {table_name}').fetchall()
    print(f"The {operation.lower()} of '{column_to_calculate}' is: {result[0][0]}")

# Modify data
elif option == '3':
    modify_column = input("Enter the name of the column to modify: ")
    old_value = input("Enter the old value you want to modify: ")
    new_value = input("Enter the new value: ")
    conn.execute(f'UPDATE {table_name} SET {modify_column} = \'{new_value}\' WHERE {modify_column} = \'{old_value}\'')
    print("Data modified successfully.")

else:
    print("Invalid option!")

# Save the changes back to the Parquet file
conn.execute(f'COPY (SELECT * FROM {table_name}) TO \'{parquet_file}\' (FORMAT PARQUET)')

# Close connection
conn.close()
