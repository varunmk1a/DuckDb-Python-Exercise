import duckdb

# Connect to DuckDB
conn = duckdb.connect(database=':memory:', read_only=False)

# Load the Parquet file into a DuckDB table
parquet_file = 'D:/Training/DuckDb-Python-Exercise/Par/train.pqt'
table_name = 'train_table'
conn.execute(f'CREATE TABLE {table_name} AS SELECT * FROM parquet_scan(\'{parquet_file}\')')

# Retrieve column names using system tables
column_names = conn.execute(f'SELECT column_name FROM information_schema.columns WHERE table_name = \'{table_name}\'').fetchall()
print("Columns in the Parquet file:")
for col in column_names:
    print(col[0])

# Now you can perform any additional operations like editing column names, updating values, etc.

# Edit column names
edit_columns = input("Do you want to edit column names? (y/n): ").lower()
if edit_columns == 'y':
    for i, col in enumerate(column_names):
        new_name = input(f"Enter new name for column '{col[0]}' (press Enter to keep the same): ")
        if new_name:
            conn.execute(f'ALTER TABLE {table_name} RENAME COLUMN {col[0]} TO {new_name}')

# Ask user which column and value they want to edit
edit_column = input("Which column do you want to edit?: ")
edit_value = input(f"Enter the new value for column '{edit_column}': ")

# Update the specified value in the chosen column
conn.execute(f'UPDATE {table_name} SET {edit_column} = \'{edit_value}\' WHERE 1=1')

# Save the changes back to the Parquet file
conn.execute(f'COPY (SELECT * FROM {table_name}) TO \'{parquet_file}\' (FORMAT PARQUET)')

# Close connection
conn.close()
