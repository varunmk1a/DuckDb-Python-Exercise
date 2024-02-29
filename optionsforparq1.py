import pyarrow.parquet as pq

#Block for options to view/edit and remove data
def perform_operations(data_frame):
    while True:
        print("\nOptions:")
        print("1. View column names")
        print("2. View data inside a specific column")
        print("3. Calculate sum of a column")
        print("4. Calculate average of a column")
        print("5. Edit a value in a column")
        print("6. Remove an entire column")
        print("7. View first 50 values in a column")
        print("8. Exit")

        choice = input("Enter your choice (1-8): ")

        if choice == '1':
            print("Columns in the DataFrame:")
            print(data_frame.columns)
        elif choice == '2':
            column_name = input("Enter the column name to view data: ")
            if column_name in data_frame.columns:
                print(data_frame[column_name])
            else:
                print(f"Column '{column_name}' not found in the DataFrame.")
        elif choice == '3':
            column_name = input("Enter the column name to calculate sum: ")
            if column_name in data_frame.columns:
                column_sum = data_frame[column_name].sum()
                print(f"Sum of '{column_name}': {column_sum}")
            else:
                print(f"Column '{column_name}' not found in the DataFrame.")
        elif choice == '4':
            column_name = input("Enter the column name to calculate average: ")
            if column_name in data_frame.columns:
                column_average = data_frame[column_name].mean()
                print(f"Average of '{column_name}': {column_average}")
            else:
                print(f"Column '{column_name}' not found in the DataFrame.")
        elif choice == '5':
            column_name = input("Enter the column name to edit a value: ")
            if column_name in data_frame.columns:
                value_to_edit = input(f"Enter the value to edit in '{column_name}': ")
                new_value = input(f"Enter the new value for '{column_name}': ")
                data_frame.loc[data_frame[column_name] == value_to_edit, column_name] = new_value
                print(f"Value '{value_to_edit}' replaced with '{new_value}' in '{column_name}'.")
            else:
                print(f"Column '{column_name}' not found in the DataFrame.")
        elif choice == '6':
            column_name = input("Enter the column name to remove: ")
            if column_name in data_frame.columns:
                data_frame = data_frame.drop(columns=[column_name])
                print(f"Column '{column_name}' removed from the DataFrame.")
            else:
                print(f"Column '{column_name}' not found in the DataFrame.")
        elif choice == '7':
            column_name = input("Enter the column name to view first 50 values: ")
            if column_name in data_frame.columns:
                print(data_frame[column_name].head(50))
            else:
                print(f"Column '{column_name}' not found in the DataFrame.")
        elif choice == '8':
            print("Exiting the program.")
            break
        else:
            print("Invalid choice. Please enter a number between 1 and 8.")

# Parquet file location
parquet_file_path = 'D:\Training\DuckDb-Python-Exercise\Par\mtrain.pqt'

# Pandas dataframe
df = pq.read_table(parquet_file_path).to_pandas()

# Call the f*
perform_operations(df)
