import pandas as pd

# Replace with your actual GitHub URL and personal access token
github_url = "https://raw.githubusercontent.com/<your_username>/<your_repo>/master/userdata1.parquet"
personal_access_token = "<your_personal_access_token>"

# Access the file using pandas with authentication for GitHub
headers = {"Authorization": f"token {personal_access_token}"}
df = pd.read_parquet(github_url, headers=headers)

# Display column names
print("--- Available columns ---")
print(df.columns.to_list())

# Get user input for operations
while True:
    operation = input("Enter the operation you want to perform (e.g., sum, mean, count, etc.): ")
    column = input("Enter the column to perform the operation on: ")

    # Validate user input
    if column not in df.columns:
        print(f"Invalid column name '{column}'. Please try again.")
        continue

    try:
        # Perform the operation
        result = df[column].agg(operation)
        print("Result:", result)
        break  # Exit the loop after a valid operation

    except ValueError:
        print(f"Invalid operation '{operation}'. Please try again.")

# Get user input for data modification
modify = input("Would you like to modify the data? (y/n): ")
if modify.lower() == "y":
    modification_type = input("Enter modification type (modify entire row or column): ")
    modification_type = modification_type.lower()

    if modification_type == "row":
        # Modify entire row (replace with your specific logic)
        row_index = int(input("Enter the index of the row to modify: "))
        if 0 <= row_index < len(df):
            # Prompt user for new values and update the row
            new_values = []
            for col in df.columns:
                new_value = input(f"Enter new value for '{col}': ")
                new_values.append(new_value)
            df.iloc[row_index] = new_values
        else:
            print(f"Invalid row index '{row_index}'.")
    elif modification_type == "column":
        # Modify column (replace with your specific logic)
        column_to_modify = input("Enter the column to modify: ")
        if column_to_modify in df.columns:
            # Prompt user for new values and update the column
            new_values = []
            for i in range(len(df)):
                new_value = input(f"Enter new value for row {i + 1} of '{column_to_modify}': ")
                new_values.append(new_value)
            df[column_to_modify] = new_values
        else:
            print(f"Invalid column name '{column_to_modify}'.")
    else:
        print(f"Invalid modification type '{modification_type}'.")

    # You cannot directly save modified data back to GitHub using pandas within the script.
    print("Data modification complete. However, saving to GitHub is not possible within this script.")

print("Exiting...")