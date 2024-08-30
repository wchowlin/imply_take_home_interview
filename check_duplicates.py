import pandas as pd

def check_name_duplicates(csv_file):
    """Checks for duplicate values in the 'Name' column of a CSV file.

    Args:
        csv_file (str): The path to the CSV file.

    Returns:
        None
    """
    # Load the CSV file into a DataFrame
    df = pd.read_csv(csv_file)
    
    # Check if 'Name' column exists
    if 'Name' not in df.columns:
        print("The 'Name' column is not present in the dataset.")
        return

    # Find duplicate values in the 'Name' column
    name_duplicates = df[df.duplicated(subset=['Name'], keep=False)]
    
    if not name_duplicates.empty:
        print(f"Found {name_duplicates['Name'].nunique()} unique duplicate names.")
        print("Duplicate names:")
        print(name_duplicates[['Name']].drop_duplicates())
    else:
        print("No duplicate names found.")

# Path to your CSV file
csv_file_path = 'combined_cities.csv'

# Check for duplicates in the 'Name' column
check_name_duplicates(csv_file_path)
