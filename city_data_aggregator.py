import pandas as pd
import fastavro, os
import argparse

def list_files_in_directory(directory):
    """Generates a list of file paths in the given directory.

    Args:
        directory (str): The directory path.

    Returns:
        list: A list of file paths.
    """
    files = []
    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)
        if os.path.isfile(filepath):
            files.append(filepath)
    return files

def avro_dataframe(filename):
    """Read an avro file and convert it to DataFrame.

    Args:
    filename: A name of an avro file.

    Returns:
    A pandas DataFrame containing the data.
    """
    with open(filename, 'rb') as avro_file:
        reader = fastavro.reader(avro_file)
        city_list = [record for record in reader]
    df = pd.DataFrame(city_list)
    return df

def csv_dataframe(filename):
    """Read an csv file and convert it to DataFrame.

    Args:
    filename: A name of an csv file.

    Returns:
    A pandas DataFrame containing the data.
    """
    return pd.read_csv(filename)

def json_dataframe(filename):
    """Read a json file and convert it to DataFrame.

    Args:
    filename: A name of an json file.

    Returns:
    A pandas DataFrame containing the data.
    """  
    return pd.read_json(filename)

def combine_city_lists(filenames):
    """Combines multiple city lists into a single DataFrame.

    Args:
        filenames: A list of filenames.

    Returns:
        A pandas DataFrame containing the combined data.
    """
    dataframes = []
    for filename in filenames:
        if filename.endswith('.json'):
            df = json_dataframe(filename)
        elif filename.endswith('.avro'):
            df = avro_dataframe(filename)
        elif filename.endswith('.csv'):
            df = csv_dataframe(filename)
        else:
            raise ValueError("Unsupported file format: {}".format(filename))
        dataframes.append(df)

        combined_df = pd.concat(dataframes, ignore_index=True)

        #Note: This will remove the records that have the same values on all three columns (i.e. 'Name', 'CountryCode', and 'Population').
        #      If we want to ensure that there are not dupplicate city name and country code, we can narrow it down to only 'Name' and 'CountryCode'.
        #      For example: combined_df = combined_df.drop_duplicates(subset=['Name', 'CountryCode'])
        
        combined_df = combined_df.drop_duplicates(subset=['Name', 'CountryCode', 'Population'])
        combined_df = combined_df.sort_values(by='Name')

    return combined_df

if __name__ == '__main__':
    directory = 'DataSet'
    filenames = list_files_in_directory(directory)
    combined_df = combine_city_lists(filenames)

    # Write the combined data to a CSV file
    combined_df.to_csv('combined_cities.csv', index=False)

    # Answer the questions
    print(f"Total rows: {combined_df.shape[0]}")
    print(f"City with largest population: {combined_df.nlargest(1, 'Population')['Name'].values[0]} with {combined_df.nlargest(1, 'Population')['Population'].values[0]} in population")
    print(f"Total population of cities in Brazil: {combined_df[combined_df['CountryCode'] == 'BRA']['Population'].sum()}")