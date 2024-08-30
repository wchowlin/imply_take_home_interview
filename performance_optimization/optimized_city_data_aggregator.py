import pandas as pd
import fastavro
import os
from multiprocessing import Pool

def list_files_in_directory(directory):
    """Generates a list of file paths in the given directory."""
    files = []
    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)
        if os.path.isfile(filepath):
            files.append(filepath)
    return files

def avro_dataframe(filename):
    """Read an avro file and convert it to DataFrame."""
    with open(filename, 'rb') as avro_file:
        reader = fastavro.reader(avro_file)
        city_list = [record for record in reader]
    return pd.DataFrame(city_list)

def process_file(filename):
    """Reads a file and returns a DataFrame."""
    if filename.endswith('.json'):
        return pd.read_json(filename)
    elif filename.endswith('.avro'):
        return avro_dataframe(filename)
    elif filename.endswith('.csv'):
        chunks = pd.read_csv(filename, chunksize=1000)  # Adjust chunksize as needed to reduce memory usage (read)
        return pd.concat(chunks, ignore_index=True)
    else:
        raise ValueError("Unsupported file format: {}".format(filename))

def combine_city_lists(filenames):
    """Combines multiple city lists into a single DataFrame."""
    with Pool() as pool:
        dataframes = pool.map(process_file, filenames)

    combined_df = pd.concat(dataframes, ignore_index=True)
    combined_df = combined_df.drop_duplicates(subset=['Name', 'CountryCode'])
    combined_df = combined_df.sort_values(by='Name')

    return combined_df

if __name__ == '__main__':
    directory = '../DataSet'
    filenames = list_files_in_directory(directory)
    combined_df = combine_city_lists(filenames)

    # Write the combined data to a CSV file
    combined_df.to_csv('combined_cities.csv', index=False)

    # Answer the questions
    print("Total rows:", combined_df.shape[0])
    print("City with largest population:", combined_df.nlargest(1, 'Population')['Name'].values[0])
    print("Total population of cities in Brazil:", combined_df[combined_df['CountryCode'] == 'BRA']['Population'].sum())
