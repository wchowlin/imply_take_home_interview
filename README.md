# Take Home Exercise - City Data Aggregation and Analysis

## Overview

This project is a coding exercise that combines city data from three different file formats—JSON, AVRO, and CSV—into a single, consolidated CSV file. The task involves removing any duplicate entries, sorting the data alphabetically by city name, and answering specific questions about the dataset.

## Project Structure

- `DataSet/` - Directory containing the three source files:
  - `CityListA.json` - JSON file with city data
  - `CityListB.avro` - AVRO file with city data
  - `CityListC.csv` - CSV file with city data
- `city_data_aggregator.py` - Python script that processes the data files, combines them, and answers the specified questions.
- `combined_cities.csv` - Output CSV file containing the combined and sorted city data.

## Prerequisites

- Python 3.7+
- Required Python packages:
  - `pandas`
  - `fastavro`

You can install the required packages using pip:


pip install pandas fastavro

## Usage

1. **Place Your Data Files**  
   Ensure that your data files (`CityListA.json`, `CityListB.avro`, `CityListC.csv`) are located in the `DataSet` directory.

2. **Run the Script**  
   Execute the `city_data_aggregator.py` script to process the data:

   python city_data_aggregator.py

3. **Generated Output**  
   The script will create a file named `combined_cities.csv` containing the combined and deduplicated city records, sorted alphabetically by city name. Additionally, the script will print the answers to the following questions in the console:

   - **Total Rows**: The total number of rows in the combined dataset.
   - **City with the Largest Population**: The city with the highest population.
   - **Total Population in Brazil**: The sum of populations of all cities in Brazil (where `CountryCode == 'BRA'`).

## Output

- **`combined_cities.csv`**: A CSV file that includes all unique city records, organized alphabetically by city name.

## Questions Answered

- **Total Rows**: The total number of rows in the combined dataset.
- **City with the Largest Population**: The city with the highest population.
- **Total Population in Brazil**: The total population of all cities in Brazil (`CountryCode == 'BRA'`).

## Improvements and Scalability

### Performance Improvements

- **Optimize I/O Operations**: Implement chunk-based reading for large files to minimize memory usage.
- **Parallel Processing**: Leverage multiprocessing to load and process files concurrently.

### Scalability for Larger Datasets

- **Distributed Computing**: Use frameworks like Apache Spark or Dask for distributed processing of large datasets.
- **Data Partitioning**: Partition the dataset by city or country, allowing chunks of data to be processed in parallel across multiple machines.
- **Cloud Storage**: Store data in cloud-based solutions such as Amazon S3 or Google Cloud Storage, and use serverless functions or cloud-based data processing services to handle the data.
