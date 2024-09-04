import pandas as pd
from termcolor import colored

def analyze_city_data(csv_file):
    """Analyze city data from the combined CSV file.

    Args:
        csv_file (str): Path to the combined city CSV file.
    """
    df = pd.read_csv(csv_file)

    # Unique city names count
    unique_city_names = df['Name'].nunique()
    print(colored("Count of unique city names:", "green"), unique_city_names)
    
    # Top 2 countries with the most cities listed
    top_countries = df['CountryCode'].value_counts().head(2)
    print(colored("Top 2 countries with the most cities listed:", "green"))
    print(top_countries)

    # Average population size per city
    average_population = df['Population'].mean()
    print(colored("Average population size per city:", "green"), f"{average_population:.2f}")

    # Number of cities with population greater than 1 million
    cities_above_million = df[df['Population'] > 1000000].shape[0]
    print(colored("Number of cities with population greater than 1 million:", "green"), cities_above_million)

    # Smallest city by population and its country
    smallest_city = df.nsmallest(1, 'Population')
    print(colored("Smallest city by population:", "green"), f"{smallest_city['Name'].values[0]} in {smallest_city['CountryCode'].values[0]} with {smallest_city['Population'].values[0]} population")

    # Cities with duplicate names across different countries
    duplicate_city_names = df[df.duplicated(subset='Name', keep=False)].sort_values('Name')
    print(colored("Cities with duplicate names across different countries:", "green"))
    print(duplicate_city_names[['Name', 'CountryCode', 'Population']])

if __name__ == '__main__':
    csv_file = 'combined_cities.csv'
    analyze_city_data(csv_file)
