# data_cruncher.py
import pandas as pd

class DataCruncher:
    @staticmethod
    def merge(csv_files):
        # Initialize an empty list to store the dataframes
        dataframes = []

        # Loop through the list of CSV files
        for csv_file in csv_files:
            # Read each CSV file into a DataFrame and append it to the list
            dataframes.append(pd.read_csv(csv_file))

        # Concatenate all the dataframes into one
        merged_df = pd.concat(dataframes)

        # Save the merged dataframe to a new CSV file
        merged_df.to_csv('merged.csv', index=False)