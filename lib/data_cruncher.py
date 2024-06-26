"""
Utility module for merging multiple CSV files into one.
"""
import pandas as pd

class DataCruncher:
    """
    DataCruncher is a class that provides a method to merge multiple CSV files into one.
    """

    # pylint: disable=too-few-public-methods
    def __init__(self):
        pass

    @staticmethod
    def merge(csv_files: list):
        """
        Merge multiple CSV files into one.
        :param csv_files: A list of CSV file paths.
        """
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
