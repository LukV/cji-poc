from rapidfuzz import fuzz, process
import pandas as pd
import json
import re

class ETLPipeline:
    def __init__(self, input_csv, mapping_json):
        self.input_csv = input_csv
        self.mapping_json = mapping_json
        self.data = None
        self.mapping = None

    def _load_mappings(self):
        """Load mappings from a JSON file."""
        with open(self.mapping_json, 'r') as file:
            self.mapping = json.load(file)

    def load_data(self) -> int:
        """Load data from the CSV file."""
        self.data = pd.read_csv(self.input_csv)
        return len(self.data)

    def filter_udb_location_type(self) -> int:
        """Filter out records based on UdbLocationID mapping."""
        # Ensure the mapping is loaded
        if self.mapping is None:
            self._load_mappings()
        # Filter logic
        self.data = self.data[self.data['udbLocationType'].isin(self.mapping.keys()) | self.data['udbLocationType'].isna()]

        return len(self.data)

    def fill_empty_location_type(self):
        """"Update the locationType column based on the mapping."""
        # Apply the mapping to the locationType column
        self.data['locationType'] = self.data['udbLocationType'].map(self.mapping).fillna(self.data['locationType'])

    def group_by_id(self):
        """Group the data by ID and aggregate the values dynamically."""
        # Define specific aggregation rules for known columns
        special_aggregations = { }

        # Create a dynamic aggregation dictionary
        aggregation_dict = {}
        for col in self.data.columns:
            if col in special_aggregations:
                aggregation_dict[col] = special_aggregations[col]
            elif col == 'subject':
                continue
            else:
                aggregation_dict[col] = 'first'

        # Group by 'subject' and apply the aggregation
        self.data = self.data.groupby('subject').agg(aggregation_dict).reset_index()

        return len(self.data)


    def advanced_cleanup_strings(self):
        # Function to clean location names based on the specified rules
        def advanced_clean_location_name(name):
            name = str(name)

            # Remove integers
            name = re.sub(r'\d', '', name)
            
            # Replace special quotes with standard quotes
            name = name.replace('’', "'").replace('‘', "'").replace('“', '"').replace('”', '"')
            
            # Replace 'T with 't and handle HTML escaped characters
            name = name.replace("'T", "'t").replace("&#039;t", "'t").replace("&#;t", "'t")
            
            # Remove ' / ? if they are the only characters in the string
            if name in ["'", "/", "?"]:
                name = ""
            
            # Remove invalid characters (?, /, -) and leading/trailing whitespace
            name = re.sub(r'[?/-]', ' ', name).strip()
            
            # Replace multiple spaces with a single space
            name = re.sub(r'\s+', ' ', name)
            
            return name
        
        def replace_decimal_zeros(value):
            try:
                # Check if the value is a string and ends with '.0'
                if isinstance(value, str) and value.endswith('.0'):
                    # Convert to integer and then to string to remove .0
                    value = str(int(float(value)))
                return value
            except ValueError:
                # If the conversion fails, return the original value
                return value
            
        # Function to remove single or double quotes from both ends of a string if the string starts and ends with either.
        def strip_quotes(value):
            try:
                if isinstance(value, str):
                    if (value.startswith("'") and value.endswith("'")) or (value.startswith('"') and value.endswith('"')):
                        value = value[1:-1]
                return value
            except ValueError:
                return value

        self.data['locationName'] = self.data['locationName'].apply(advanced_clean_location_name)
        self.data['locationName'] = self.data['locationName'].apply(strip_quotes)
        self.data['postCode'] = self.data['postCode'].apply(replace_decimal_zeros)

    def deduplicate_data(self):
        df = self.data

        # Define the subset of fields to check for duplicates
        subset_fields = ['locationName', 'locationType', 'thoroughfare', 'huisnummer', 'postCode', 'city']

        # Convert the subset fields to correct casing
        df['locationName_tmp'] = df['locationName']
        for field in subset_fields:
            df[field] = df[field].str.title() if field in ['locationName', 'thoroughfare', 'city'] else df[field]

        # Find duplicates based on the subset of fields
        duplicates = df.duplicated(subset=subset_fields, keep=False)

        # Calculate the ratio of certainty
        # If all fields match exactly, the ratio is 1 (100%)
        df['certainty_ratio'] = duplicates.groupby(df[subset_fields].apply(tuple, axis=1)).transform('mean')

        # Add a column to indicate if the row is a duplicate or not
        df['is_duplicate'] = duplicates

        # Add the subject value of the first duplicate to the other duplicate rows
        df['duplicate_subject'] = df.loc[duplicates, 'subject'].groupby(df[subset_fields].apply(tuple, axis=1)).transform('first')

        # Define subsets for additional deduplication
        additional_subsets = [
            (['locationName', 'locationType', 'postCode'], 0.6),
            (['thoroughfare', 'huisnummer', 'postCode', 'city'], 0.4)
        ]

        for subset, certainty in additional_subsets:
            # Identify duplicates in the rows where is_duplicate is False
            condition = df['is_duplicate'] == False
            new_duplicates = df.loc[condition].duplicated(subset=subset, keep=False)

            # Update the certainty ratio and is_duplicate
            df.loc[condition, 'certainty_ratio'] = df.loc[condition & new_duplicates, subset].apply(tuple, axis=1).map(
                df.loc[condition & new_duplicates, subset].apply(tuple, axis=1).value_counts(normalize=True)
            ).fillna(0.0)
            df.loc[condition & new_duplicates, 'certainty_ratio'] = certainty
            df.loc[condition & new_duplicates, 'is_duplicate'] = True

            # Update the duplicate_subject
            duplicate_subjects = df.loc[condition & new_duplicates, 'subject'].groupby(
                df.loc[condition & new_duplicates, subset].apply(tuple, axis=1)
            ).transform('first')
            df.loc[condition & new_duplicates, 'duplicate_subject'] = duplicate_subjects

        # Handling location names with fuzzy matching
        condition = df['certainty_ratio'] == 0.4
        unmatched_df = df[condition]
        all_names = unmatched_df['locationName'].dropna().unique().tolist()

        matched_pairs = []
        for name in all_names:
            matches = process.extract(name, all_names, scorer=fuzz.token_sort_ratio, limit=len(all_names))
            for match in matches:
                if match[1] >= 90 and name != match[0]:  # Threshold for fuzzy matching and avoid self-matching
                    matched_pairs.append((name, match[0]))

        for original, match in matched_pairs:
            mask = (df['locationName'] == match) & condition

            # Check if 'thoroughfare', 'huisnummer', 'postCode', and 'city' also match
            thoroughfare_match = df.loc[mask, 'thoroughfare'].values[0] == df.loc[df['locationName'] == original, 'thoroughfare'].values[0]
            postCode_match = df.loc[mask, 'postCode'].values[0] == df.loc[df['locationName'] == original, 'postCode'].values[0]
            
            if thoroughfare_match and postCode_match:
                df.loc[mask, 'certainty_ratio'] = 0.5
                first_subject = df.loc[df['locationName'] == original, 'subject'].iloc[0]
                df.loc[mask, 'duplicate_subject'] = first_subject

        # Remove values in duplicate_subject with NaN if they match the values in subject to indicate the original row
        df.loc[df['duplicate_subject'] == df['subject'], 'duplicate_subject'] = ""

        # Decrease the certainty ratio for rows without postCode and city
        condition = (df['certainty_ratio'] == 1) & ((df['postCode'].isna() | df['postCode'].eq('')) | (df['city'].isna() | df['city'].eq('')))
        df.loc[condition, 'certainty_ratio'] = 0.1

        # Drop the temporary columns
        df['locationName'] = df['locationName_tmp']
        df.drop(columns=['locationName_tmp'], inplace=True)

        # Replace NaN values in certainty_ratio with the placeholder 0
        df['certainty_ratio'] = df['certainty_ratio'].fillna(0)

        self.data = df

        # Return the number of rows per certainty_ratio value
        return df['certainty_ratio'].value_counts().sort_index()

    def remove(self):
        # Open the file and read the lines
        with open('to_remove.txt', 'r') as file:
            to_remove = file.readlines()

        # Remove newline characters and create an array
        to_remove = [line.strip() for line in to_remove]

        # Drop rows that contain any of the strings in the array
        for string in to_remove:
            self.data = self.data[~self.data['subject'].str.contains(string)]

        return(len(self.data))

    def save_data(self):
        """Save the transformed data to the output CSV file."""
        self.data.to_csv("cleansed.csv", encoding='utf-8', index=False)
