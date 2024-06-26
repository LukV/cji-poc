"""ETL pipeline for data cleansing and transformation."""
import re
import json
import pandas as pd
from rapidfuzz import fuzz, process

class ETLPipeline:
    """ETLPipeline is a class that provides methods for data cleansing and transformation."""

    def __init__(self, input_csv, mapping_json):
        self.input_csv = input_csv
        self.mapping_json = mapping_json
        self.data = None
        self.mapping = None

    def _load_mappings(self):
        """Load mappings from a JSON file."""
        with open(self.mapping_json, 'r', encoding='utf-8') as file:
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
        self.data = self.data[self.data['udbLocationType'] \
            .isin(self.mapping.keys()) | self.data['udbLocationType'] \
                .isna()]

        return len(self.data)

    def fill_empty_location_type(self):
        """"Update the locationType column based on the mapping."""
        # Apply the mapping to the locationType column
        self.data['locationType'] = self.data['udbLocationType'] \
            .map(self.mapping) \
                .fillna(self.data['locationType'])

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
        """Function to clean location names based on the specified rules."""
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

        # Function to remove single or double quotes from both ends of a
        # string if the string starts and ends with either.
        def strip_quotes(value):
            try:
                if isinstance(value, str):
                    if (value.startswith("'") and value.endswith("'")) or \
                        (value.startswith('"') and value.endswith('"')):
                        value = value[1:-1]
                return value
            except ValueError:
                return value

        self.data['locationName'] = self.data['locationName'].apply(advanced_clean_location_name)
        self.data['locationName'] = self.data['locationName'].apply(strip_quotes)
        self.data['postCode'] = self.data['postCode'].apply(replace_decimal_zeros)

    def deduplicate_data(self):
        """Deduplicate the data based on specified rules."""
        df = self.data

        # Step 1: Prepare data for deduplication
        df = self._prepare_data_for_deduplication(df)

        # Step 2: Initial deduplication
        df = self._initial_deduplication(df)

        # Step 3: Additional deduplication
        df = self._additional_deduplication(df)

        # Step 4: Fuzzy matching on location names
        df = self._fuzzy_matching(df)

        # Step 5: Final adjustments
        df = self._final_adjustments(df)

        self.data = df

        # Return the number of rows per certainty_ratio value
        return df['certainty_ratio'].value_counts().sort_index()

    def _prepare_data_for_deduplication(self, df):
        """Prepare data by converting subset fields to the correct casing."""
        subset_fields = ['locationName', 'locationType',
                         'thoroughfare', 'huisnummer', 'postCode', 'city']

        df['locationName_tmp'] = df['locationName']

        for field in subset_fields:
            if field in ['locationName', 'thoroughfare', 'city']:
                df[field] = df[field].str.title()

        return df

    def _initial_deduplication(self, df):
        """Perform initial deduplication based on subset fields."""
        subset_fields = ['locationName', 'locationType', 'thoroughfare',
                         'huisnummer', 'postCode', 'city']

        duplicates = df.duplicated(subset=subset_fields, keep=False)

        df['certainty_ratio'] = duplicates.groupby(df[subset_fields]. \
            apply(tuple, axis=1)).transform('mean')

        df['is_duplicate'] = duplicates

        df['duplicate_subject'] = df.loc[duplicates, 'subject']. \
            groupby(df[subset_fields].apply(tuple, axis=1)).transform('first')

        return df

    def _additional_deduplication(self, df):
        """Perform additional deduplication with different subsets."""
        additional_subsets = [
            (['locationName', 'locationType', 'postCode'], 0.6),
            (['thoroughfare', 'huisnummer', 'postCode', 'city'], 0.4)
        ]

        for subset, certainty in additional_subsets:
            condition = df['is_duplicate'] is False
            new_duplicates = df.loc[condition].duplicated(subset=subset, keep=False)
            df.loc[condition, 'certainty_ratio'] = df.loc[condition & new_duplicates, subset] \
                .apply(tuple, axis=1).map(
                df.loc[condition & new_duplicates, subset] \
                    .apply(tuple, axis=1).value_counts(normalize=True)).fillna(0.0)

            df.loc[condition & new_duplicates, 'certainty_ratio'] = certainty
            df.loc[condition & new_duplicates, 'is_duplicate'] = True

            duplicate_subjects = df.loc[condition & new_duplicates, 'subject'].groupby(
                df.loc[condition & new_duplicates, subset].apply(tuple, axis=1)).transform('first')

            df.loc[condition & new_duplicates, 'duplicate_subject'] = duplicate_subjects

        return df

    def _fuzzy_matching(self, df):
        """Perform fuzzy matching on location names."""
        condition = df['certainty_ratio'] == 0.4
        unmatched_df = df[condition]
        all_names = unmatched_df['locationName'].dropna().unique().tolist()
        matched_pairs = []

        for name in all_names:
            matches = process.extract(name,
                                      all_names,
                                      scorer=fuzz.token_sort_ratio,
                                      limit=len(all_names))
            for match in matches:
                if match[1] >= 90 and name != match[0]:
                    matched_pairs.append((name, match[0]))

        for original, match in matched_pairs:
            mask = (df['locationName'] == match) & condition
            thoroughfare_match = df.loc[mask, 'thoroughfare'].values[0] == df. \
                loc[df['locationName'] == original, 'thoroughfare'].values[0]
            postcode_match = df.loc[mask, 'postCode'].values[0] == df. \
                loc[df['locationName'] == original, 'postCode'].values[0]

            if thoroughfare_match and postcode_match:
                df.loc[mask, 'certainty_ratio'] = 0.5
                first_subject = df.loc[df['locationName'] == original, 'subject'].iloc[0]
                df.loc[mask, 'duplicate_subject'] = first_subject

        return df

    def _final_adjustments(self, df):
        """Make final adjustments to the dataframe."""
        df.loc[df['duplicate_subject'] == df['subject'], 'duplicate_subject'] = ""
        condition = (df['certainty_ratio'] == 1) & \
            ((df['postCode'].isna() | \
                df['postCode'].eq('')) | \
                    (df['city'].isna() | df['city'].eq('')))

        df.loc[condition, 'certainty_ratio'] = 0.1
        df['locationName'] = df['locationName_tmp']
        df.drop(columns=['locationName_tmp'], inplace=True)
        df['certainty_ratio'] = df['certainty_ratio'].fillna(0)

        return df


    def remove(self):
        """Remove rows that contain specific strings."""
        # Open the file and read the lines
        with open('conf/to_remove.txt', 'r', encoding='UTF-8') as file:
            to_remove = file.readlines()

        # Remove newline characters and create an array
        to_remove = [line.strip() for line in to_remove]

        # Drop rows that contain any of the strings in the array
        for string in to_remove:
            self.data = self.data[~self.data['subject'].str.contains(string)]

        return len(self.data)

    def save_data(self):
        """Save the transformed data to the output CSV file."""
        self.data.to_csv("cleansed.csv", encoding='utf-8', index=False)
