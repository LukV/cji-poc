# Python ETL Pipeline

This project contains a Python-based ETL (Extract, Transform, Load) pipeline that processes data from CSV files.

## Features

- Load data from a CSV file
- Filter data based on UDB location type
- Group data by ID
- Perform advanced cleanup on strings
- Fill empty location types
- Remove quarantined records based on a list in `to_remove.txt`
- Deduplicate data
- Save processed data to a new CSV file
- Merge multiple CSV files into one