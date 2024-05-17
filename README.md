# Python ETL Pipeline

This project contains a Python-based ETL (Extract, Transform, Load) pipeline that processes data from CSV files.

## Features

- Extract data from a triple store
- _Aggregate_ - Merge multiple CSV files into one
- _Consolidate_ Transform data:
    - Filter data based on UDB location type
    - Group data by ID
    - Perform cleanup on strings
    - Fill empty location types
    - Remove quarantined records based on a list in `to_remove.txt`
    - Deduplicate data
- Load processed data to a CSV file
