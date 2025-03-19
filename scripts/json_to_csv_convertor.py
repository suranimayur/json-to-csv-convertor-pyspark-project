# File: scripts/json_to_csv_converter.py
#!/usr/bin/env python3
"""
JSON to CSV Converter.

Converts JSON files to CSV format for easier data processing.
Handles nested JSON structures through flattening.
"""

import json
import csv
import os
import glob
import logging
import argparse
from typing import Dict, Any, List, Set

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def flatten_json(nested_json: Dict[str, Any], prefix: str = '') -> Dict[str, Any]:
    """
    Flatten a nested JSON structure into a flat dictionary.

    Args:
        nested_json: The nested JSON object to flatten
        prefix: Prefix for nested keys

    Returns:
        A flattened dictionary with dot notation for nested keys
    """
    flattened: Dict[str, Any] = {}

    for key, value in nested_json.items():
        new_key = f"{prefix}{key}" if prefix else key

        if isinstance(value, dict):
            flattened.update(flatten_json(value, f"{new_key}_"))
        elif isinstance(value, list):
            # Convert lists to string representation
            flattened[new_key] = ','.join(map(str, value)) if value else ''
        else:
            flattened[new_key] = value

    return flattened

def convert_json_to_csv(json_file_path: str, csv_file_path: str) -> None:
    """
    Convert a JSON file to CSV format.

    Args:
        json_file_path: Path to the input JSON file
        csv_file_path: Path to the output CSV file
    """
    # Read JSON data
    with open(json_file_path, 'r') as json_file:
        data = json.load(json_file)

    if not data:
        logger.warning(f"{json_file_path} contains no data.")
        return

    # Flatten the data
    flattened_data = [flatten_json(record) for record in data]

    # Get all unique keys as fieldnames
    fieldnames: Set[str] = set()
    for record in flattened_data:
        fieldnames.update(record.keys())
    fieldnames_list = sorted(list(fieldnames))

    # Write to CSV
    with open(csv_file_path, 'w', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames_list)
        writer.writeheader()
        writer.writerows(flattened_data)

    logger.info(f"Converted {json_file_path} to {csv_file_path}")

def process_all_json_files(input_dir: str, output_dir: str) -> None:
    """
    Process all JSON files in the input directory and convert them to CSV.

    Args:
        input_dir: Directory containing JSON files
        output_dir: Directory to store CSV files
    """
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Get all JSON files
    json_files = glob.glob(os.path.join(input_dir, '*.json'))

    if not json_files:
        logger.warning(f"No JSON files found in {input_dir}")
        return

    logger.info(f"Found {len(json_files)} JSON files to process")

    # Process each file
    for json_file in json_files:
        base_name = os.path.basename(json_file)
        csv_file = os.path.join(output_dir, base_name.replace('.json', '.csv'))
        convert_json_to_csv(json_file, csv_file)

    logger.info(f"Processed {len(json_files)} JSON files")

def main():
    """Parse command line arguments and execute JSON to CSV conversion."""
    parser = argparse.ArgumentParser(description='Convert JSON files to CSV format')
    parser.add_argument('--input-dir', type=str, default='data/raw', help='Input directory containing JSON files')
    parser.add_argument('--output-dir', type=str, default='data/processed', help='Output directory for CSV files')

    args = parser.parse_args()
    process_all_json_files(args.input_dir, args.output_dir)

if __name__ == "__main__":
    main()

# Sample Output:
# 2025-03-19 12:32:15,123 - __main__ - INFO - Found 10 JSON files to process
# 2025-03-19 12:32:16,456 - __main__ - INFO - Converted data/raw/transactions_001.json to data/processed/transactions_001.csv
# 2025-03-19 12:32:17,789 - __main__ - INFO - Converted data/raw/transactions_002.json to data/processed/transactions_002.csv
# ...
# 2025-03-19 12:32:45,678 - __main__ - INFO - Converted data/raw/transactions_010.json to data/processed/transactions_010.csv
# 2025-03-19 12:32:45,680 - __main__ - INFO - Processed 10 JSON files

# Sample CSV Header:
# category,customer_id,is_gift,payment_method,price,product_id,product_name,quantity,rating,shipping_address_city,shipping_address_country,shipping_address_state,shipping_address_street,shipping_address_zip_code,tags,timestamp,transaction_id