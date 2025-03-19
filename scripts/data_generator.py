# File: scripts/data_generator.py
#!/usr/bin/env python3
"""
Data Generator Script.

Generates sample JSON datasets for big data processing pipeline.
Creates configurable number of JSON files with transaction records.
"""

import json
import random
import datetime
import os
import uuid
import logging
import argparse
from typing import Dict, List, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Type definitions
JsonRecord = Dict[str, Any]
JsonDataset = List[JsonRecord]

def generate_data(num_files: int = 10, records_per_file: int = 1000, output_dir: str = 'D:\python_projects\venv\python_projects\data\raw') -> None:
    """
    Generate sample JSON datasets for big data processing.

    Args:
        num_files: Number of JSON files to generate
        records_per_file: Number of records per file
        output_dir: Directory to store the generated files
    """
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    logger.info(f"Generating {num_files} files with {records_per_file} records each")

    # Product categories
    categories = ['Electronics', 'Clothing', 'Home & Kitchen', 'Books', 'Sports',
                 'Beauty', 'Toys', 'Automotive', 'Health', 'Grocery']

    # Product names by category
    product_names = {
        'Electronics': ['Smartphone', 'Laptop', 'Headphones', 'Tablet', 'Smart Watch', 'Camera', 'TV'],
        'Clothing': ['T-shirt', 'Jeans', 'Dress', 'Jacket', 'Shoes', 'Hat', 'Socks'],
        'Home & Kitchen': ['Blender', 'Coffee Maker', 'Toaster', 'Microwave', 'Knife Set', 'Plates'],
        'Books': ['Fiction Novel', 'Biography', 'Cookbook', 'Self-Help', 'Science Fiction', 'History'],
        'Sports': ['Basketball', 'Tennis Racket', 'Yoga Mat', 'Dumbbells', 'Running Shoes', 'Bicycle'],
        'Beauty': ['Shampoo', 'Lipstick', 'Face Cream', 'Perfume', 'Hair Dryer', 'Nail Polish'],
        'Toys': ['Action Figure', 'Board Game', 'Puzzle', 'Stuffed Animal', 'Building Blocks', 'Doll'],
        'Automotive': ['Car Wax', 'Floor Mats', 'Air Freshener', 'Tire Gauge', 'Jump Starter'],
        'Health': ['Vitamins', 'First Aid Kit', 'Thermometer', 'Pain Reliever', 'Bandages'],
        'Grocery': ['Cereal', 'Coffee', 'Pasta', 'Snacks', 'Canned Goods', 'Frozen Meals']
    }

    # Payment methods
    payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'Cash', 'Bank Transfer', 'Gift Card']

    # Generate files
    for file_num in range(1, num_files + 1):
        data: JsonDataset = []

        # Generate records for this file
        for i in range(records_per_file):
            # Generate a random timestamp within the last year
            days_ago = random.randint(0, 365)
            hours_ago = random.randint(0, 24)
            minutes_ago = random.randint(0, 60)

            timestamp = (datetime.datetime.now() -
                        datetime.timedelta(days=days_ago, hours=hours_ago, minutes=minutes_ago))

            # Select a random category and product
            category = random.choice(categories)
            product_name = random.choice(product_names.get(category, ['Product']))

            # Create a record
            record: JsonRecord = {
                'transaction_id': str(uuid.uuid4()),
                'customer_id': str(uuid.uuid4())[:8],
                'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'product_id': str(uuid.uuid4())[:8],
                'product_name': product_name,
                'category': category,
                'price': round(random.uniform(10.0, 1000.0), 2),
                'quantity': random.randint(1, 10),
                'payment_method': random.choice(payment_methods),
                'shipping_address': {
                    'street': f"{random.randint(100, 9999)} {random.choice(['Main', 'Oak', 'Pine', 'Maple', 'Cedar'])} St",
                    'city': random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']),
                    'state': random.choice(['NY', 'CA', 'IL', 'TX', 'AZ']),
                    'zip_code': f"{random.randint(10000, 99999)}",
                    'country': 'USA'
                },
                'is_gift': random.choice([True, False]),
                'rating': random.randint(1, 5) if random.random() > 0.3 else None,
                'tags': random.sample(['sale', 'new', 'trending', 'limited', 'exclusive'],
                                     k=random.randint(0, 3))
            }

            data.append(record)

        # Write to file
        filename = os.path.join(output_dir, f"transactions_{file_num:03d}.json")
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)

        logger.info(f"Generated {filename} with {records_per_file} records")

    logger.info(f"Data generation completed: {num_files} files created with {records_per_file} records each")

def main():
    """Parse command line arguments and execute data generation."""
    parser = argparse.ArgumentParser(description='Generate sample JSON datasets for big data processing')
    parser.add_argument('--num-files', type=int, default=10, help='Number of JSON files to generate')
    parser.add_argument('--records-per-file', type=int, default=1000, help='Number of records per file')
    parser.add_argument('--output-dir', type=str, default='data/raw', help='Output directory for JSON files')

    args = parser.parse_args()
    generate_data(args.num_files, args.records_per_file, args.output_dir)

if __name__ == "__main__":
    main()

# Sample Output:
# 2025-03-19 12:31:15,123 - __main__ - INFO - Generating 10 files with 1000 records each
# 2025-03-19 12:31:16,456 - __main__ - INFO - Generated data/raw/transactions_001.json with 1000 records
# 2025-03-19 12:31:17,789 - __main__ - INFO - Generated data/raw/transactions_002.json with 1000 records
# ...
# 2025-03-19 12:31:45,678 - __main__ - INFO - Generated data/raw/transactions_010.json with 1000 records
# 2025-03-19 12:31:45,680 - __main__ - INFO - Data generation completed: 10 files created with 1000 records each

# Sample Data (First Record):
# {
#   "transaction_id": "f8a7b6c5-d4e3-42f1-a0b9-c8d7e6f5a4b3",
#   "customer_id": "a1b2c3d4",
#   "timestamp": "2024-05-15 10:30:45",
#   "product_id": "e5f6g7h8",
#   "product_name": "Laptop",
#   "category": "Electronics",
#   "price": 899.99,
#   "quantity": 1,
#   "payment_method": "Credit Card",
#   "shipping_address": {
#     "street": "1234 Oak St",
#     "city": "New York",
#     "state": "NY",
#     "zip_code": "10001",
#     "country": "USA"
#   },
#   "is_gift": false,
#   "rating": 5,
#   "tags": ["new", "trending"]
# }