# File: scripts/pyspark_transformer.py
#!/usr/bin/env python3
"""
PySpark Data Transformer.

Transforms and aggregates data using Apache Spark for large-scale processing.
Creates multiple analytical views of the data using Spark SQL.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count as spark_count, avg as spark_avg
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, to_date
from pyspark.sql.types import IntegerType, DoubleType
import os
import logging
import argparse
from typing import Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def initialize_spark(app_name: str = "Big Data Processing Pipeline") -> SparkSession:
    """
    Initialize a Spark session.
    
    Args:
        app_name: Name of the Spark application
        
    Returns:
        Initialized SparkSession
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()
    
    logger.info("Initialized Spark session")
    return spark

def load_csv_files(spark: SparkSession, input_dir: str) -> Any:
    """
    Load all CSV files from the input directory into a Spark DataFrame.
    
    Args:
        spark: SparkSession
        input_dir: Directory containing CSV files
        
    Returns:
        Spark DataFrame with all data
    """
    # Load all CSV files with header
    df = spark.read.option("header", "true").csv(input_dir)
    
    record_count = df.count()
    column_count = len(df.columns)
    logger.info(f"Loaded CSV data with {record_count} records and {column_count} columns")
    return df

def clean_data(spark: SparkSession, df: Any) -> Any:
    """
    Clean and prepare the data for analysis using Spark.
    
    Args:
        spark: SparkSession
        df: Input Spark DataFrame
        
    Returns:
        Cleaned Spark DataFrame with additional features
    """
    # Convert string columns to appropriate types
    df = df.withColumn("price", col("price").cast(DoubleType())) \
           .withColumn("quantity", col("quantity").cast(IntegerType())) \
           .withColumn("rating", col("rating").cast(DoubleType()))
    
    # Convert timestamp to date components
    df = df.withColumn("date", to_date(col("timestamp"))) \
           .withColumn("year", year(col("timestamp"))) \
           .withColumn("month", month(col("timestamp"))) \
           .withColumn("day", dayofmonth(col("timestamp"))) \
           .withColumn("day_of_week", dayofweek(col("timestamp")))
    
    # Calculate total price
    df = df.withColumn("total_price", col("price") * col("quantity"))
    
    # Handle missing values
    df = df.na.fill({"rating": 0})
    
    # Convert boolean columns if present
    if "is_gift" in df.columns:
        df = df.withColumn("is_gift", col("is_gift").cast(IntegerType()))
    
    record_count = df.count()
    logger.info(f"Cleaned data: {record_count} records")
    return df

def aggregate_data(spark: SparkSession, df: Any) -> Dict[str, Any]:
    """
    Perform various aggregations on the data using Spark.
    
    Args:
    # File: scripts/pyspark_transformer.py (continued)

    Args:
        spark: SparkSession
        df: Input Spark DataFrame

    Returns:
        Dictionary of aggregated Spark DataFrames
    """
    # Register the DataFrame as a temporary view for SQL queries
    df.createOrReplaceTempView("transactions")

    # 1. Sales by category
    logger.info("Creating sales by category aggregation")
    sales_by_category = spark.sql("""
        SELECT
            category,
            SUM(total_price) as total_price,
            SUM(quantity) as quantity,
            COUNT(transaction_id) as num_transactions
        FROM transactions
        GROUP BY category
    """)

    # 2. Sales by date
    logger.info("Creating sales by date aggregation")
    sales_by_date = spark.sql("""
        SELECT
            date,
            SUM(total_price) as total_price,
            SUM(quantity) as quantity,
            COUNT(transaction_id) as num_transactions
        FROM transactions
        GROUP BY date
        ORDER BY date
    """)

    # 3. Product performance
    logger.info("Creating product performance aggregation")
    product_performance = spark.sql("""
        SELECT
            category,
            product_name,
            SUM(total_price) as total_price,
            SUM(quantity) as quantity,
            COUNT(transaction_id) as num_transactions,
            AVG(rating) as avg_rating
        FROM transactions
        GROUP BY category, product_name
    """)

    # 4. Payment method analysis (if column exists)
    payment_analysis = None
    if "payment_method" in df.columns:
        logger.info("Creating payment method analysis")
        payment_analysis = spark.sql("""
            SELECT
                payment_method,
                SUM(total_price) as total_price,
                COUNT(transaction_id) as num_transactions
            FROM transactions
            GROUP BY payment_method
        """)

    # 5. Gift purchase analysis (if column exists)
    gift_analysis = None
    if "is_gift" in df.columns:
        logger.info("Creating gift purchase analysis")
        gift_analysis = spark.sql("""
            SELECT
                category,
                is_gift,
                SUM(total_price) as total_price,
                COUNT(transaction_id) as num_transactions
            FROM transactions
            GROUP BY category, is_gift
        """)

    # Return all aggregations as a dictionary
    aggregations = {
        "sales_by_category": sales_by_category,
        "sales_by_date": sales_by_date,
        "product_performance": product_performance
    }

    if payment_analysis:
        aggregations["payment_analysis"] = payment_analysis

    if gift_analysis:
        aggregations["gift_analysis"] = gift_analysis

    logger.info(f"Created {len(aggregations)} aggregation views")
    return aggregations

def save_aggregations(aggregations: Dict[str, Any], output_dir: str) -> None:
    """
    Save aggregated Spark DataFrames to CSV files.

    Args:
        aggregations: Dictionary of aggregated Spark DataFrames
        output_dir: Directory to store output files
    """
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Save each aggregation to a separate file
    for name, df in aggregations.items():
        output_path = os.path.join(output_dir, name)
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
        logger.info(f"Saved {name} to {output_path}")

def process_data_with_spark(input_dir: str, output_dir: str) -> None:
    """
    End-to-end data processing pipeline using Spark.

    Args:
        input_dir: Directory containing input CSV files
        output_dir: Directory to store output files
    """
    # Initialize Spark
    spark = initialize_spark()

    try:
        # Load data
        df = load_csv_files(spark, input_dir)

        # Clean data
        cleaned_df = clean_data(spark, df)

        # Save cleaned data
        cleaned_output = os.path.join(output_dir, "cleaned_data")
        logger.info(f"Saving cleaned data to {cleaned_output}")
        cleaned_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(cleaned_output)

        # Aggregate data
        aggregations = aggregate_data(spark, cleaned_df)

        # Save aggregations
        save_aggregations(aggregations, output_dir)

        logger.info("Data processing with Spark completed successfully!")
    finally:
        # Stop Spark session
        spark.stop()
        logger.info("Spark session stopped")

def main():
    """Parse command line arguments and execute Spark data transformation."""
    parser = argparse.ArgumentParser(description='Transform and aggregate data using Apache Spark')
    parser.add_argument('--input-dir', type=str, default='data/processed', help='Input directory containing CSV files')
    parser.add_argument('--output-dir', type=str, default='data/curated_spark', help='Output directory for aggregated data')

    args = parser.parse_args()
    process_data_with_spark(args.input_dir, args.output_dir)

if __name__ == "__main__":
    main()

# Sample Output:
# 2025-03-19 12:34:15,123 - __main__ - INFO - Initialized Spark session
# 2025-03-19 12:34:20,456 - __main__ - INFO - Loaded CSV data with 10000 records and 11 columns
# 2025-03-19 12:34:25,789 - __main__ - INFO - Cleaned data: 10000 records
# 2025-03-19 12:34:26,123 - __main__ - INFO - Creating sales by category aggregation
# 2025-03-19 12:34:27,456 - __main__ - INFO - Creating sales by date aggregation
# 2025-03-19 12:34:28,789 - __main__ - INFO - Creating product performance aggregation
# 2025-03-19 12:34:29,123 - __main__ - INFO - Creating payment method analysis
# 2025-03-19 12:34:30,456 - __main__ - INFO - Creating gift purchase analysis
# 2025-03-19 12:34:31,789 - __main__ - INFO - Created 5 aggregation views
# 2025-03-19 12:34:32,123 - __main__ - INFO - Saving cleaned data to data/curated_spark/cleaned_data
# 2025-03-19 12:34:40,456 - __main__ - INFO - Saved sales_by_category to data/curated_spark/sales_by_category
# 2025-03-19 12:34:45,789 - __main__ - INFO - Saved sales_by_date to data/curated_spark/sales_by_date
# 2025-03-19 12:34:50,123 - __main__ - INFO - Saved product_performance to data/curated_spark/product_performance
# 2025-03-19 12:34:55,456 - __main__ - INFO - Saved payment_analysis to data/curated_spark/payment_analysis
# 2025-03-19 12:35:00,789 - __main__ - INFO - Saved gift_analysis to data/curated_spark/gift_analysis
# 2025-03-19 12:35:01,123 - __main__ - INFO - Data processing with Spark completed successfully!
# 2025-03-19 12:35:01,456 - __main__ - INFO - Spark session stopped

# Sample Spark SQL Result (sales_by_category):
# +------------+------------+--------+----------------+
# |    category| total_price|quantity|num_transactions|
# +------------+------------+--------+----------------+
# | Electronics|  1254789.45|    2345|            2567|
# |    Clothing|   876543.21|    4321|            1987|
# |Home & Kitchen|   543210.98|    1234|            1456|
# |       Books|   321098.76|    3456|            1234|
# |      Sports|   210987.65|    2345|             987|
# +------------+------------+--------+----------------+