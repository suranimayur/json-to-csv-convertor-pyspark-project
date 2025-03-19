# File: D:\python_projects\venv\python_projects\scripts\pipeline.py
#!/usr/bin/env python3
"""
Main Pipeline Orchestrator.

Coordinates the execution of all pipeline steps:
1. Data Generation
2. JSON to CSV Conversion
3. Data Transformation and Aggregation

Handles logging, error handling, and execution time tracking.
"""

import os
import sys
import time
import json
import logging
import argparse
from datetime import datetime
from typing import Dict, Any, Optional, Callable

# Add the parent directory to sys.path to make imports work correctly
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

# Configure logging
log_dir = os.path.join(parent_dir, 'logs')
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, f'pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('data_pipeline')

def load_config(config_file: Optional[str] = None) -> Dict[str, Any]:
    """
    Load pipeline configuration from JSON file.

    Args:
        config_file: Path to the configuration file

    Returns:
        Dictionary with configuration parameters
    """
    if config_file is None:
        config_file = os.path.join(parent_dir, 'config', 'pipeline_config.json')

    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
        logger.info(f"Loaded configuration from {config_file}")
        return config
    except Exception as e:
        logger.error(f"Error loading configuration: {str(e)}")
        # Default configuration
        default_config = {
            "num_files": 10,
            "records_per_file": 1000,
            "raw_data_dir": os.path.join(parent_dir, "data", "raw"),
            "processed_data_dir": os.path.join(parent_dir, "data", "processed"),
            "curated_data_dir": os.path.join(parent_dir, "data", "curated")
        }
        logger.info(f"Using default configuration: {default_config}")
        return default_config

def run_step(step_name: str, func: Callable, *args, **kwargs) -> bool:
    """
    Run a pipeline step and log its execution time.

    Args:
        step_name: Name of the step
        func: Function to execute
        *args, **kwargs: Arguments to pass to the function

    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Starting step: {step_name}")
    start_time = time.time()

    try:
        func(*args, **kwargs)
        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"Completed step: {step_name} in {duration:.2f} seconds")
        return True
    except Exception as e:
        logger.error(f"Error in step {step_name}: {str(e)}")
        return False

def run_pipeline(config_file: Optional[str] = None, use_spark: bool = False) -> bool:
    """
    Run the complete data processing pipeline.

    Args:
        config_file: Path to the configuration file (optional)
        use_spark: Whether to use Spark for data transformation

    Returns:
        True if pipeline completed successfully, False otherwise
    """
    # Load configuration
    config = load_config(config_file)

    # Record pipeline start time
    pipeline_start = time.time()
    logger.info("Starting data processing pipeline")

    # Import modules directly
    try:
        # Step 1: Generate data
        from scripts.data_generator import generate_data
        success = run_step("Data Generation", generate_data,
                          num_files=config['num_files'],
                          records_per_file=config['records_per_file'],
                          output_dir=config['raw_data_dir'])

        if not success:
            logger.error("Pipeline failed at data generation step")
            return False

        # Step 2: Convert JSON to CSV
        from scripts.json_to_csv_convertor import process_all_json_files
        success = run_step("JSON to CSV Conversion", process_all_json_files,
                          input_dir=config['raw_data_dir'],
                          output_dir=config['processed_data_dir'])

        if not success:
            logger.error("Pipeline failed at JSON to CSV conversion step")
            return False

        # Step 3: Transform and aggregate data
        if use_spark:
            curated_dir = f"{config['curated_data_dir']}_spark"
            from scripts.pyspark_transformer import process_data_with_spark
            success = run_step("Data Transformation (Spark)", process_data_with_spark,
                              input_dir=config['processed_data_dir'],
                              output_dir=curated_dir)
        else:
            from scripts.data_transformer import process_data
            success = run_step("Data Transformation", process_data,
                              input_dir=config['processed_data_dir'],
                              output_dir=config['curated_data_dir'])

        if not success:
            logger.error("Pipeline failed at data transformation step")
            return False

        # Record pipeline end time
        pipeline_end = time.time()
        total_duration = pipeline_end - pipeline_start
        logger.info(f"Pipeline completed successfully in {total_duration:.2f} seconds")

        return True

    except ImportError as e:
        logger.error(f"Failed to import required module: {str(e)}")
        logger.error("Make sure all script files are in the correct location")
        return False

def main():
    """Parse command line arguments and execute the pipeline."""
    parser = argparse.ArgumentParser(description='Run the data processing pipeline')
    parser.add_argument('--config', type=str, help='Path to configuration file')
    parser.add_argument('--use-spark', action='store_true', help='Use Spark for data transformation')

    args = parser.parse_args()
    success = run_pipeline(args.config, args.use_spark)

    if not success:
        logger.error("Pipeline execution failed")
        sys.exit(1)

    logger.info("Pipeline execution completed successfully")

if __name__ == "__main__":
    main()