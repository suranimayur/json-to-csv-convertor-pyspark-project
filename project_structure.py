# File: scripts/project_structure.py
#!/usr/bin/env python3
"""
Project structure setup script.
Creates the necessary directory structure for the data processing pipeline.
"""

import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_project_structure():
    """Create the project directory structure."""
    # Define directories
    directories = [
        'data/raw',
        'data/processed',
        'data/curated',
        'scripts',
        'config',
        'logs'
    ]

    # Create directories
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        logger.info(f"Created directory: {directory}")

    # Create placeholder files
    if not os.path.exists('config/pipeline_config.json'):
        with open('config/pipeline_config.json', 'w') as f:
            f.write('''{
  "num_files": 10,
  "records_per_file": 1000,
  "raw_data_dir": "data/raw",
  "processed_data_dir": "data/processed",
  "curated_data_dir": "data/curated"
}''')
        logger.info("Created pipeline configuration file")

    if not os.path.exists('scripts/__init__.py'):
        open('scripts/__init__.py', 'w').close()
        logger.info("Created scripts package initialization file")

    logger.info("Project structure created successfully!")

if __name__ == "__main__":
    create_project_structure()

# Sample Output:
# 2025-03-19 12:30:45,123 - __main__ - INFO - Created directory: data/raw
# 2025-03-19 12:30:45,125 - __main__ - INFO - Created directory: data/processed
# 2025-03-19 12:30:45,127 - __main__ - INFO - Created directory: data/curated
# 2025-03-19 12:30:45,129 - __main__ - INFO - Created directory: scripts
# 2025-03-19 12:30:45,131 - __main__ - INFO - Created directory: config
# 2025-03-19 12:30:45,133 - __main__ - INFO - Created directory: logs
# 2025-03-19 12:30:45,135 - __main__ - INFO - Created pipeline configuration file
# 2025-03-19 12:30:45,137 - __main__ - INFO - Created scripts package initialization file
# 2025-03-19 12:30:45,139 - __main__ - INFO - Project structure created successfully!