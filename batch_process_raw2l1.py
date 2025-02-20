import os
import subprocess
import logging
from datetime import datetime
import argparse

# Configure argument parser
parser = argparse.ArgumentParser(description="Batch process ceilometer files within a specific date range.")
parser.add_argument("--start_date", required=True, help="Start date in YYYYMMDD format.")
parser.add_argument("--end_date", required=True, help="End date in YYYYMMDD format.")
args = parser.parse_args()

# Parse start and end dates from arguments
try:
    start_date = datetime.strptime(args.start_date, "%Y%m%d")
    end_date = datetime.strptime(args.end_date, "%Y%m%d")
except ValueError:
    print("Error: Dates must be in YYYYMMDD format.")
    exit(1)

# Configure logging
log_dir = "/home/thomasfgklima/Dokumente/urbisphere/processing/ACTRIS_raw2l1_tg/logs"
os.makedirs(log_dir, exist_ok=True)

# Create unique log file names with timestamps
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
detailed_log_file = os.path.join(log_dir, f"detailed_log_{timestamp}.log")
error_log_file = os.path.join(log_dir, f"error_log_{timestamp}.log")

# Set up logging
logger = logging.getLogger("batch_processing")
logger.setLevel(logging.INFO)

# Detailed log handler
detailed_handler = logging.FileHandler(detailed_log_file)
detailed_handler.setLevel(logging.INFO)
detailed_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
detailed_handler.setFormatter(detailed_formatter)

# Error log handler
error_handler = logging.FileHandler(error_log_file)
error_handler.setLevel(logging.ERROR)
error_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
error_handler.setFormatter(error_formatter)

# Add handlers to logger
logger.addHandler(detailed_handler)
logger.addHandler(error_handler)

# Define directories and script paths
base_data_dir = "/home/thomasfgklima/Nextcloud/fgdata/obs/UCO/GRUN/ceilometer"
output_dir = "/home/thomasfgklima/Dokumente/urbisphere/processing/STRATfinder/input_raw2l1/GRUN"
config_file = "/home/thomasfgklima/Dokumente/urbisphere/processing/ACTRIS_raw2l1_tg/raw2l1/conf/conf_lufft_chm15k_eprofile_GRUN.ini"
python_executable = "python"
raw2l1_script = "/home/thomasfgklima/Dokumente/urbisphere/processing/ACTRIS_raw2l1_tg/raw2l1/raw2l1.py"

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

# Iterate over all .nc files in the base_data_dir
for root, _, files in os.walk(base_data_dir):
    for file in files:
        if file.endswith(".nc"):
            # Extract date and other details from the filename
            parts = file.split("_")
            if len(parts) < 4:
                logger.warning(f"Skipping invalid file: {file}")
                continue

            date_str = parts[0]  # e.g., "20220804"
            try:
                file_date = datetime.strptime(date_str, "%Y%m%d")
            except ValueError:
                logger.warning(f"Skipping file with invalid date format: {file}")
                continue

            # Check if the file's date falls within the specified range
            if not (start_date <= file_date <= end_date):
                logger.info(f"Skipping file outside date range: {file}")
                continue

            identifier = parts[1]  # e.g., "99001"
            instrument = parts[2]  # e.g., "CHM160145"
            file_suffix = parts[3].split(".")[0]  # e.g., "000", "001"

            # Define input and output file paths
            input_file = os.path.join(root, file)
            output_file = os.path.join(output_dir, f"{file}")

            # Log processing start
            logger.info(f"Processing file: {file}")

            # Build the command
            command = [
                python_executable,
                raw2l1_script,
                date_str,
                config_file,
                input_file,
                output_file,
            ]

            # Run the command
            try:
                subprocess.run(command, check=True)
                logger.info(f"Successfully processed: {file}")
            except subprocess.CalledProcessError as e:
                logger.error(f"Error processing {file}: {e}")
            except Exception as ex:
                logger.error(f"Unexpected error for {file}: {ex}")

# Log batch processing completion
logger.info("Batch processing completed.")
