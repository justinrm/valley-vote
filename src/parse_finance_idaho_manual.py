"""Parses manually acquired Idaho campaign finance data (e.g., CSV from records request)."""

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional, Dict, Any

import pandas as pd

# Local imports - Assuming these paths are correct relative to src
from .config import (
    # Need to define a placeholder mapping in config.py
    # MANUAL_FINANCE_COLUMN_MAP,
    PROCESSED_DATA_DIR # Assuming PROCESSED_DATA_DIR is defined in config
)
from .utils import (
    setup_logging,
    convert_to_csv,
    # Potentially add load_csv_or_excel, clean_name, etc. as needed
)

# Setup logging
# Using a distinct logger name for this script
logger = logging.getLogger(Path(__file__).stem)

def load_manual_finance_data(file_path: Path) -> Optional[pd.DataFrame]:
    """Loads the finance data from the specified file (CSV or Excel)."""
    logger.info(f"Attempting to load manual finance data from: {file_path}")

    # Placeholder: Add logic to determine file type and load accordingly
    # Using pandas read_csv or read_excel
    # Example:
    try:
        if file_path.suffix.lower() == ".csv":
            df = pd.read_csv(file_path, low_memory=False) # low_memory=False can help with mixed types
        elif file_path.suffix.lower() in [".xlsx", ".xls"]:
            df = pd.read_excel(file_path)
        else:
            logger.error(f"Unsupported file type: {file_path.suffix}. Please provide a CSV or Excel file.")
            return None
        logger.info(f"Successfully loaded {len(df)} rows from {file_path.name}")
        return df
    except FileNotFoundError:
        logger.error(f"Input file not found: {file_path}")
        return None
    except Exception as e:
        logger.error(f"Error loading file {file_path}: {e}", exc_info=True)
        return None

def clean_and_standardize_data(df: pd.DataFrame, column_map: Dict[str, str]) -> pd.DataFrame:
    """Cleans the raw data and standardizes column names."""
    logger.info("Starting data cleaning and standardization...")

    # 1. Rename columns based on the map
    # Placeholder: Define MANUAL_FINANCE_COLUMN_MAP in config.py
    # actual_map = MANUAL_FINANCE_COLUMN_MAP # Load from config
    actual_map = column_map # Passed as argument for now
    # Filter map to include only columns present in the DataFrame
    rename_map = {k: v for k, v in actual_map.items() if k in df.columns}
    missing_cols = [k for k in actual_map if k not in df.columns]
    if missing_cols:
        logger.warning(f"Source file missing expected columns (will be ignored): {missing_cols}")

    df = df.rename(columns=rename_map)
    logger.info(f"Renamed columns: {list(rename_map.values())}")

    # 2. Data Cleaning (Placeholders - requires knowledge of actual data)
    # - Convert date columns (e.g., 'contribution_date') to datetime objects
    #   pd.to_datetime(df['contribution_date'], errors='coerce')
    # - Convert amount columns (e.g., 'contribution_amount') to numeric
    #   pd.to_numeric(df['contribution_amount'].astype(str).str.replace(r'[$,]', '', regex=True), errors='coerce')
    # - Clean names (filer, contributor) using utils.clean_name if needed
    #   df['contributor_name_clean'] = df['contributor_name'].apply(clean_name)
    # - Standardize addresses, states, zip codes
    # - Handle missing values (NaN/None)

    logger.warning("Data cleaning steps are placeholders. Update based on actual data format!")

    # 3. Select and reorder columns based on the target schema (from data_schema.md)
    # target_columns = [col for col in provisional_schema_columns if col in df.columns]
    # df = df[target_columns]

    logger.info("Finished data cleaning and standardization.")
    return df

def main():
    """Main function to parse arguments and orchestrate parsing."""
    parser = argparse.ArgumentParser(description="Parse manually acquired Idaho campaign finance data.")
    parser.add_argument("input_file", type=str, help="Path to the input data file (CSV or Excel).")
    parser.add_argument("-o", "--output-dir", type=str, default=str(PROCESSED_DATA_DIR),
                        help=f"Directory to save the processed CSV file (default: {PROCESSED_DATA_DIR})")
    parser.add_argument("--log-file", type=str, default=None, help="Path to log file (defaults to console and ./logs/<script_name>.log)")
    parser.add_argument("--year", type=int, default=None, help="Optional: Year to associate with the data (used in output filename)")

    args = parser.parse_args()

    # Setup logging (use default path within logs/ dir if not specified)
    log_file_path = args.log_file or Path("logs") / f"{Path(__file__).stem}.log"
    setup_logging(log_filename=str(log_file_path))

    logger.info(f"--- Starting Manual Finance Data Parser --- ")
    logger.info(f"Input file: {args.input_file}")
    logger.info(f"Output directory: {args.output_dir}")

    input_path = Path(args.input_file)
    output_dir_path = Path(args.output_dir)
    output_dir_path.mkdir(parents=True, exist_ok=True)

    # --- Load Data --- 
    raw_df = load_manual_finance_data(input_path)
    if raw_df is None:
        sys.exit(1) # Exit if loading failed

    # --- Clean and Standardize --- 
    # Placeholder Column Map - DEFINE THIS IN config.py based on actual data!
    # Example: {"Date Received": "contribution_date", "Amount": "contribution_amount", ...}
    placeholder_map = {
        # "Source Column Name 1": "target_schema_col_1",
        # "Source Column Name 2": "target_schema_col_2",
    }
    if not placeholder_map:
        logger.error("Placeholder column map is empty. Please define the expected column mapping.")
        logger.warning("Skipping cleaning/standardization. Saving raw loaded data.")
        processed_df = raw_df # Save raw if no map
    else:
        processed_df = clean_and_standardize_data(raw_df, placeholder_map)

    # --- Save Processed Data --- 
    output_filename = f"finance_contributions_processed_{args.year or 'all'}_ID.csv"
    output_path = output_dir_path / output_filename

    logger.info(f"Saving processed data to: {output_path}")
    # Use convert_to_csv utility
    # Decide which columns to include based on the provisional schema
    # provisional_schema_columns = ["contribution_id", "legislator_id", ... ] # Define list based on docs/data_schema.md
    # final_columns = [col for col in provisional_schema_columns if col in processed_df.columns]

    try:
        # convert_to_csv(processed_df, output_path, columns=final_columns, index=False)
        # For now, save all columns present after potential renaming
        processed_df.to_csv(output_path, index=False, encoding='utf-8')
        logger.info(f"Successfully saved {len(processed_df)} rows.")
    except Exception as e:
        logger.error(f"Error saving processed data to {output_path}: {e}", exc_info=True)
        sys.exit(1)

    logger.info("--- Manual Finance Data Parser Finished ---")

if __name__ == "__main__":
    main() 