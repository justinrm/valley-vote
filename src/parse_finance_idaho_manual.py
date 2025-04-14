#!/usr/bin/env python3
"""
Parses and cleans manually collected Idaho campaign finance data,
categorizing different file types into separate processed outputs.
"""

import argparse
import csv
import json
import logging
import os
import re
import warnings
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd

from src.config import LOG_DIR, RAW_DATA_DIR, PROCESSED_DATA_DIR
from src.utils import setup_logging

# --- Logging Setup ---
logger = setup_logging('parse_finance_idaho_manual.log', LOG_DIR)
logging.captureWarnings(True) # Route warnings through logging system

# --- Constants ---
MANUAL_FINANCE_RAW_DIR = RAW_DATA_DIR / 'campaign_finance' / 'idaho'
MANUAL_FINANCE_PROCESSED_DIR = PROCESSED_DATA_DIR / 'finance' / 'idaho_manual'

# --- Helper Functions ---

def clean_amount(amount_str: Optional[str]) -> Optional[float]:
    """Cleans a string representation of a monetary amount."""
    if pd.isna(amount_str):
        return None
    if isinstance(amount_str, (int, float)):
        return float(amount_str) # Already numeric

    # Remove currency symbols, commas, and extra spaces
    cleaned = re.sub(r'[$,\s]', '', str(amount_str))

    # Handle potential parentheses for negative numbers if needed (not seen yet)
    # if cleaned.startswith('(') and cleaned.endswith(')'):
    #     cleaned = '-' + cleaned[1:-1]

    try:
        return float(cleaned)
    except (ValueError, TypeError):
        logger.warning(f"Could not parse amount: {amount_str}")
        return None

def _read_csv_with_fallback(file_path: Path, header: int = 0) -> Optional[pd.DataFrame]:
    """Reads a CSV using UTF-8, falling back to latin-1. Handles bad lines."""
    try:
        # Use 'warn' to log bad lines but continue parsing
        return pd.read_csv(file_path, low_memory=False, encoding='utf-8', on_bad_lines='warn', header=header)
    except UnicodeDecodeError:
        logger.warning(f"UTF-8 decoding failed for {file_path.name}, trying latin-1.")
        try:
            return pd.read_csv(file_path, low_memory=False, encoding='latin-1', on_bad_lines='warn', header=header)
        except Exception as e_inner:
            logger.error(f"Pandas error reading {file_path.name} with latin-1: {str(e_inner)}", exc_info=True)
            return None
    except Exception as e_outer:
        # Catch other potential pd.read_csv errors
        logger.error(f"Pandas error reading {file_path.name}: {str(e_outer)}", exc_info=True)
        return None

def parse_transaction_csv(file_path: Union[str, Path]) -> Optional[pd.DataFrame]:
    """Loads and performs specific cleaning for transaction-based CSVs."""
    file_path = Path(file_path)
    logger.info(f"Parsing TRANSACTION file: {file_path.name}")
    try:
        df = _read_csv_with_fallback(file_path)
        if df is None:
            return None # Error already logged

        # Handle potential extra header row
        if df.shape[1] == 1 and pd.isna(df.iloc[0, 0]):
             logger.debug(f"Skipping potential extra header row in {file_path.name}")
             df = _read_csv_with_fallback(file_path, header=1)
             if df is None: return None

        logger.info(f"Loaded {len(df):,} records from {file_path.name}")
        df.columns = df.columns.str.strip() # Clean column names first

        # Clean Amounts - Specific to transaction files
        # Use standardized names if possible, or common patterns
        amount_cols_patterns = ['Amount', 'Interest Amount', 'Loan Amount'] # Simplified patterns
        actual_amount_cols = [c for c in df.columns if any(p in c for p in amount_cols_patterns)]

        if not actual_amount_cols:
             logger.warning(f"No standard amount columns found pattern match in {file_path.name}")
        for col in actual_amount_cols:
            clean_col_name = f'{col}_clean'
            logger.debug(f"Cleaning amount column: {col} -> {clean_col_name}")
            df[clean_col_name] = df[col].apply(clean_amount)
            
            # Check for rows that failed parsing
            failed_parse_mask = df[clean_col_name].isna() & df[col].notna()
            failed_indices = df.index[failed_parse_mask].tolist()
            if failed_indices:
                logger.warning(f"Found {len(failed_indices)} amounts that failed parsing in column '{col}' of file {file_path.name}. Example indices: {failed_indices[:5]}")
                # Optionally log the actual failing values for first few indices:
                # for idx in failed_indices[:3]: 
                #     logger.debug(f"    Index {idx}: Original value '{df.loc[idx, col]}' failed amount parsing.")

        # Parse Dates - Specific to transaction files
        date_cols_patterns = ['Date'] # Simplified pattern
        actual_date_cols = [c for c in df.columns if any(p in c for p in date_cols_patterns)]

        if not actual_date_cols:
             logger.warning(f"No standard date columns found matching pattern in {file_path.name}")
        for col in actual_date_cols:
            logger.debug(f"Parsing date column: {col}")
            df[f'{col}_dt'] = pd.to_datetime(df[col], errors='coerce')
            invalid_dates = df[f'{col}_dt'].isna().sum()
            if invalid_dates > 0:
                # Log only if a significant portion is invalid, or reduce log level
                if invalid_dates == len(df):
                     logger.warning(f"All dates in column '{col}' are invalid in {file_path.name}")
                else:
                     logger.info(f"Found {invalid_dates} invalid dates in column '{col}' in {file_path.name}")


        # Add source file information
        df['source_file'] = file_path.name
        logger.info(f"Finished initial parsing for transaction file: {file_path.name}")
        return df

    except pd.errors.EmptyDataError:
        logger.warning(f"File is empty: {file_path.name}")
        return None
    except Exception as e: # Catch errors during cleaning/parsing
        logger.error(f"Error processing {file_path.name} after reading: {str(e)}", exc_info=True)
        return None

def parse_generic_csv(file_path: Union[str, Path], file_type: str) -> Optional[pd.DataFrame]:
    """Loads a generic CSV file (e.g., entities, reports) with basic handling."""
    file_path = Path(file_path)
    logger.info(f"Parsing {file_type.upper()} file: {file_path.name}")
    try:
        df = _read_csv_with_fallback(file_path)
        if df is None:
            return None

        # Handle potential extra header row common in downloaded files
        # Heuristic: Check if the first row looks like a title (few non-NaNs vs second row)
        if len(df) > 1 and df.iloc[0].count() < df.iloc[1].count() * 0.5 :
            logger.debug(f"Skipping potential extra header row in {file_path.name}")
            df = _read_csv_with_fallback(file_path, header=1) # Re-read skipping first row
            if df is None: return None

        logger.info(f"Loaded {len(df):,} records from {file_path.name}")
        # Add source file information
        df['source_file'] = file_path.name
        # Basic cleaning: strip whitespace from column names
        df.columns = df.columns.str.strip()
        logger.info(f"Finished parsing {file_type} file: {file_path.name}")
        return df

    except pd.errors.EmptyDataError:
        logger.warning(f"File is empty: {file_path.name}")
        return None
    except Exception as e:
        logger.error(f"Error parsing {file_type} file {file_path.name}: {str(e)}", exc_info=True)
        return None

def save_dataframe(df: pd.DataFrame, category_name: str, processed_dir: Path):
    """Saves a dataframe to a CSV file, logging success or failure."""
    output_filename = f"idaho_manual_{category_name}.csv"
    output_path = processed_dir / output_filename
    try:
        df.to_csv(output_path, index=False, encoding='utf-8')
        logger.info(f"Successfully saved '{category_name}' data ({len(df):,} records) to: {output_path}")
        return True
    except Exception as e:
        logger.error(f"Error saving '{category_name}' data to {output_path}: {str(e)}", exc_info=True)
        return False

def process_all_manual_finance(
    raw_dir: Path = MANUAL_FINANCE_RAW_DIR,
    processed_dir: Path = MANUAL_FINANCE_PROCESSED_DIR,
    file_pattern: str = "*.csv"
) -> bool:
    """Parses all manual finance files, categorizes them, and saves separate outputs."""

    if not raw_dir.exists():
        logger.error(f"Raw data directory not found: {raw_dir}")
        return False

    processed_dir.mkdir(parents=True, exist_ok=True)

    # Initialize lists for each category
    category_dfs: dict[str, List[pd.DataFrame]] = {
        "transactions": [],
        "committees": [],
        "candidates": [],
        "report_list": [],
        "filing_activity": [],
        "other": []
    }

    all_files = list(raw_dir.glob(file_pattern))
    csv_files = [f for f in all_files if f.suffix.lower() == '.csv']

    logger.info(f"Found {len(csv_files)} CSV files in {raw_dir}")

    if not csv_files:
        logger.warning(f"No CSV files found in {raw_dir}. Nothing to process.")
        return True

    # Categorize and parse files
    for file_path in csv_files:
        fname = file_path.name.lower()
        df = None
        assigned_category = None

        # --- Categorization Logic ---
        # Order matters slightly - more specific names first if overlap exists
        if "committeedownload" in fname:
            assigned_category = "committees"
            df = parse_generic_csv(file_path, file_type=assigned_category)
        elif "candidates_2020-2025" in fname: # Specific candidate file name
             assigned_category = "candidates"
             df = parse_generic_csv(file_path, file_type=assigned_category)
        elif "filedreportlistdownload" in fname:
             assigned_category = "report_list"
             df = parse_generic_csv(file_path, file_type=assigned_category)
        elif "campaign finance 2020-2025" in fname: # Specific activity file name
             assigned_category = "filing_activity"
             df = parse_generic_csv(file_path, file_type=assigned_category)
        elif "contribution and loan" in fname or "expenditures" in fname or \
             "independentexpendituredownload" in fname or \
             "expendituredownload" in fname or \
             "contributiondownload" in fname or \
             "loan&debtdownload" in fname:
            assigned_category = "transactions"
            df = parse_transaction_csv(file_path)
        else:
            logger.warning(f"Uncategorized file: {file_path.name}. Attempting generic parse as 'other'.")
            assigned_category = "other"
            df = parse_generic_csv(file_path, file_type=assigned_category)

        # Append DF if parsing was successful
        if df is not None and assigned_category is not None:
            category_dfs[assigned_category].append(df)
        elif df is None:
             logger.error(f"Parsing failed for file: {file_path.name}")
             # Consider if a single file failure should halt everything (currently doesn't)


    # Consolidate and save each category
    overall_success = True
    for category_name, df_list in category_dfs.items():
        if not df_list:
            logger.info(f"No dataframes found for category '{category_name}'. Skipping save.")
            continue

        logger.info(f"Consolidating {len(df_list)} dataframes for category '{category_name}'...")
        try:
            # Concatenate, handling potential schema differences gracefully
            consolidated_df = pd.concat(df_list, ignore_index=True, sort=False)

            # --- Further Cleaning/Standardization (Optional per category) ---
            if category_name == "transactions":
                cols_to_drop = [
                    'Timed Report Date_dt',
                    'Public Distribution Start Date_dt',
                    'Public Distribution End Date_dt'
                ]
                # Drop columns only if they exist, ignore errors if not
                existing_cols_to_drop = [col for col in cols_to_drop if col in consolidated_df.columns]
                if existing_cols_to_drop:
                    consolidated_df.drop(columns=existing_cols_to_drop, inplace=True)
                    logger.info(f"Dropped unreliable date columns from transactions: {existing_cols_to_drop}")

            # --- Save Processed Data ---
            if not save_dataframe(consolidated_df, category_name, processed_dir):
                 overall_success = False # Mark failure if save fails

        except Exception as e:
            logger.error(f"Error during consolidation for category '{category_name}': {str(e)}", exc_info=True)
            overall_success = False # Mark failure if consolidation fails

    return overall_success

def parse_arguments():
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(description="Parse manually collected Idaho campaign finance data.")
    parser.add_argument(
        "--csv-reports",
        required=True,
        help="Path to the CSV file containing campaign finance reports.",
    )
    parser.add_argument(
        "--csv-candidates",
        required=True,
        help="Path to the CSV file containing candidate details.",
    )
    parser.add_argument(
        "--csv-committees",
        required=False, # Make it optional
        help="Optional path to the CSV file containing committee details.",
    )
    parser.add_argument(
        "--output-dir",
        default="data/processed",
        help="Directory to save the processed finance data JSON file.",
    )
    return parser.parse_args()

def load_csv_reports(csv_path: str) -> List[Dict[str, Any]]:
    """Loads campaign finance reports from a CSV file, skipping the first line."""
    reports = []
    header = []
    try:
        with open(csv_path, mode='r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            try:
                next(reader)  # Skip the first title line
                header = next(reader)  # Read the actual header row
            except StopIteration:
                logging.error(f"CSV file {csv_path} appears to be empty or missing header.")
                return []

            # Use DictReader with the correct header
            dict_reader = csv.DictReader(csvfile, fieldnames=header)
            for row in dict_reader:
                # Basic cleaning/type conversion can be added here if needed
                reports.append(row)

        logging.info(f"Successfully loaded {len(reports)} reports from {csv_path}")
        return reports
    except FileNotFoundError:
        logging.error(f"CSV file not found: {csv_path}")
        return []
    except Exception as e:
        logging.error(f"Error reading CSV file {csv_path}: {e}")
        return []

def load_csv_candidates(csv_path: str) -> Dict[str, Dict[str, Any]]:
    """Loads candidate details from a CSV file, skipping the first line.

    Assumes the second line is the header and uses 'Filing Entity ID' as the key.
    """
    candidates_dict = {}
    header = []
    # !! IMPORTANT: Verify this is the correct identifier column in candidates CSV !!
    candidate_id_column = 'Filing Entity ID'

    try:
        with open(csv_path, mode='r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            try:
                next(reader) # Skip the first title line
                header = next(reader) # Read the actual header row
                if candidate_id_column not in header:
                    logging.error(f"Candidate ID column '{candidate_id_column}' not found in header: {header}")
                    return {}
            except StopIteration:
                logging.error(f"CSV file {csv_path} appears to be empty or missing header.")
                return {}

            # Use DictReader with the correct header
            # We need to recreate the reader or seek back, easier to iterate from here
            dict_reader = csv.DictReader(csvfile, fieldnames=header)
            for row in dict_reader:
                candidate_id = row.get(candidate_id_column)
                if candidate_id:
                    # Clean up potential whitespace
                    candidate_id = candidate_id.strip()
                    if candidate_id in candidates_dict:
                        logging.warning(f"Duplicate candidate ID '{candidate_id}' found in {csv_path}. Overwriting previous entry.")
                    candidates_dict[candidate_id] = row
                else:
                    logging.warning(f"Row missing candidate ID ('{candidate_id_column}') in {csv_path}: {row}")

        logging.info(f"Successfully loaded {len(candidates_dict)} candidate entries from {csv_path}")
        return candidates_dict
    except FileNotFoundError:
        logging.error(f"Candidates CSV file not found: {csv_path}")
        return {}
    except Exception as e:
        logging.error(f"Error reading candidates CSV file {csv_path}: {e}")
        return {}

def load_csv_committees(csv_path: str) -> Dict[str, Dict[str, Any]]:
    """Loads committee details from a CSV file, skipping the first line.

    Assumes the second line is the header and uses 'Filing Entity ID' as the key.
    Returns an empty dict if the file path is None or empty.
    """
    if not csv_path:
        logging.info("No committee CSV path provided. Skipping committee load.")
        return {}

    committees_dict = {}
    header = []
    # !! IMPORTANT: Verify this is the correct identifier column in committees CSV !!
    committee_id_column = 'Filing Entity ID'

    try:
        with open(csv_path, mode='r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            try:
                next(reader) # Skip the first title line
                header = next(reader) # Read the actual header row
                if committee_id_column not in header:
                    logging.error(f"Committee ID column '{committee_id_column}' not found in header: {header}")
                    return {}
            except StopIteration:
                logging.error(f"Committee CSV file {csv_path} appears to be empty or missing header.")
                return {}

            dict_reader = csv.DictReader(csvfile, fieldnames=header)
            for row in dict_reader:
                committee_id = row.get(committee_id_column)
                if committee_id:
                    committee_id = committee_id.strip()
                    if committee_id in committees_dict:
                        logging.warning(f"Duplicate committee ID '{committee_id}' found in {csv_path}. Overwriting previous entry.")
                    committees_dict[committee_id] = row
                else:
                    logging.warning(f"Row missing committee ID ('{committee_id_column}') in {csv_path}: {row}")

        logging.info(f"Successfully loaded {len(committees_dict)} committee entries from {csv_path}")
        return committees_dict
    except FileNotFoundError:
        logging.error(f"Committees CSV file not found: {csv_path}")
        return {}
    except Exception as e:
        logging.error(f"Error reading committees CSV file {csv_path}: {e}")
        return {}

def combine_data(reports: List[Dict[str, Any]], 
                 candidates: Dict[str, Dict[str, Any]], 
                 committees: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Combines report data with candidate or committee details.

    Args:
        reports: A list of dictionaries, where each dictionary represents a report from the CSV.
        candidates: A dictionary keyed by Filing Entity ID containing candidate details.
        committees: A dictionary keyed by Filing Entity ID containing committee details.

    Returns:
        A list of dictionaries, where each dictionary represents a report enriched
        with corresponding entity (candidate or committee) details.
    """
    combined_data = []
    reports_without_match = 0

    candidate_key_in_reports_csv = 'Filing Entity Id'

    for report in reports:
        entity_type = 'unknown' # Track if match is candidate or committee
        details = None
        candidate_identifier = report.get(candidate_key_in_reports_csv)

        if not candidate_identifier:
            logging.warning(f"Report missing identifier key ('{candidate_key_in_reports_csv}'): {report.get('ReportName', 'N/A')}")
            reports_without_match += 1
            continue

        # Clean identifier before lookup
        candidate_identifier = candidate_identifier.strip()

        # Try matching with candidates first
        candidate_details = candidates.get(candidate_identifier)
        if candidate_details:
            details = candidate_details
            entity_type = 'candidate'
        else:
            # If no candidate match, try committees (if committee data exists)
            committee_details = committees.get(candidate_identifier)
            if committee_details:
                details = committee_details
                entity_type = 'committee'

        # Enrich and append if details were found
        if details:
            enriched_report = report.copy()
            enriched_report['entity_details'] = details # Use a generic name
            enriched_report['entity_type'] = entity_type
            combined_data.append(enriched_report)
        else:
            # Log only if no match was found in either candidates or committees
            filer_type = report.get('FilerType', 'N/A') # Get FilerType from report for context
            logging.warning(f"No candidate or committee details found for identifier '{candidate_identifier}' (FilerType: {filer_type}) in report: {report.get('ReportName', 'N/A')}")
            reports_without_match += 1
            # Option: could append the report without details if needed
            # combined_data.append(report)

    logging.info(f"Combined data for {len(combined_data)} reports ({len(combined_data) - reports_without_match} matched).")
    if reports_without_match > 0:
        logging.warning(f"{reports_without_match} reports could not be matched with candidate or committee details.")

    return combined_data

def save_processed_data(data: List[Dict[str, Any]], output_dir: str, filename: str = "processed_finance_data.json"):
    """Saves the processed data to a JSON file."""
    if not data:
        logging.warning("No processed data to save.")
        return

    output_path = os.path.join(output_dir, filename)

    try:
        os.makedirs(output_dir, exist_ok=True) # Ensure the output directory exists
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        logging.info(f"Successfully saved processed data to {output_path}")
    except IOError as e:
        logging.error(f"Error writing JSON file to {output_path}: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred while saving data: {e}")

def main():
    """Main function to parse and combine finance data."""
    args = parse_arguments()
    logging.info(f"Starting manual finance data processing.")
    logging.info(f"Reading CSV reports from: {args.csv_reports}")
    logging.info(f"Reading CSV candidates from: {args.csv_candidates}")
    if args.csv_committees:
        logging.info(f"Reading CSV committees from: {args.csv_committees}")
    else:
        logging.info("No committee CSV provided.")

    # Load data
    reports_data = load_csv_reports(args.csv_reports)
    candidates_data = load_csv_candidates(args.csv_candidates)
    committees_data = load_csv_committees(args.csv_committees) # Load committees (will be {} if path is None)

    # Check only essential data was loaded
    if not reports_data or not candidates_data:
        logging.error("Failed to load required reports or candidates data. Exiting.")
        return

    # Combine data
    processed_data = combine_data(reports_data, candidates_data, committees_data)

    if not processed_data:
        logging.warning("No data combined. Check identifiers and input files.")
        # Decide if processing should stop or continue

    # Save processed data
    save_processed_data(processed_data, args.output_dir)

    logging.info(f"Manual finance data processing finished.")

if __name__ == "__main__":
    main()
