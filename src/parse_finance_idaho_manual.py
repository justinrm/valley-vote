#!/usr/bin/env python3
"""
Parse manually acquired Idaho campaign finance CSV files.

This script reads the specific CSV files downloaded from the Idaho SOS website
(or obtained via public records request) and transforms them into the
standardized format expected by the Valley Vote project.
"""

# Standard library imports
import argparse
import logging
from pathlib import Path
import sys
from typing import Dict, List, Optional, Union
import re

# Third-party imports
import pandas as pd
from tqdm import tqdm

# Local imports
# Need to adjust path if running standalone vs as module
try:
    from .config import FINANCE_SCRAPE_LOG_FILE # Assuming shared log file name
    from .utils import setup_logging, setup_project_paths, convert_to_csv
    from .data_collection import FINANCE_COLUMN_MAPS # Use existing maps
except ImportError:
    # Handle running as script from project root
    sys.path.append(str(Path(__file__).parent.parent))
    from src.config import FINANCE_SCRAPE_LOG_FILE
    from src.utils import setup_logging, setup_project_paths, convert_to_csv
    from src.data_collection import FINANCE_COLUMN_MAPS

# --- Configure Logging ---
# Use a distinct logger name for this manual parser
MANUAL_FINANCE_LOG_FILE = "manual_finance_parser.log"
logger = logging.getLogger(Path(MANUAL_FINANCE_LOG_FILE).stem)

# --- Constants ---
# Define expected column mappings based on observed CSVs and PDF keys (if available)
# These might need refinement after inspecting file headers
# Using lowercase keys for easier matching after reading headers
MANUAL_CONTRIBUTION_MAP = {
    # Potential columns from 'contribution and loan 20XX.csv' / 'ContributionDownload.csv'
    'transactionid': 'transaction_id',
    'committeename': 'committee_name', # Map committee name if present
    'reportname': 'report_name',
    'contributiontype': 'contribution_type', # Keep type if useful
    'contributiondate': 'contribution_date',
    'amount': 'contribution_amount',
    'formtype': 'form_type', # e.g., Schedule A
    'contributorname': 'contributor_name',
    'address': 'contributor_address', # Assuming single address column
    'city': 'contributor_city',
    'state': 'contributor_state',
    'zipcode': 'contributor_zip',
    'occupation': 'occupation',
    'employer': 'employer',
    'description': 'contribution_description', # May contain purpose/notes
    # Add other potential fields like 'Cash/Non-Cash', 'Candidate/Measure Name', etc.
}

MANUAL_EXPENDITURE_MAP = {
    # Potential columns from 'expenditures 20XX.csv' / 'ExpenditureDownload.csv'
    'transactionid': 'transaction_id',
    'committeename': 'committee_name', # Map committee name if present
    'reportname': 'report_name',
    'expendituretype': 'expenditure_type', # Keep type
    'expendituredate': 'expenditure_date',
    'amount': 'expenditure_amount',
    'formtype': 'form_type', # e.g., Schedule B
    'payeename': 'payee_name',
    'address': 'payee_address', # Assuming single address column
    'city': 'payee_city',
    'state': 'payee_state',
    'zipcode': 'payee_zip',
    'purpose': 'expenditure_purpose',
    'paymentcode': 'payment_code', # Cash/Non-Cash?
    # Add others like 'Candidate/Measure Name'
}

MANUAL_LOAN_MAP = {
     # Potential columns from 'Loan&DebtDownload.csv' or combined files
     'transactionid': 'transaction_id', # May need prefixing (e.g., LOAN_) if combined
     'committeename': 'committee_name',
     'reportname': 'report_name',
     'loandate': 'loan_date', # Assuming specific date field
     'date': 'loan_date', # Fallback
     'lendername': 'lender_name',
     'address': 'lender_address',
     'city': 'lender_city',
     'state': 'lender_state',
     'zipcode': 'lender_zip',
     'amount': 'loan_amount',
     'interestrate': 'loan_interest_rate',
     'duedate': 'loan_due_date',
     'description': 'loan_description',
     # Add other fields like guarantor, payments, outstanding balance if present
}

MANUAL_COMMITTEE_MAP = {
    # Potential columns from 'CommitteeDownload.csv'
    'committeeid': 'committee_id_raw', # Keep original ID if needed
    'committeename': 'committee_name',
    'committeetype': 'committee_type',
    'electioncycle': 'election_cycle',
    'address': 'committee_address',
    'city': 'committee_city',
    'state': 'committee_state',
    'zipcode': 'committee_zip',
    'treasurername': 'treasurer_name',
    'active': 'is_active', # Map status field
    'registrationdate': 'registration_date',
    # Add others like contact info, associated candidates/measures
}

MANUAL_CANDIDATE_MAP = {
    # Potential columns from 'candidates_2020-2025.csv'
    'candidateid': 'candidate_id_raw', # Keep original ID
    'firstname': 'first_name',
    'middlename': 'middle_name',
    'lastname': 'last_name',
    'fullname': 'candidate_name', # Assuming a full name column exists
    'party': 'party',
    'electioncycle': 'election_cycle',
    'office': 'office_sought',
    'district': 'district',
    'status': 'candidate_status',
    # Add others like filing date, committee links
}

# --- Parsing Functions ---

def _read_and_clean_csv(file_path: Path, expected_cols: list[str] | None = None) -> pd.DataFrame | None:
    """Reads a CSV file, cleans headers, and checks for expected columns."""
    if not file_path.exists():
        logger.warning(f"File not found: {file_path}")
        return None
    if file_path.stat().st_size == 0:
        logger.info(f"File is empty: {file_path.name}")
        return None

    try:
        # Try reading with UTF-8 first
        try:
            df = pd.read_csv(file_path, low_memory=False, encoding='utf-8', on_bad_lines='warn')
        except UnicodeDecodeError:
            logger.warning(f"UTF-8 decoding failed for {file_path.name}, trying latin-1.")
            df = pd.read_csv(file_path, low_memory=False, encoding='latin-1', on_bad_lines='warn')
        except pd.errors.ParserError as e:
             # Log specific parsing errors if they occur even with on_bad_lines='warn'
             # (though 'warn' should prevent this from being fatal)
             logger.error(f"Pandas parsing error in {file_path.name}: {e}")
             return None


        if df.empty:
            logger.info(f"File read successfully but is empty (only headers?): {file_path.name}")
            return df # Return empty DataFrame

        # Clean column names: lowercase, strip whitespace, replace non-alphanumeric with _
        df.columns = [
            re.sub(r'[\\s\\W]+', '_', col.strip().lower())
            for col in df.columns
        ]

        # Check for expected columns if provided
        if expected_cols:
            missing_cols = [col for col in expected_cols if col not in df.columns]
            if missing_cols:
                logger.warning(f"Missing expected columns in {file_path.name}: {missing_cols}")
                logger.warning(f"Available columns: {list(df.columns)}")

            extra_cols = [col for col in df.columns if col not in expected_cols]
            if extra_cols:
                 logger.debug(f"Extra columns found in {file_path.name} (will be kept): {extra_cols}")


        return df
    except Exception as e:
        logger.error(f"Error reading or cleaning CSV file {file_path}: {e}")
        # Optionally re-raise or handle specific exceptions differently
        import traceback
        logger.error(traceback.format_exc())
        return None


def parse_finance_file(file_path: Path, column_map: Dict[str, str], data_type_label: str) -> Optional[pd.DataFrame]:
    """Parses a single finance file (contributions, expenditures, loans)."""
    logger.info(f"Parsing {data_type_label} file: {file_path.name}")
    df = _read_and_clean_csv(file_path)
    if df is None:
        return None

    rename_dict = {csv_col: std_col for csv_col, std_col in column_map.items() if csv_col in df.columns}
    df = df.rename(columns=rename_dict)

    # Select only the columns that were successfully mapped
    standard_columns = list(rename_dict.values())
    df = df[standard_columns] # Keep only mapped columns for now

    # --- Data Type Conversion & Cleaning (Example) ---
    # Amounts
    amount_col_std = None
    if 'contribution_amount' in df.columns: amount_col_std = 'contribution_amount'
    elif 'expenditure_amount' in df.columns: amount_col_std = 'expenditure_amount'
    elif 'loan_amount' in df.columns: amount_col_std = 'loan_amount'

    if amount_col_std:
        # Ensure string type before cleaning
        df[amount_col_std] = df[amount_col_std].astype(str)
        # Remove $, ,, handle () for negatives
        df[amount_col_std] = df[amount_col_std].str.replace(r'[$,]', '', regex=True)
        neg_mask = df[amount_col_std].str.startswith('(') & df[amount_col_std].str.endswith(')')
        df.loc[neg_mask, amount_col_std] = '-' + df.loc[neg_mask, amount_col_std].str.slice(1, -1)
        df[amount_col_std] = pd.to_numeric(df[amount_col_std], errors='coerce')

    # Dates
    date_col_std = None
    if 'contribution_date' in df.columns: date_col_std = 'contribution_date'
    elif 'expenditure_date' in df.columns: date_col_std = 'expenditure_date'
    elif 'loan_date' in df.columns: date_col_std = 'loan_date'

    if date_col_std:
        df[date_col_std] = pd.to_datetime(df[date_col_std], errors='coerce')

    # Add metadata
    df['data_type'] = data_type_label
    df['source_file'] = file_path.name
    df['parse_timestamp'] = pd.Timestamp.now().isoformat()

    logger.info(f"Parsed {len(df)} records from {file_path.name}")
    return df


# --- Main Orchestration Function ---

def parse_manual_idaho_finance_data(
    raw_finance_dir: Path,
    processed_dir: Path,
    start_year: int,
    end_year: int
) -> bool:
    """
    Reads manual Idaho finance CSVs, parses, standardizes, and saves processed data.
    """
    logger.info(f"--- Starting Manual Idaho Finance Data Parsing ({start_year}-{end_year}) ---")
    logger.info(f"Raw data directory: {raw_finance_dir}")
    logger.info(f"Processed output directory: {processed_dir}")

    if not raw_finance_dir.is_dir():
        logger.error(f"Raw finance directory not found: {raw_finance_dir}")
        return False
    processed_dir.mkdir(parents=True, exist_ok=True)

    all_contributions = []
    all_expenditures = []
    all_loans = []
    all_committees = []
    all_candidates = []
    # Add lists for other file types if needed (independent expenditures, filed reports)

    # --- Process Yearly Files ---
    years = list(range(start_year, end_year + 1))
    logger.info(f"Processing yearly files for years: {years}...")
    for year in tqdm(years, desc="Processing Yearly Files"):
        # Contributions & Loans (assuming combined file naming)
        contrib_loan_file = raw_finance_dir / f"contribution and loan {year}.csv"
        if contrib_loan_file.exists():
            # Placeholder: Need logic to separate contributions and loans if combined
            # For now, parse as contributions, potential loan data might be miscategorized
            df_contrib = parse_finance_file(contrib_loan_file, MANUAL_CONTRIBUTION_MAP, 'contribution')
            if df_contrib is not None: all_contributions.append(df_contrib)
            # TODO: Add logic to parse loans separately or identify them within this file
        else:
             logger.debug(f"File not found: {contrib_loan_file.name}")

        # Expenditures
        expend_file = raw_finance_dir / f"expenditures {year}.csv"
        if expend_file.exists():
            df_expend = parse_finance_file(expend_file, MANUAL_EXPENDITURE_MAP, 'expenditure')
            if df_expend is not None: all_expenditures.append(df_expend)
        else:
            logger.debug(f"File not found: {expend_file.name}")

    # --- Process Other Specific Files ---
    logger.info("Processing specific download files...")
    # Example: CommitteeDownload.csv (adapt pattern if filename varies)
    committee_files = list(raw_finance_dir.glob("CommitteeDownload*.csv"))
    if committee_files:
        # Assuming only one committee file, process the first found
        df_committee = _read_and_clean_csv(committee_files[0])
        if df_committee is not None:
            rename_dict = {csv_col: std_col for csv_col, std_col in MANUAL_COMMITTEE_MAP.items() if csv_col in df_committee.columns}
            df_committee = df_committee.rename(columns=rename_dict)
            df_committee['source_file'] = committee_files[0].name
            all_committees.append(df_committee[list(rename_dict.values()) + ['source_file']]) # Keep only mapped + source
            logger.info(f"Parsed {len(df_committee)} committee records from {committee_files[0].name}")
    else:
        logger.warning("No CommitteeDownload*.csv file found.")

    # Example: candidates_2020-2025.csv
    candidate_file = raw_finance_dir / "candidates_2020-2025.csv"
    if candidate_file.exists():
        df_candidate = _read_and_clean_csv(candidate_file)
        if df_candidate is not None:
            rename_dict = {csv_col: std_col for csv_col, std_col in MANUAL_CANDIDATE_MAP.items() if csv_col in df_candidate.columns}
            df_candidate = df_candidate.rename(columns=rename_dict)
            df_candidate['source_file'] = candidate_file.name
             # Dates might need parsing: pd.to_datetime(df_candidate['some_date_col'], errors='coerce')
            all_candidates.append(df_candidate[list(rename_dict.values()) + ['source_file']])
            logger.info(f"Parsed {len(df_candidate)} candidate records from {candidate_file.name}")
    else:
         logger.warning(f"Candidate file not found: {candidate_file.name}")

    # Example: Loan&DebtDownload.csv
    loan_files = list(raw_finance_dir.glob("Loan&DebtDownload*.csv"))
    if loan_files:
         df_loan = parse_finance_file(loan_files[0], MANUAL_LOAN_MAP, 'loan')
         if df_loan is not None: all_loans.append(df_loan)
    else:
         logger.warning("No Loan&DebtDownload*.csv file found.")

    # TODO: Add parsing for other relevant files:
    # - IndependentExpenditureDownload.csv
    # - FiledReportListDownload*.csv
    # - campaign finance 2020-2025.csv (Decide strategy: parse this OR yearly files?)

    # --- Consolidate and Save Processed Data ---
    logger.info("Consolidating parsed data...")
    processed_something = False

    # Consolidate contributions
    if all_contributions:
        df_contrib_final = pd.concat(all_contributions, ignore_index=True)
        output_path = processed_dir / f"finance_contributions_manual_ID_{start_year}-{end_year}.csv"
        logger.info(f"Saving {len(df_contrib_final)} processed contribution records to {output_path}...")
        df_contrib_final.to_csv(output_path, index=False, encoding='utf-8')
        processed_something = True
    else: logger.warning("No contribution data processed.")

    # Consolidate expenditures
    if all_expenditures:
        df_expend_final = pd.concat(all_expenditures, ignore_index=True)
        output_path = processed_dir / f"finance_expenditures_manual_ID_{start_year}-{end_year}.csv"
        logger.info(f"Saving {len(df_expend_final)} processed expenditure records to {output_path}...")
        df_expend_final.to_csv(output_path, index=False, encoding='utf-8')
        processed_something = True
    else: logger.warning("No expenditure data processed.")

    # Consolidate loans
    if all_loans:
        df_loan_final = pd.concat(all_loans, ignore_index=True)
        output_path = processed_dir / f"finance_loans_manual_ID_{start_year}-{end_year}.csv"
        logger.info(f"Saving {len(df_loan_final)} processed loan records to {output_path}...")
        df_loan_final.to_csv(output_path, index=False, encoding='utf-8')
        processed_something = True
    else: logger.warning("No loan data processed.")

     # Consolidate committees
    if all_committees:
        df_committee_final = pd.concat(all_committees, ignore_index=True)
        output_path = processed_dir / f"finance_committees_manual_ID_{start_year}-{end_year}.csv"
        logger.info(f"Saving {len(df_committee_final)} processed committee records to {output_path}...")
        df_committee_final.to_csv(output_path, index=False, encoding='utf-8')
        processed_something = True
    else: logger.warning("No committee data processed.")

    # Consolidate candidates
    if all_candidates:
        df_candidate_final = pd.concat(all_candidates, ignore_index=True)
        output_path = processed_dir / f"finance_candidates_manual_ID_{start_year}-{end_year}.csv"
        logger.info(f"Saving {len(df_candidate_final)} processed candidate records to {output_path}...")
        df_candidate_final.to_csv(output_path, index=False, encoding='utf-8')
        processed_something = True
    else: logger.warning("No candidate data processed.")


    logger.info(f"--- Manual Idaho Finance Parsing Finished ---")
    return processed_something


# --- Main Execution Block (for standalone testing) ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Parse manually acquired Idaho campaign finance CSV files.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('--start-year', type=int, required=True,
                        help='Start year for data processing')
    parser.add_argument('--end-year', type=int, required=True,
                        help='End year for data processing')
    parser.add_argument('--data-dir', type=str, default=None,
                        help='Override base data directory (default: ./data)')
    # Add other arguments as needed (e.g., specifying input dir more granularly)

    args = parser.parse_args()

    # --- Standalone Setup ---
    try:
        paths = setup_project_paths(args.data_dir)
    except SystemExit:
        sys.exit(1)

    # Setup logging for this module specifically
    logger = setup_logging(MANUAL_FINANCE_LOG_FILE, paths['log'])

    # Define input and output directories based on project structure
    # Expects manual files to be in data/raw/campaign_finance/idaho/
    raw_finance_dir = paths['raw'] / 'campaign_finance' / 'idaho'
    processed_dir = paths['processed']

    # --- Run the main parsing logic ---
    success = False
    try:
        success = parse_manual_idaho_finance_data(
            raw_finance_dir=raw_finance_dir,
            processed_dir=processed_dir,
            start_year=args.start_year,
            end_year=args.end_year
        )

        if success:
            print(f"Manual finance parsing finished successfully.")
            print(f"Processed files saved in: {processed_dir}")
            exit_code = 0
        else:
            print("Manual finance parsing finished, but may not have processed all expected data. Check logs.")
            exit_code = 1 # Indicate potential issues

    except Exception as e:
        logger.critical(f"Critical unhandled error during standalone manual finance parsing: {e}", exc_info=True)
        exit_code = 2
    finally:
        logging.shutdown()
        sys.exit(exit_code) 