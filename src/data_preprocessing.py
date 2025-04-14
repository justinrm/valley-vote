# src/data_preprocessing.py
"""Data preprocessing and feature engineering for Valley Vote.

This module handles the consolidation, cleaning, and feature engineering of data
from various sources to create the final dataset for predictive modeling.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Union, Any
import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.feature_extraction.text import TfidfVectorizer

from src.utils import setup_logging, load_json, convert_to_csv
from src.config import (
    RAW_DATA_DIR,
    PROCESSED_DATA_DIR,
    LOG_DIR,
    STATUS_CODES,
    SPONSOR_TYPES,
    VOTE_TEXT_MAP
)

# --- Logging Setup ---
logger = setup_logging('data_preprocessing.log', LOG_DIR)

class DataPreprocessor:
    """Handles data preprocessing and feature engineering for Valley Vote."""

    def __init__(self, base_data_dir: Optional[Union[str, Path]] = None):
        """Initialize the data preprocessor.

        Args:
            base_data_dir: Optional override for the base data directory (should point to the root 'data' dir)
        """
        # Define paths relative to the base data directory
        base_data_path = Path(base_data_dir) if base_data_dir else Path('data') # Default to 'data' in workspace root
        self.processed_dir = base_data_path / 'processed'
        self.raw_dir = base_data_path / 'raw'
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Initialized DataPreprocessor. Processed data expected/saved in: {self.processed_dir}")

        # Initialize data storage with type hints
        self.bills_df: Optional[pd.DataFrame] = None
        self.votes_df: Optional[pd.DataFrame] = None
        self.legislators_df: Optional[pd.DataFrame] = None
        self.sponsors_df: Optional[pd.DataFrame] = None
        self.committees_df: Optional[pd.DataFrame] = None
        self.committee_membership_df: Optional[pd.DataFrame] = None # For memberships
        self.finance_df: Optional[pd.DataFrame] = None # For finance data (matched or raw)
        self.roll_calls_df: Optional[pd.DataFrame] = None # For roll call details

    def load_all_data(self) -> bool:
        """Load all available data from the processed directory."""
        logger.info(f"Loading data from: {self.processed_dir}")
        all_loaded_successfully = True # Track overall success

        def _load_csv(filename: str, attribute_name: str, critical: bool = False) -> bool:
            """Helper to load a single CSV file."""
            nonlocal all_loaded_successfully
            path = self.processed_dir / filename
            loaded_this = False
            if path.exists():
                try:
                    df = pd.read_csv(path)
                    setattr(self, attribute_name, df)
                    logger.info(f"Loaded {len(df):,} records from {filename}")
                    loaded_this = True
                except pd.errors.EmptyDataError:
                    logger.warning(f"File exists but is empty: {path}. Setting {attribute_name} to None.")
                    setattr(self, attribute_name, None)
                    # Decide if empty critical file is a failure
                    if critical:
                        all_loaded_successfully = False
                except Exception as e:
                    logger.error(f"Error loading {filename}: {str(e)}", exc_info=True)
                    setattr(self, attribute_name, None) # Ensure it's None on error
                    if critical:
                        all_loaded_successfully = False
            else:
                logger.warning(f"File not found, skipping: {path}")
                setattr(self, attribute_name, None) # Ensure attribute is None if file not found
                if critical:
                    logger.error(f"Critical file {filename} not found.")
                    all_loaded_successfully = False

            return loaded_this


        # Define which files are critical for the core pipeline
        # Hardcoding filenames for ID 2023 for now
        # TODO: Make this dynamic based on state/year parameters
        state = 'ID' # Example, should be passed or detected
        year = 2022 # Target year with likely non-empty data
        consolidated_suffix = f"_{year}_{state}"

        _load_csv(f'bills{consolidated_suffix}.csv', 'bills_df', critical=True)
        _load_csv(f'votes{consolidated_suffix}.csv', 'votes_df', critical=True)
        # Load legislators without year suffix
        _load_csv(f'legislators_{state}.csv', 'legislators_df', critical=True)
        # Load roll calls - Mark as non-critical for now as it might be missing
        _load_csv(f'roll_calls{consolidated_suffix}.csv', 'roll_calls_df', critical=False)
        _load_csv(f'sponsors{consolidated_suffix}.csv', 'sponsors_df')
        _load_csv(f'committees{consolidated_suffix}.csv', 'committees_df')
        # Assuming committee memberships and finance are consolidated differently or not needed for this step
        _load_csv(f'committee_memberships_scraped_consolidated_{state}_2020-2025.csv', 'committee_membership_df') # Assuming a consolidated name

        # Try loading matched finance, fallback to raw parsed finance if needed
        # Assuming finance is consolidated with a different pattern
        finance_consolidated_pattern = f'finance_matched_{state}_2020-2025.csv' # Example pattern
        finance_parsed_pattern = f'finance_parsed_{state}_2020-2025.csv' # Example pattern

        if not _load_csv(finance_consolidated_pattern, 'finance_df'):
             logger.info(f"{finance_consolidated_pattern} not found or failed to load, attempting to load {finance_parsed_pattern}")
             _load_csv(finance_parsed_pattern, 'finance_df')

        if not all_loaded_successfully:
             logger.critical("One or more critical data files failed to load. Preprocessing may fail or be incomplete.")
             # Return False immediately if critical files failed
             return False

        logger.info("Data loading attempt finished.")
        return True

    def validate_data(self) -> bool:
        """Validate the structure, keys, and basic content of loaded DataFrames."""
        logger.info("Starting data validation...")
        overall_valid = True # Track overall validity

        # Helper function for validation checks
        def _check_df(df: Optional[pd.DataFrame], name: str, required_cols: List[str], id_cols: List[str] = []) -> bool:
            """Performs validation checks on a single DataFrame. More robust to empty DFs."""
            nonlocal overall_valid
            if df is None:
                logger.warning(f"{name} DataFrame is not loaded. Skipping validation.")
                return True

            if df.empty:
                logger.warning(f"{name} DataFrame is loaded but empty. Skipping detailed validation.")
                # Still check if required columns *exist* even if empty
                missing_cols = [col for col in required_cols if col not in df.columns]
                if missing_cols:
                    logger.error(f"{name} DataFrame (empty) missing required columns: {missing_cols}. Found: {df.columns.tolist()}")
                    overall_valid = False # Mark overall as invalid if headers are missing
                return True # Treat empty DF as 'valid enough' to continue for now

            df_valid = True
            # 1. Check required columns
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                logger.error(f"{name} DataFrame missing required columns: {missing_cols}. Found: {df.columns.tolist()}")
                df_valid = False
                overall_valid = False # Mark overall as invalid

            # 2. Check for nulls in crucial ID columns (only if column exists)
            if id_cols:
                for id_col in id_cols:
                    if id_col in df.columns:
                        if df[id_col].isnull().any():
                            num_nulls = df[id_col].isnull().sum()
                            logger.warning(f"{name} DataFrame has {num_nulls} null values in crucial ID column '{id_col}'.")
                            # Consider making this an error depending on the column's importance
                            # df_valid = False
                    elif id_col in required_cols: # Only error if the missing ID col was required
                         logger.error(f"Required ID column '{id_col}' not found in {name} DataFrame for null check.")
                         df_valid = False # Mark as invalid if required ID col is missing
                         overall_valid = False

            # 3. Check data types (example - keep commented)
            # if 'bill_id' in df.columns and not pd.api.types.is_integer_dtype(df['bill_id']):
            #     logger.warning(f"{name} column 'bill_id' is not integer type.")

            # Do not update overall_valid here based on df_valid, it was done above
            return df_valid

        # --- Define requirements and validate each essential DataFrame ---
        # Call _check_df for each
        _check_df(self.bills_df, "Bills",
                    required_cols=['bill_id', 'session_id', 'status', 'date_introduced'],
                    id_cols=['bill_id'])

        _check_df(self.votes_df, "Votes",
                    required_cols=['vote_id', 'roll_call_id', 'legislator_id', 'vote_text'],
                    id_cols=['vote_id', 'roll_call_id', 'legislator_id'])

        _check_df(self.legislators_df, "Legislators",
                    required_cols=['legislator_id', 'session_id', 'name', 'party_id'],
                    id_cols=['legislator_id'])

        _check_df(self.roll_calls_df, "Roll Calls",
                    required_cols=['roll_call_id', 'bill_id', 'date'],
                    id_cols=['roll_call_id', 'bill_id'])

        # --- Validate optional DataFrames if they loaded ---
        _check_df(self.sponsors_df, "Sponsors",
                    required_cols=['sponsor_id', 'bill_id', 'legislator_id', 'sponsor_type'],
                    id_cols=['sponsor_id', 'bill_id', 'legislator_id'])

        _check_df(self.committee_membership_df, "Committee Membership",
                    required_cols=['committee_id', 'legislator_id', 'session_year'],
                    id_cols=['committee_id', 'legislator_id'])

        # --- Cross-DataFrame ID Consistency Checks (Make more robust) ---
        logger.info("Performing cross-DataFrame ID consistency checks...")
        # Check Votes vs Legislators
        if self.votes_df is not None and not self.votes_df.empty and \
           self.legislators_df is not None and not self.legislators_df.empty:
            try:
                if 'legislator_id' not in self.votes_df.columns or \
                   'legislator_id' not in self.legislators_df.columns:
                    logger.warning("Skipping Votes<->Legislators ID check: missing 'legislator_id' column in one or both DataFrames.")
                else:
                    # Check inferred types before attempting conversion
                    votes_leg_dtype_str = pd.api.types.infer_dtype(self.votes_df['legislator_id'])
                    master_leg_dtype_str = pd.api.types.infer_dtype(self.legislators_df['legislator_id'])

                    if votes_leg_dtype_str == 'empty' or master_leg_dtype_str == 'empty':
                        logger.warning("Skipping Votes<->Legislators ID check: 'legislator_id' column is effectively empty in one or both DataFrames.")
                    else:
                        # Proceed with comparison if types are not empty
                        common_leg_dtype = np.result_type(self.votes_df['legislator_id'].dtype, self.legislators_df['legislator_id'].dtype)
                        leg_ids_votes = self.votes_df['legislator_id'].dropna().astype(common_leg_dtype).unique()
                        leg_ids_master = self.legislators_df['legislator_id'].dropna().astype(common_leg_dtype).unique()
                        missing_legislators = np.setdiff1d(leg_ids_votes, leg_ids_master)
                        if len(missing_legislators) > 0:
                            logger.warning(f"{len(missing_legislators)} legislator_ids in votes_df not found in legislators_df. Example: {missing_legislators[:5]}")
                            # overall_valid = False # Decide if this is critical
            except KeyError as e:
                 logger.error(f"KeyError during Votes<->Legislators ID consistency check: {e}")
                 overall_valid = False
            except Exception as e:
                 logger.error(f"Error during Votes<->Legislators ID consistency check: {e}", exc_info=True)
                 # Don't necessarily invalidate overall, but log the error

        # Check Votes vs Roll Calls
        if self.votes_df is not None and not self.votes_df.empty and \
           self.roll_calls_df is not None and not self.roll_calls_df.empty:
            try:
                if 'roll_call_id' not in self.votes_df.columns or \
                   'roll_call_id' not in self.roll_calls_df.columns:
                    logger.warning("Skipping Votes<->RollCalls ID check: missing 'roll_call_id' column.")
                else:
                    votes_rc_dtype_str = pd.api.types.infer_dtype(self.votes_df['roll_call_id'])
                    master_rc_dtype_str = pd.api.types.infer_dtype(self.roll_calls_df['roll_call_id'])

                    if votes_rc_dtype_str == 'empty' or master_rc_dtype_str == 'empty':
                         logger.warning("Skipping Votes<->RollCalls ID check: 'roll_call_id' column is effectively empty.")
                    else:
                        common_rc_dtype = np.result_type(self.votes_df['roll_call_id'].dtype, self.roll_calls_df['roll_call_id'].dtype)
                        rc_ids_votes = self.votes_df['roll_call_id'].dropna().astype(common_rc_dtype).unique()
                        rc_ids_master = self.roll_calls_df['roll_call_id'].dropna().astype(common_rc_dtype).unique()
                        missing_roll_calls = np.setdiff1d(rc_ids_votes, rc_ids_master)
                        if len(missing_roll_calls) > 0:
                            logger.warning(f"{len(missing_roll_calls)} roll_call_ids in votes_df not found in roll_calls_df. Example: {missing_roll_calls[:5]}")
                            # overall_valid = False
            except KeyError as e:
                 logger.error(f"KeyError during Votes<->RollCalls ID consistency check: {e}")
                 overall_valid = False
            except Exception as e:
                 logger.error(f"Error during Votes<->RollCalls ID consistency check: {e}", exc_info=True)

        if overall_valid:
            logger.info("Data validation completed successfully.")
        else:
            logger.error("Data validation finished with warnings or errors. Please review logs carefully before proceeding.")
        # Return the overall validity status, which might be False due to missing required columns
        # but the pipeline might continue if the subsequent steps can handle it.
        return overall_valid

    def clean_data(self) -> bool:
        """Clean and standardize data types and values."""
        logger.info("Starting data cleaning...")
        try:
            # --- Ensure Critical DataFrames Exist ---
            if self.bills_df is None:
                 logger.warning("Bills DataFrame is None. Skipping Bills cleaning.")
            else:
                # --- Bills Cleaning ---
                logger.debug("Cleaning Bills DataFrame...")
                if 'status' in self.bills_df.columns:
                     self.bills_df['status_desc'] = self.bills_df['status'].map(STATUS_CODES).fillna('Unknown Status')
                else:
                    logger.warning("'status' column missing in Bills DF, cannot create 'status_desc'.")
                
                # Handle dates with more robust parsing
                date_cols_bills = ['date_introduced', 'date_last_action']
                for col in date_cols_bills:
                    if col in self.bills_df.columns:
                        self.bills_df[col] = pd.to_datetime(self.bills_df[col], errors='coerce')
                        # Log number of invalid dates
                        invalid_dates = self.bills_df[col].isna().sum()
                        if invalid_dates > 0:
                            logger.warning(f"Found {invalid_dates} invalid dates in {col}")
                    else:
                         logger.warning(f"Date column '{col}' missing in Bills DF.")
                
                # Ensure bill_id is correct type and handle invalid values
                if 'bill_id' in self.bills_df.columns:
                    self.bills_df['bill_id'] = pd.to_numeric(self.bills_df['bill_id'], errors='coerce').astype('Int64')
                    invalid_bill_ids = self.bills_df['bill_id'].isna().sum()
                    if invalid_bill_ids > 0:
                        logger.warning(f"Found {invalid_bill_ids} invalid bill IDs")
                else:
                     logger.warning("'bill_id' column missing in Bills DF.")

                # Clean and standardize bill subjects
                if 'subjects' in self.bills_df.columns:
                    self.bills_df['subjects'] = self.bills_df['subjects'].fillna('')
                    self.bills_df['subjects'] = self.bills_df['subjects'].str.strip().str.lower()
                    self.bills_df['num_subjects'] = self.bills_df['subjects'].str.split(';').str.len()
                else:
                     logger.warning("'subjects' column missing in Bills DF.")

            # --- Roll Calls Cleaning (Check if exists) ---
            if self.roll_calls_df is None:
                 logger.warning("Roll Calls DataFrame is None. Skipping Roll Calls cleaning.")
            else:
                logger.debug("Cleaning Roll Calls DataFrame...")
                if 'date' in self.roll_calls_df.columns:
                    self.roll_calls_df['vote_date'] = pd.to_datetime(self.roll_calls_df['date'], errors='coerce')
                    invalid_dates = self.roll_calls_df['vote_date'].isna().sum()
                    if invalid_dates > 0:
                        logger.warning(f"Found {invalid_dates} invalid vote dates")
                else:
                    logger.warning("Roll Calls DataFrame missing 'date' column. Cannot create 'vote_date'.")

                # Ensure roll_call_id and bill_id types are consistent for merging
                if 'roll_call_id' in self.roll_calls_df.columns:
                    self.roll_calls_df['roll_call_id'] = pd.to_numeric(self.roll_calls_df['roll_call_id'], errors='coerce').astype('Int64')
                if 'bill_id' in self.roll_calls_df.columns:
                    self.roll_calls_df['bill_id'] = pd.to_numeric(self.roll_calls_df['bill_id'], errors='coerce').astype('Int64')
                
                # Clean vote counts
                vote_count_cols = ['yea', 'nay', 'absent', 'excused']
                for col in vote_count_cols:
                    if col in self.roll_calls_df.columns:
                        self.roll_calls_df[col] = pd.to_numeric(self.roll_calls_df[col], errors='coerce').fillna(0).astype('Int64')

            # --- Votes Cleaning (Check if exists) ---
            if self.votes_df is None:
                 logger.warning("Votes DataFrame is None. Skipping Votes cleaning.")
            else:
                logger.debug("Cleaning Votes DataFrame...")
                # Map vote text to standardized values
                if 'vote_text' in self.votes_df.columns:
                    self.votes_df['vote_value'] = self.votes_df['vote_text'].map(VOTE_TEXT_MAP).fillna(-2)  # -2 for unknown/absent
                else:
                     logger.warning("'vote_text' column missing in Votes DF. Cannot create 'vote_value'.")
                
                # Ensure legislator_id and roll_call_id are correct types
                if 'legislator_id' in self.votes_df.columns:
                     self.votes_df['legislator_id'] = pd.to_numeric(self.votes_df['legislator_id'], errors='coerce').astype('Int64')
                     invalid_leg_ids = self.votes_df['legislator_id'].isna().sum()
                     if invalid_leg_ids > 0:
                         logger.warning(f"Found {invalid_leg_ids} invalid legislator IDs in votes")
                if 'roll_call_id' in self.votes_df.columns:
                     self.votes_df['roll_call_id'] = pd.to_numeric(self.votes_df['roll_call_id'], errors='coerce').astype('Int64')
                     invalid_rc_ids = self.votes_df['roll_call_id'].isna().sum()
                     if invalid_rc_ids > 0:
                         logger.warning(f"Found {invalid_rc_ids} invalid roll call IDs in votes")

            # --- Legislators Cleaning (Check if exists) ---
            if self.legislators_df is None:
                 logger.warning("Legislators DataFrame is None. Skipping Legislators cleaning.")
            else:
                logger.debug("Cleaning Legislators DataFrame...")
                # Ensure legislator_id is correct type
                if 'legislator_id' in self.legislators_df.columns:
                     self.legislators_df['legislator_id'] = pd.to_numeric(self.legislators_df['legislator_id'], errors='coerce').astype('Int64')
                
                # Clean and standardize party information
                if 'party_id' in self.legislators_df.columns:
                    self.legislators_df['party_id'] = pd.to_numeric(self.legislators_df['party_id'], errors='coerce').astype('Int64')
                    invalid_party_ids = self.legislators_df['party_id'].isna().sum()
                    if invalid_party_ids > 0:
                        logger.warning(f"Found {invalid_party_ids} invalid party IDs")

                # Clean and standardize names
                if 'name' in self.legislators_df.columns:
                    self.legislators_df['name'] = self.legislators_df['name'].str.strip()
                    self.legislators_df['name'] = self.legislators_df['name'].str.title()

            # --- Committee Membership Cleaning (Check if exists) ---
            if self.committee_membership_df is None:
                logger.warning("Committee Membership DataFrame is None. Skipping Committee Membership cleaning.")
            elif not self.committee_membership_df.empty:
                logger.debug("Cleaning Committee Membership DataFrame...")
                # Ensure IDs are correct types (Check if columns exist)
                if 'committee_id' in self.committee_membership_df.columns:
                    self.committee_membership_df['committee_id'] = pd.to_numeric(self.committee_membership_df['committee_id'], errors='coerce').astype('Int64')
                if 'legislator_id' in self.committee_membership_df.columns:
                    self.committee_membership_df['legislator_id'] = pd.to_numeric(self.committee_membership_df['legislator_id'], errors='coerce').astype('Int64')
                
                # Clean and standardize roles (Check if column exists)
                if 'role' in self.committee_membership_df.columns:
                    self.committee_membership_df['role'] = self.committee_membership_df['role'].str.strip().str.title()
                    self.committee_membership_df['is_leader'] = self.committee_membership_df['role'].str.contains('Chair|Leader', case=False)
            else:
                 logger.debug("Committee Membership DataFrame is empty. Skipping cleaning.")

            logger.info("Data cleaning completed successfully.")
            return True

        except Exception as e:
            logger.error(f"Error during data cleaning: {str(e)}", exc_info=True)
            return False

    def engineer_features(self) -> bool:
        """Engineer features for predictive modeling."""
        logger.info("Starting feature engineering...")
        try:
            # --- Subject Vector Feature ---
            logger.debug("Creating subject vectors...")
            if 'subjects' in self.bills_df.columns:
                # Clean and prepare subject text
                subject_texts = self.bills_df['subjects'].fillna('').str.replace(';', ' ')
                
                # Log check for empty subject strings before vectorization
                empty_subject_mask = (subject_texts.str.strip() == '')
                num_empty_subjects = empty_subject_mask.sum()
                if num_empty_subjects > 0:
                    logger.warning(f"Found {num_empty_subjects} bills with effectively empty subject strings before TF-IDF vectorization.")
                    # Example logging of affected bill IDs (optional, can be verbose)
                    # if logger.isEnabledFor(logging.DEBUG):
                    #     empty_subject_bill_ids = self.bills_df.loc[empty_subject_mask, 'bill_id'].tolist()
                    #     logger.debug(f"Bill IDs with empty subjects (sample): {empty_subject_bill_ids[:10]}")

                # Create and fit TF-IDF vectorizer
                vectorizer = TfidfVectorizer(
                    lowercase=True,
                    stop_words='english',
                    max_features=1000,
                    ngram_range=(1, 2)  # Include bigrams
                )
                
                try:
                    # Transform subjects to TF-IDF vectors
                    subject_vectors = vectorizer.fit_transform(subject_texts)
                    
                    # Convert to DataFrame and add to bills_df
                    subject_df = pd.DataFrame(
                        subject_vectors.toarray(),
                        columns=[f'subject_{i}' for i in range(subject_vectors.shape[1])]
                    )
                    self.bills_df = pd.concat([self.bills_df, subject_df], axis=1)
                    logger.info(f"Created {subject_vectors.shape[1]} subject vector features")
                except ValueError as e:
                    if "empty vocabulary" in str(e):
                         logger.warning("TF-IDF vectorizer failed due to empty vocabulary (likely empty input). Skipping subject vector creation.")
                         # Create empty columns to maintain schema if needed, or just skip
                         # subject_cols = [f'subject_{i}' for i in range(1000)] # Assume max_features
                         # for col in subject_cols:
                         #     if col not in self.bills_df.columns: self.bills_df[col] = 0
                    else:
                         logger.error(f"ValueError during TF-IDF vectorization: {e}", exc_info=True)
                         # Propagate other ValueErrors
                         raise e
                except Exception as e:
                     logger.error(f"Unexpected error during TF-IDF vectorization: {e}", exc_info=True)
                     # Propagate other exceptions
                     raise e
            else:
                logger.warning("'subjects' column not found in Bills DataFrame. Skipping subject vector creation.")

            # --- Influence Score Feature ---
            logger.debug("Calculating influence scores...")
            if self.legislators_df is not None and self.bills_df is not None and self.committee_membership_df is not None:
                # Initialize influence scores
                self.legislators_df['influence_score'] = 0.0
                
                # Calculate leadership score (0-100)
                leadership_roles = ['Speaker', 'Majority Leader', 'Minority Leader', 'Whip', 'Caucus Chair']
                self.legislators_df['leadership_score'] = self.legislators_df['role'].apply(
                    lambda x: 100 if x in leadership_roles[:3] else 50 if x in leadership_roles[3:] else 0
                )
                
                # Calculate bill success rate (0-100)
                if 'sponsors_df' in self.__dict__ and self.sponsors_df is not None:
                    # Get bills sponsored by each legislator
                    sponsored_bills = self.sponsors_df.groupby('legislator_id')['bill_id'].apply(list)
                    
                    # Calculate success rate
                    def calculate_success_rate(legislator_id):
                        if legislator_id not in sponsored_bills.index:
                            return 0
                        bill_ids = sponsored_bills[legislator_id]
                        bills = self.bills_df[self.bills_df['bill_id'].isin(bill_ids)]
                        if len(bills) == 0:
                            return 0
                        passed_bills = bills['status'].isin(['Passed', 'Chaptered']).sum()
                        return (passed_bills / len(bills)) * 100
                    
                    self.legislators_df['bill_success_score'] = self.legislators_df['legislator_id'].apply(calculate_success_rate)
                else:
                    self.legislators_df['bill_success_score'] = 0
                    logger.warning("Sponsors DataFrame not available. Setting bill success score to 0.")
                
                # Calculate committee leadership score (0-100)
                # Initialize committee score first
                self.legislators_df['committee_score'] = 0
                
                if self.committee_membership_df is not None:
                    # Check if 'role' column exists before using it
                    if 'role' in self.committee_membership_df.columns and not self.committee_membership_df.empty:
                        try:
                            committee_roles = self.committee_membership_df.groupby('legislator_id')['role'].agg(list)
                            
                            def calculate_committee_score(legislator_id):
                                if legislator_id not in committee_roles.index:
                                    return 0
                                roles = committee_roles[legislator_id]
                                if any('Chair' in role for role in roles if role is not None):
                                    return 100
                                elif any('Vice Chair' in role for role in roles if role is not None):
                                    return 50
                                elif len(roles) > 0:
                                    return 25
                                return 0
                            
                            self.legislators_df['committee_score'] = self.legislators_df['legislator_id'].apply(calculate_committee_score)
                            logger.info("Calculated committee scores.")
                        except Exception as e:
                             logger.error(f"Error calculating committee score: {e}", exc_info=True)
                             # Keep default committee_score of 0
                    else:
                         logger.warning("Committee membership DataFrame is missing 'role' column or is empty. Setting committee score to 0.")
                else:
                    logger.warning("Committee membership DataFrame not available. Setting committee score to 0.")
                
                # Calculate final influence score (weighted average)
                # Ensure all component columns exist before calculation
                required_influence_cols = ['leadership_score', 'bill_success_score', 'committee_score']
                if all(col in self.legislators_df.columns for col in required_influence_cols):
                    self.legislators_df['influence_score'] = (
                        0.3 * self.legislators_df['leadership_score'] +
                        0.4 * self.legislators_df['bill_success_score'] +
                        0.3 * self.legislators_df['committee_score']
                    ).fillna(0) # Fill potential NaNs resulting from calculation
                    logger.info("Influence scores calculated successfully")
                else:
                    missing_cols = [col for col in required_influence_cols if col not in self.legislators_df.columns]
                    logger.error(f"Cannot calculate final influence score: Missing component columns {missing_cols}. Setting influence_score to 0.")
                    self.legislators_df['influence_score'] = 0.0
            else:
                 logger.warning("Skipping influence score calculation: one or more required DataFrames (Legislators, Bills, Committee Memberships) are missing.")
                 if self.legislators_df is not None:
                      # Ensure influence score columns exist even if calculation skipped
                      for col in ['leadership_score', 'bill_success_score', 'committee_score', 'influence_score']:
                           if col not in self.legislators_df.columns:
                                self.legislators_df[col] = 0.0

            # --- Existing Feature Engineering ---
            # --- Bill Features ---
            if self.bills_df is not None and self.sponsors_df is not None:
                logger.debug("Engineering bill features (sponsor counts)...")
                try:
                    # Ensure IDs are compatible
                    self.bills_df['bill_id'] = self.bills_df['bill_id'].astype('Int64')
                    self.sponsors_df['bill_id'] = self.sponsors_df['bill_id'].astype('Int64')

                    sponsor_counts = self.sponsors_df.groupby('bill_id')['sponsor_type'].value_counts().unstack(fill_value=0)
                    # Map types robustly using SPONSOR_TYPES mapping (assuming 1: Primary, 2: Cosponsor)
                    sponsor_counts['num_primary_sponsors'] = sponsor_counts.get(1, 0) # Use .get() with default
                    sponsor_counts['num_cosponsors'] = sponsor_counts.get(2, 0)
                    # Calculate total sponsors more accurately by summing relevant types or all if needed
                    sponsor_counts['num_total_sponsors'] = sponsor_counts.sum(axis=1) # Sum all columns (includes other types)

                    # Merge counts into bills_df, overwriting if columns exist
                    feature_cols = ['num_primary_sponsors', 'num_cosponsors', 'num_total_sponsors']
                    if any(col in self.bills_df.columns for col in feature_cols):
                         self.bills_df.drop(columns=[c for c in feature_cols if c in self.bills_df.columns], inplace=True)
                    self.bills_df = self.bills_df.merge(
                        sponsor_counts[feature_cols],
                        on='bill_id',
                        how='left'
                    )
                    # Fill NaNs for bills with no sponsors and ensure integer type
                    fill_values_sponsors = {col: 0 for col in feature_cols}
                    self.bills_df.fillna(value=fill_values_sponsors, inplace=True)
                    for col in feature_cols:
                        self.bills_df[col] = self.bills_df[col].astype(int)
                    logger.info("Engineered sponsor count features for bills.")
                except KeyError as e:
                    logger.error(f"KeyError during sponsor count calculation (check bill_id/sponsor_type): {e}")
                except Exception as e:
                    logger.error(f"Error engineering sponsor features: {e}", exc_info=True)
            else:
                 logger.warning("Skipping bill sponsor features: Bills or Sponsors data missing/invalid.")

            # --- Legislator Features ---
            if self.legislators_df is not None:
                logger.debug("Engineering legislator features (seniority, committee counts)...")
                self.legislators_df['legislator_id'] = self.legislators_df['legislator_id'].astype('Int64')

                # Approximate Seniority
                if 'session_id' in self.legislators_df.columns:
                    try:
                        # Extract YYYY from start of session_id string
                        self.legislators_df['session_year'] = self.legislators_df['session_id'].str.extract(r'^(\d{4})', expand=False)
                        self.legislators_df['session_year'] = pd.to_numeric(self.legislators_df['session_year'], errors='coerce').astype('Int64')

                        if not self.legislators_df['session_year'].isnull().all():
                            # Calculate seniority based on first year seen
                            first_session = self.legislators_df.dropna(subset=['session_year']).groupby('legislator_id')['session_year'].min()
                            max_session_year = self.legislators_df['session_year'].max()
                            if pd.isna(max_session_year): max_session_year = datetime.now().year # Fallback

                            self.legislators_df['seniority_years'] = max_session_year - self.legislators_df['legislator_id'].map(first_session)
                            self.legislators_df['seniority_years'] = self.legislators_df['seniority_years'].fillna(0).astype(int)
                            logger.info("Engineered approximate legislator seniority.")
                        else:
                            logger.warning("Could not reliably extract 'session_year' for seniority calculation.")
                            if 'seniority_years' not in self.legislators_df.columns: self.legislators_df['seniority_years'] = 0
                    except Exception as e:
                        logger.error(f"Error calculating seniority: {e}", exc_info=True)
                        if 'seniority_years' not in self.legislators_df.columns: self.legislators_df['seniority_years'] = 0
                else:
                     logger.warning("'session_id' not found, cannot calculate seniority.")
                     if 'seniority_years' not in self.legislators_df.columns: self.legislators_df['seniority_years'] = 0


                # Committee Count
                if self.committee_membership_df is not None and not self.committee_membership_df.empty:
                     try:
                        self.committee_membership_df['legislator_id'] = self.committee_membership_df['legislator_id'].astype('Int64')
                        # Count distinct committees per legislator (overall or per session)
                        committee_counts = self.committee_membership_df.groupby('legislator_id')['committee_id'].nunique()

                        if 'num_committees' in self.legislators_df.columns: self.legislators_df.drop(columns=['num_committees'], inplace=True)
                        self.legislators_df = self.legislators_df.merge(
                            committee_counts.rename('num_committees'),
                            on='legislator_id',
                            how='left'
                        )
                        self.legislators_df['num_committees'] = self.legislators_df['num_committees'].fillna(0).astype(int)
                        logger.info("Engineered legislator committee count feature.")
                     except KeyError as e:
                         logger.error(f"KeyError calculating committee counts (check IDs): {e}")
                     except Exception as e:
                         logger.error(f"Error calculating committee counts: {e}", exc_info=True)
                         if 'num_committees' not in self.legislators_df.columns: self.legislators_df['num_committees'] = 0
                else:
                    logger.warning("Committee membership data not available/empty, skipping committee count feature.")
                    if 'num_committees' not in self.legislators_df.columns: self.legislators_df['num_committees'] = 0
            else:
                logger.warning("Skipping legislator features: Legislators data missing.")

            # --- Vote / Roll Call Features ---
            if self.votes_df is not None and self.legislators_df is not None and 'party_id' in self.legislators_df.columns:
                 logger.debug("Engineering vote features (party alignment)...")
                 try:
                    # Ensure IDs are compatible for merging
                    self.votes_df['legislator_id'] = self.votes_df['legislator_id'].astype('Int64')
                    self.legislators_df['legislator_id'] = self.legislators_df['legislator_id'].astype('Int64')
                    self.votes_df['roll_call_id'] = self.votes_df['roll_call_id'].astype('Int64')

                    # Get unique legislator-party mapping (handle potential duplicates/changes across sessions if needed)
                    legislator_info = self.legislators_df[['legislator_id', 'party_id']].drop_duplicates(subset=['legislator_id'])
                    # TODO: Improve handling of party changes over time if necessary

                    # Merge party onto votes
                    votes_with_party = pd.merge(self.votes_df, legislator_info, on='legislator_id', how='left')
                    votes_with_party['party_id'] = votes_with_party['party_id'].fillna('O') # Fill missing party as Other

                    # Calculate party majority vote per roll call (using cleaned vote_value: 1=Yea, 0=Nay)
                    valid_votes = votes_with_party[votes_with_party['vote_value'].isin([0, 1])].copy()
                    if not valid_votes.empty:
                        valid_votes['vote_value'] = valid_votes['vote_value'].astype(int)
                        # Calculate mode, handle ties/empty groups
                        party_mode_vote = valid_votes.groupby(['roll_call_id', 'party_id'])['vote_value']\
                                             .apply(lambda x: x.mode()[0] if not x.mode().empty else np.nan)\
                                             .reset_index()\
                                             .rename(columns={'vote_value': 'party_majority_vote'})

                        # Merge party majority back
                        votes_with_party = pd.merge(votes_with_party, party_mode_vote, on=['roll_call_id', 'party_id'], how='left')

                        # Determine if legislator voted with party
                        votes_with_party['voted_with_party'] = (
                            votes_with_party['vote_value'] == votes_with_party['party_majority_vote']
                        )
                        # Set to NA if vote wasn't Yea/Nay or no party majority existed
                        votes_with_party['voted_with_party'] = np.where(
                            (~votes_with_party['vote_value'].isin([0, 1])) | votes_with_party['party_majority_vote'].isnull(),
                            pd.NA,
                            votes_with_party['voted_with_party']
                        ).astype('boolean') # Nullable Boolean

                        # Add feature back to main votes_df, overwriting if exists
                        if 'voted_with_party' in self.votes_df.columns: self.votes_df.drop(columns=['voted_with_party'], inplace=True)
                        self.votes_df = pd.merge(
                            self.votes_df,
                            votes_with_party[['vote_id', 'voted_with_party']],
                            on='vote_id',
                            how='left'
                        )
                        logger.info("Engineered 'voted_with_party' feature.")
                    else:
                        logger.warning("No valid Yea/Nay votes found to calculate party alignment.")
                        if 'voted_with_party' not in self.votes_df.columns:
                             self.votes_df['voted_with_party'] = pd.NA
                             self.votes_df['voted_with_party'] = self.votes_df['voted_with_party'].astype('boolean')
                 except KeyError as e:
                      logger.error(f"KeyError during party alignment calculation (check IDs): {e}")
                 except Exception as e:
                      logger.error(f"Error engineering party alignment feature: {e}", exc_info=True)
            else:
                logger.warning("Could not calculate party alignment: Votes, Legislators, or 'party_id' missing/invalid.")
                if self.votes_df is not None and 'voted_with_party' not in self.votes_df.columns:
                     self.votes_df['voted_with_party'] = pd.NA
                     self.votes_df['voted_with_party'] = self.votes_df['voted_with_party'].astype('boolean')


            # --- Recalculate/Refine Existing Features ---
            logger.debug("Recalculating core features...")
            # Bill Success Rate (using roll call outcomes if possible)
            if self.bills_df is not None and self.roll_calls_df is not None and self.votes_df is not None:
                try:
                    # Option 1: Use roll call outcome if available (e.g., a 'passed' column)
                    if 'passed' in self.roll_calls_df.columns:
                         # Assuming 'passed' is 1 for pass, 0 for fail
                         roll_call_outcomes = self.roll_calls_df[['bill_id', 'passed']].dropna()
                         # Ensure types are numeric for aggregation
                         roll_call_outcomes['passed'] = pd.to_numeric(roll_call_outcomes['passed'], errors='coerce')
                         roll_call_outcomes = roll_call_outcomes.dropna()
                         # Aggregate outcome per bill (e.g., mean if multiple votes, or max/min)
                         # If one fail means bill fails, use min. If one pass means pass, use max. Mean for proportion.
                         bill_final_outcome = roll_call_outcomes.groupby('bill_id')['passed'].mean() # Or .min()/.max()
                         self.bills_df['success_rate'] = self.bills_df['bill_id'].map(bill_final_outcome).fillna(0.5) # Default 0.5 if no outcome data
                         logger.info("Recalculated bill success rate based on roll call outcomes.")
                    # Option 2: Fallback to average legislator vote
                    elif 'vote_value' in self.votes_df.columns:
                        yea_nay_votes = self.votes_df[self.votes_df['vote_value'].isin([0, 1])].copy()
                        if not yea_nay_votes.empty:
                             # Ensure bill_id is compatible type
                             yea_nay_votes['bill_id'] = pd.to_numeric(yea_nay_votes['bill_id'], errors='coerce').astype('Int64')
                             bill_avg_vote = yea_nay_votes.groupby('bill_id')['vote_value'].mean()
                             if 'success_rate' in self.bills_df.columns: self.bills_df.drop(columns=['success_rate'], inplace=True)
                             self.bills_df = self.bills_df.merge(bill_avg_vote.rename('success_rate'), on='bill_id', how='left')
                             self.bills_df['success_rate'] = self.bills_df['success_rate'].fillna(0.5) # Fill NaN with neutral 0.5
                             logger.info("Recalculated bill success rate based on average legislator vote (fallback).")
                        else:
                             logger.warning("No Yea/Nay votes found for fallback success rate calculation.")
                             if 'success_rate' not in self.bills_df.columns: self.bills_df['success_rate'] = 0.5
                    else:
                         logger.warning("Cannot calculate bill success rate: No 'passed' column in roll calls and no 'vote_value' in votes.")
                         if 'success_rate' not in self.bills_df.columns: self.bills_df['success_rate'] = 0.5
                except Exception as e:
                    logger.error(f"Error calculating bill success rate: {e}", exc_info=True)
                    if self.bills_df is not None and 'success_rate' not in self.bills_df.columns: self.bills_df['success_rate'] = 0.5


            # Days to First Vote
            if self.bills_df is not None and self.votes_df is not None and 'vote_date' in self.votes_df.columns and 'date_introduced' in self.bills_df.columns:
                 try:
                     valid_bills = self.bills_df.dropna(subset=['date_introduced', 'bill_id'])
                     valid_votes = self.votes_df.dropna(subset=['vote_date', 'bill_id'])
                     if not valid_bills.empty and not valid_votes.empty:
                         first_votes = valid_votes.groupby('bill_id')['vote_date'].min().rename('first_vote_date')
                         if 'first_vote_date' in valid_bills.columns: valid_bills.drop(columns=['first_vote_date'], inplace=True)
                         bills_with_first_vote = valid_bills.merge(first_votes, on='bill_id', how='left')

                         bills_with_first_vote['days_to_first_vote'] = (
                             bills_with_first_vote['first_vote_date'] - bills_with_first_vote['date_introduced']
                         ).dt.days
                         bills_with_first_vote['days_to_first_vote'] = bills_with_first_vote['days_to_first_vote'].astype('Int64')

                         # Merge back into original bills_df
                         if 'days_to_first_vote' in self.bills_df.columns: self.bills_df.drop(columns=['days_to_first_vote'], inplace=True)
                         self.bills_df = self.bills_df.merge(bills_with_first_vote[['bill_id', 'days_to_first_vote']], on='bill_id', how='left')
                         logger.info("Recalculated days to first vote feature.")
                     else:
                        logger.warning("Missing valid dates/IDs for days_to_first_vote calculation.")
                        if 'days_to_first_vote' not in self.bills_df.columns: self.bills_df['days_to_first_vote'] = pd.NA
                 except Exception as e:
                    logger.error(f"Error calculating days to first vote: {e}", exc_info=True)
                    if self.bills_df is not None and 'days_to_first_vote' not in self.bills_df.columns: self.bills_df['days_to_first_vote'] = pd.NA


            # Legislator Voting Patterns
            if self.legislators_df is not None and self.votes_df is not None and 'vote_value' in self.votes_df.columns:
                 try:
                    valid_leg_votes = self.votes_df.dropna(subset=['legislator_id', 'vote_value'])
                    if not valid_leg_votes.empty:
                        yea_nay_leg_votes = valid_leg_votes[valid_leg_votes['vote_value'].isin([0, 1])].copy()
                        yea_nay_leg_votes['vote_value'] = yea_nay_leg_votes['vote_value'].astype(int)

                        if not yea_nay_leg_votes.empty:
                            legislator_vote_stats = yea_nay_leg_votes.groupby('legislator_id')['vote_value'].agg(
                                vote_agreement_rate = 'mean',        # Mean of 0s and 1s (Yea rate)
                                total_votes_cast = 'count',       # Count Yea/Nay only
                                vote_consistency_std = 'std'        # Std Dev of Yea/Nay
                            ).fillna({'vote_consistency_std': 0}) # Fill std NaN (single vote) with 0
                        else: # Handle legislators with no Yea/Nay votes
                             unique_legs = valid_leg_votes['legislator_id'].unique()
                             legislator_vote_stats = pd.DataFrame(index=unique_legs)
                             legislator_vote_stats['vote_agreement_rate'] = 0.5
                             legislator_vote_stats['total_votes_cast'] = 0
                             legislator_vote_stats['vote_consistency_std'] = 0.0

                        # Total recorded votes (including non-voting like -1, -2, -99)
                        total_recorded = valid_leg_votes.groupby('legislator_id')['vote_id'].count().rename('total_votes_recorded')
                        legislator_vote_stats = legislator_vote_stats.merge(total_recorded, left_index=True, right_index=True, how='left')
                        legislator_vote_stats['total_votes_recorded'] = legislator_vote_stats['total_votes_recorded'].fillna(0)

                        # Merge stats back, overwriting old columns
                        stats_cols = legislator_vote_stats.columns
                        if any(col in self.legislators_df.columns for col in stats_cols):
                             self.legislators_df.drop(columns=[c for c in stats_cols if c in self.legislators_df.columns], inplace=True)
                        self.legislators_df = self.legislators_df.merge(
                            legislator_vote_stats,
                            on='legislator_id',
                            how='left'
                        )

                        # Fill NaNs for legislators who had no votes AT ALL (weren't in votes_df)
                        fill_values_voting = {
                             'vote_agreement_rate': 0.5, 'total_votes_cast': 0,
                             'total_votes_recorded': 0, 'vote_consistency_std': 0.0
                        }
                        self.legislators_df.fillna(fill_values_voting, inplace=True)
                        self.legislators_df[['total_votes_cast', 'total_votes_recorded']] = self.legislators_df[['total_votes_cast', 'total_votes_recorded']].astype(int)

                        logger.info("Recalculated legislator voting pattern features.")
                    else:
                        logger.warning("No valid votes found to calculate legislator voting patterns.")
                        # Ensure columns exist with defaults
                        default_voting_cols = {'vote_agreement_rate': 0.5, 'total_votes_cast': 0, 'total_votes_recorded': 0, 'vote_consistency_std': 0.0}
                        for col, default in default_voting_cols.items():
                            if col not in self.legislators_df.columns: self.legislators_df[col] = default
                 except Exception as e:
                     logger.error(f"Error calculating legislator voting patterns: {e}", exc_info=True)


            logger.info("Feature engineering completed successfully.")
            return True
        except Exception as e:
            logger.error(f"Error during feature engineering: {str(e)}", exc_info=True)
            return False

    def validate_features(self) -> bool:
        """Validate engineered features for quality and consistency."""
        logger.info("Starting feature validation...")
        try:
            validation_passed = True
            
            # --- Subject Vector Validation ---
            if 'subjects' in self.bills_df.columns:
                subject_cols = [col for col in self.bills_df.columns if col.startswith('subject_')]
                if subject_cols:
                    # Check for NaN values
                    nan_count = self.bills_df[subject_cols].isna().sum().sum()
                    if nan_count > 0:
                        logger.warning(f"Found {nan_count} NaN values in subject vectors")
                    
                    # Check for zero vectors
                    zero_vectors = (self.bills_df[subject_cols] == 0).all(axis=1).sum()
                    if zero_vectors > 0:
                        logger.warning(f"Found {zero_vectors} bills with zero subject vectors")
                    
                    # Log feature statistics
                    logger.info(f"Subject vector statistics:\n{self.bills_df[subject_cols].describe()}")

            # --- Influence Score Validation ---
            if 'influence_score' in self.legislators_df.columns:
                # Check score range
                min_score = self.legislators_df['influence_score'].min()
                max_score = self.legislators_df['influence_score'].max()
                if min_score < 0 or max_score > 100:
                    logger.error(f"Influence scores out of range: min={min_score}, max={max_score}")
                    validation_passed = False
                
                # Check component scores
                for score_col in ['leadership_score', 'bill_success_score', 'committee_score']:
                    if score_col in self.legislators_df.columns:
                        min_val = self.legislators_df[score_col].min()
                        max_val = self.legislators_df[score_col].max()
                        if min_val < 0 or max_val > 100:
                            logger.error(f"{score_col} out of range: min={min_val}, max={max_val}")
                            validation_passed = False
                
                # Log score distribution
                logger.info(f"Influence score distribution:\n{self.legislators_df['influence_score'].describe()}")

            # --- Feature Correlation Check ---
            if 'influence_score' in self.legislators_df.columns:
                # Check correlation between influence components
                score_cols = ['leadership_score', 'bill_success_score', 'committee_score']
                if all(col in self.legislators_df.columns for col in score_cols):
                    correlations = self.legislators_df[score_cols].corr()
                    logger.info(f"Correlation between influence components:\n{correlations}")
                    
                    # Check for high correlations that might indicate redundancy
                    high_corr = (correlations.abs() > 0.8) & (correlations.abs() < 1.0)
                    if high_corr.any().any():
                        logger.warning("High correlations detected between influence components")

            # --- Feature Completeness Check ---
            required_features = {
                'bills_df': ['subject_0', 'num_subjects'],  # Example required features
                'legislators_df': ['influence_score', 'leadership_score', 'bill_success_score', 'committee_score']
            }
            
            for df_name, features in required_features.items():
                df = getattr(self, df_name, None)
                if df is not None:
                    missing_features = [f for f in features if f not in df.columns]
                    if missing_features:
                        logger.error(f"Missing required features in {df_name}: {missing_features}")
                        validation_passed = False

            if validation_passed:
                logger.info("Feature validation completed successfully")
            else:
                logger.error("Feature validation completed with errors")
            
            return validation_passed

        except Exception as e:
            logger.error(f"Error during feature validation: {str(e)}", exc_info=True)
            return False

    def save_processed_data(self) -> bool:
        """Save the processed and feature-engineered dataframes to the processed directory."""
        logger.info(f"Saving processed data to: {self.processed_dir}")
        all_saved = True

        def _save_csv(df: Optional[pd.DataFrame], filename: str):
            nonlocal all_saved
            if df is not None:
                path = self.processed_dir / filename
                try:
                    # Use consistent NaN representation and UTF-8 encoding
                    df.to_csv(path, index=False, na_rep='NA', encoding='utf-8')
                    logger.info(f"Saved {len(df):,} records to {filename}")
                except Exception as e:
                    logger.error(f"Error saving {filename}: {str(e)}", exc_info=True)
                    all_saved = False
            else:
                logger.warning(f"DataFrame for {filename} is None, skipping save.")

        # Save all dataframes that might have been modified or loaded
        _save_csv(self.bills_df, 'processed_bills.csv')
        _save_csv(self.votes_df, 'processed_votes.csv')
        _save_csv(self.legislators_df, 'processed_legislators.csv')
        _save_csv(self.sponsors_df, 'processed_sponsors.csv')
        _save_csv(self.committees_df, 'processed_committees.csv') # Save if loaded
        _save_csv(self.roll_calls_df, 'processed_roll_calls.csv') # Save if loaded/modified
        # Optionally save others if they were processed/modified
        # _save_csv(self.committee_membership_df, 'processed_committee_memberships.csv')
        # _save_csv(self.finance_df, 'processed_finance.csv')

        if not all_saved:
            logger.error("One or more files failed to save.")
        else:
            logger.info("All processed data frames saved successfully.")
        return all_saved

    def process_all(self) -> bool:
        """Run the complete preprocessing pipeline."""
        logger.info("Starting complete preprocessing pipeline...")
        final_success = True # Track overall success
        
        # Load data
        if not self.load_all_data():
            logger.error("Failed to load data")
            # Do not return False immediately, allow processing to continue if possible
            final_success = False
        
        # Validate data structure
        if not self.validate_data():
            logger.error("Data validation failed")
            # Do not return False, just log the error and continue
            final_success = False
        
        # Clean data (Check if critical DFs are present before attempting)
        if self.bills_df is None or self.votes_df is None or self.legislators_df is None:
             logger.critical("Cannot proceed with cleaning: One or more critical DataFrames (Bills, Votes, Legislators) are None.")
             final_success = False
        elif not self.clean_data():
            logger.error("Data cleaning failed")
            final_success = False
        
        # Engineer features (Check if critical DFs are present)
        if self.bills_df is None:
            logger.critical("Cannot proceed with feature engineering: Bills DataFrame is None.")
            final_success = False
        elif not self.engineer_features():
            logger.error("Feature engineering failed")
            final_success = False
        
        # Validate features (Check if critical DFs are present)
        if self.bills_df is None or self.legislators_df is None:
             logger.critical("Cannot proceed with feature validation: Bills or Legislators DataFrame is None.")
             final_success = False
        elif not self.validate_features():
            logger.error("Feature validation failed")
            final_success = False
        
        # Save processed data (Attempt even if steps failed, might save partial results)
        if not self.save_processed_data():
            logger.error("Failed to save processed data")
            final_success = False
        
        if final_success:
            logger.info("Preprocessing pipeline completed successfully")
        else:
            logger.error("Preprocessing pipeline completed with one or more errors.")
            
        return final_success

    def create_feature_matrix(self, filename: str = 'voting_feature_matrix.csv') -> bool:
        """Merges processed dataframes to create the final feature matrix for modeling.

        The grain of the matrix is typically (legislator_id, roll_call_id).
        
        Args:
            filename: The name for the output CSV file.
            
        Returns:
            bool: True if the matrix was created and saved successfully.
        """
        logger.info("Starting feature matrix creation...")

        # --- Check if necessary dataframes are loaded --- 
        required_dfs = {
            'votes_df': self.votes_df, 
            'legislators_df': self.legislators_df,
            'roll_calls_df': self.roll_calls_df, 
            'bills_df': self.bills_df
        }
        
        missing_critical = False
        for name, df in required_dfs.items():
            if df is None:
                logger.error(f"Cannot create feature matrix: Required DataFrame '{name}' is missing.")
                missing_critical = True
        if missing_critical:
            return False

        try:
            # --- Start with the votes data (core of the matrix) ---
            logger.debug("Starting merge process with votes_df.")
            # Select relevant columns from votes initially
            matrix_df = self.votes_df[['vote_id', 'roll_call_id', 'legislator_id', 'vote_value', 'vote_date', 'voted_with_party']].copy()
            # Ensure vote_value is the target (typically 0 or 1)
            matrix_df = matrix_df[matrix_df['vote_value'].isin([0, 1])] # Filter for valid votes only
            if matrix_df.empty:
                logger.error("No valid votes (0 or 1) found in votes_df. Cannot create feature matrix.")
                return False
            logger.info(f"Filtered votes_df to {len(matrix_df):,} valid voting instances (Yea/Nay). Target: 'vote_value'.")

            # --- Merge Legislator Features ---
            logger.debug("Merging legislator features...")
            legislator_features = [
                'legislator_id', 'party_id', 'seniority_years', 'num_committees',
                'vote_agreement_rate', 'total_votes_cast', 'total_votes_recorded', 'vote_consistency_std'
                # Add other relevant legislator features here
            ]
            # Ensure legislator_id is of compatible type
            self.legislators_df['legislator_id'] = self.legislators_df['legislator_id'].astype(matrix_df['legislator_id'].dtype)
            # Select unique legislators if multiple sessions exist, maybe take latest info? Or merge needs session key too?
            # For now, assume legislators_df has one relevant entry per legislator_id or handle duplicates
            unique_legislators = self.legislators_df[legislator_features].drop_duplicates(subset=['legislator_id'], keep='last') # Keep latest record if duplicates
            
            matrix_df = pd.merge(
                matrix_df,
                unique_legislators,
                on='legislator_id',
                how='left',
                suffixes=('', '_leg') # Suffix if columns conflict (shouldn't with selection)
            )
            if matrix_df['party_id'].isnull().any(): # Check if merge failed for some legislators
                logger.warning(f"{matrix_df['party_id'].isnull().sum()} rows failed to merge legislator features.")

            # --- Merge Roll Call & Bill Features ---
            logger.debug("Merging roll call and bill features...")
            # Select features from roll calls and bills
            roll_call_features = ['roll_call_id', 'bill_id', 'description', 'chamber', 'motion'] # Add relevant roll call columns
            bill_features = [
                'bill_id', 'status_desc', 'date_introduced',
                'num_primary_sponsors', 'num_cosponsors', 'num_total_sponsors',
                'success_rate', 'days_to_first_vote'
                # Add bill subject/category features if available
            ]
            
            # Ensure IDs are compatible
            self.roll_calls_df['roll_call_id'] = self.roll_calls_df['roll_call_id'].astype(matrix_df['roll_call_id'].dtype)
            self.roll_calls_df['bill_id'] = self.roll_calls_df['bill_id'].astype('Int64')
            self.bills_df['bill_id'] = self.bills_df['bill_id'].astype('Int64')

            # Merge roll call info first (to get bill_id)
            relevant_roll_calls = self.roll_calls_df[[col for col in roll_call_features if col in self.roll_calls_df.columns]].drop_duplicates(subset=['roll_call_id'])
            matrix_df = pd.merge(
                matrix_df,
                relevant_roll_calls,
                on='roll_call_id',
                how='left',
                suffixes=('', '_rc')
            )
            if matrix_df['bill_id'].isnull().any():
                logger.warning(f"{matrix_df['bill_id'].isnull().sum()} rows failed to merge roll call features (or roll call had no bill_id).")

            # Merge bill info using the bill_id obtained from roll calls
            relevant_bills = self.bills_df[[col for col in bill_features if col in self.bills_df.columns]].drop_duplicates(subset=['bill_id'])
            matrix_df = pd.merge(
                matrix_df,
                relevant_bills,
                on='bill_id',
                how='left',
                suffixes=('', '_bill') # Suffix needed if date_introduced conflicts etc.
            )
            # Check merge success based on a required bill column
            if 'status_desc' in matrix_df and matrix_df['status_desc'].isnull().any():
                 num_null_status = matrix_df[matrix_df['bill_id'].notnull() & matrix_df['status_desc'].isnull()].shape[0]
                 if num_null_status > 0:
                     logger.warning(f"{num_null_status} rows with valid bill_id failed to merge bill features.")

            # --- Final Feature Selection & Cleaning --- 
            logger.debug("Selecting final features and performing final cleaning...")
            # Define final columns (adjust based on available and desired features)
            final_columns = [
                # Identifiers
                'vote_id', 'roll_call_id', 'legislator_id', 'bill_id',
                # Target Variable
                'vote_value',
                # Legislator Features
                'party_id', 'seniority_years', 'num_committees', 'vote_agreement_rate', 
                'total_votes_cast', 'total_votes_recorded', 'vote_consistency_std', 'voted_with_party',
                # Bill/Roll Call Features
                'vote_date', 'status_desc', 'date_introduced', 'num_primary_sponsors',
                'num_cosponsors', 'num_total_sponsors', 'success_rate', 'days_to_first_vote',
                # Optional roll call details
                'chamber', 'motion' # Add description if useful and cleaned
            ]
            # Filter matrix to only include existing columns from the final_columns list
            existing_final_columns = [col for col in final_columns if col in matrix_df.columns]
            matrix_df = matrix_df[existing_final_columns]

            # Handle missing values (imputation strategy depends on model)
            # Example: Fill numeric NaNs with median/mean, categorical with mode or 'Unknown'
            numeric_cols = matrix_df.select_dtypes(include=np.number).columns
            for col in numeric_cols:
                 if matrix_df[col].isnull().any():
                     median_val = matrix_df[col].median()
                     matrix_df[col].fillna(median_val, inplace=True)
                     logger.debug(f"Filled NaNs in numeric column '{col}' with median ({median_val}).")
            
            categorical_cols = matrix_df.select_dtypes(include=['object', 'category']).columns
            for col in categorical_cols:
                 if matrix_df[col].isnull().any():
                     mode_val = matrix_df[col].mode()[0] if not matrix_df[col].mode().empty else 'Unknown'
                     matrix_df[col].fillna(mode_val, inplace=True)
                     logger.debug(f"Filled NaNs in categorical column '{col}' with mode ({mode_val}).")
            
            # Convert boolean columns explicitly if needed (after filling NA)
            bool_cols = matrix_df.select_dtypes(include=['boolean']).columns
            for col in bool_cols:
                matrix_df[col] = matrix_df[col].fillna(False).astype(bool) # Example: fill NA bools with False

            # Ensure target variable 'vote_value' is integer (0 or 1)
            matrix_df['vote_value'] = matrix_df['vote_value'].astype(int)

            # --- Save the Feature Matrix ---
            output_path = self.processed_dir / filename
            logger.info(f"Saving final feature matrix ({len(matrix_df):,} rows, {len(matrix_df.columns)} columns) to: {output_path}")
            matrix_df.to_csv(output_path, index=False, na_rep='NA', encoding='utf-8')
            logger.info("Feature matrix created successfully.")
            return True

        except KeyError as e:
             logger.critical(f"KeyError during feature matrix creation: {e}. Check column names and merge steps.", exc_info=True)
             return False
        except Exception as e:
             logger.critical(f"Error creating feature matrix: {str(e)}", exc_info=True)
             return False

# Custom exception for pipeline control
class PipelineError(Exception):
    pass

def main():
    """Main function to instantiate and run the data preprocessing pipeline."""
    logger.info("Executing main function of data_preprocessing.py")
    # Option to override base data dir if needed, e.g., from CLI args in a real app
    # preprocessor = DataPreprocessor(base_data_dir='/path/to/your/data')
    preprocessor = DataPreprocessor()
    success = preprocessor.process_all()

    if success:
        logger.info("Data preprocessing main execution completed successfully.")
        # Indicate success for automation/scripting
        # sys.exit(0)
    else:
        logger.error("Data preprocessing main execution failed.")
        # Indicate failure
        # sys.exit(1)

if __name__ == "__main__":
    main()