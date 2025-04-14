# Standard library imports
import logging
import json
import time
import random
from datetime import datetime
import re
from pathlib import Path
from typing import List, Dict, Any, Optional, Iterable, Union, Tuple

# Third-party imports
import requests
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log
)

# Local imports
from .config import (
    FINANCE_API_KEY,
    FINANCE_BASE_URL,
    FINANCE_MAX_RETRIES,
    FINANCE_DEFAULT_WAIT_SECONDS,
    DATA_COLLECTION_LOG_FILE,
)
from .utils import (
    setup_logging,
    save_json,
    convert_to_csv,
    fetch_page,
    load_json,
    clean_name,
    ensure_dir,
    setup_project_paths
)

# --- Configure Logging ---
logger = logging.getLogger(__name__)

# --- Custom Exceptions ---
class FinanceAPIError(Exception):
    """Custom exception for API errors during finance data collection."""
    pass

class FinanceRateLimitError(Exception):
    """Custom exception for rate limiting during finance data requests."""
    pass

# --- API Fetching Logic ---
@retry(
    stop=stop_after_attempt(FINANCE_MAX_RETRIES),
    wait=wait_exponential(multiplier=1.5, min=2, max=60),
    retry=retry_if_exception_type((requests.exceptions.RequestException, FinanceRateLimitError)),
    before_sleep=before_sleep_log(logging.getLogger(), logging.WARNING)
)
def fetch_finance_data(endpoint: str, params: Dict[str, Any], wait_time: Optional[float] = None) -> Optional[Dict]:
    """
    Fetch finance data from API with robust retry logic and error handling.

    Args:
        endpoint: API endpoint to fetch from.
        params: API parameters for the request.
        wait_time: Optional override for wait time before this specific call.

    Returns:
        Dictionary containing the JSON response data, or None on failure.

    Raises:
        FinanceRateLimitError: If rate limit is hit (triggers retry).
        requests.exceptions.RequestException: For network issues (triggers retry).
    """
    if not FINANCE_API_KEY:
        logger.error("Cannot fetch finance data: FINANCE_API_KEY is not set.")
        return None

    request_params = params.copy()
    request_params['api_key'] = FINANCE_API_KEY
    
    base_wait = wait_time if wait_time is not None else FINANCE_DEFAULT_WAIT_SECONDS
    sleep_duration = max(0.1, base_wait + random.uniform(-0.2, 0.4))
    logger.debug(f"Sleeping for {sleep_duration:.2f}s before finance API request")
    time.sleep(sleep_duration)

    try:
        logger.info(f"Fetching finance data from endpoint: {endpoint}")
        log_params = {k: v for k, v in request_params.items() if k != 'api_key'}
        logger.debug(f"Request params (api_key omitted): {log_params}")

        url = f"{FINANCE_BASE_URL}/{endpoint}"
        response = requests.get(url, params=request_params, timeout=30, headers={'Accept': 'application/json'})

        if response.status_code == 429:
            logger.warning(f"Finance API rate limit hit (HTTP 429). Backing off...")
            raise FinanceRateLimitError("Rate limit exceeded")

        response.raise_for_status()

        try:
            data = response.json()
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON response from finance API. Status: {response.status_code}")
            return None

        if data.get('status') == 'error':
            error_msg = data.get('error', {}).get('message', 'Unknown finance API error')
            logger.error(f"Finance API error: {error_msg}")
            return None
        
        logger.debug(f"Successfully fetched finance data from endpoint: {endpoint}")
        return data

    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code if e.response is not None else 'N/A'
        logger.error(f"HTTP error {status_code} fetching finance data: {str(e)}")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"Request exception fetching finance data: {str(e)}")
        raise

def get_candidates_list(year: int, state: str) -> Optional[List[Dict[str, Any]]]:
    """
    Fetch the list of candidates for a specific election year and state.
    
    Args:
        year: Election year
        state: Two-letter state code
        
    Returns:
        List of candidate dictionaries or None on failure
    """
    logger.info(f"Fetching candidates list for {state}, {year}")
    
    params = {
        'year': year,
        'state': state,
        'office_type': 'legislative'
    }
    
    try:
        response = fetch_finance_data('candidates', params)
        if not response or 'candidates' not in response:
            logger.error(f"Failed to fetch valid candidates data for {state}, {year}")
            return None
            
        candidates = response.get('candidates', [])
        logger.info(f"Found {len(candidates)} candidates for {state}, {year}")
        return candidates
    except Exception as e:
        logger.error(f"Error fetching candidates list: {str(e)}", exc_info=True)
        return None

def get_candidate_contributions(candidate_id: str, year: int) -> Optional[List[Dict[str, Any]]]:
    """
    Fetch contributions for a specific candidate.
    
    Args:
        candidate_id: Unique ID for the candidate
        year: Election year
        
    Returns:
        List of contribution dictionaries or None on failure
    """
    logger.info(f"Fetching contributions for candidate ID: {candidate_id}")
    
    params = {
        'candidate_id': candidate_id,
        'year': year,
        'per_page': 100,
        'page': 1
    }
    
    all_contributions = []
    
    try:
        while True:
            response = fetch_finance_data('contributions', params)
            if not response or 'contributions' not in response:
                if not all_contributions:
                    logger.error(f"Failed to fetch any valid contributions for candidate {candidate_id}")
                    return None
                break
                
            contributions = response.get('contributions', [])
            if not contributions:
                break
                
            all_contributions.extend(contributions)
            logger.debug(f"Fetched {len(contributions)} contributions (page {params['page']})")
            
            # Check if there are more pages
            if len(contributions) < params['per_page']:
                break
                
            params['page'] += 1
        
        logger.info(f"Found {len(all_contributions)} total contributions for candidate {candidate_id}")
        return all_contributions
    except Exception as e:
        logger.error(f"Error fetching contributions: {str(e)}", exc_info=True)
        return all_contributions if all_contributions else None

def collect_finance_data(year: int, state: str, paths: Dict[str, Path]) -> bool:
    """
    Collect finance data for a specific year and state.
    
    Args:
        year: Election year to collect data for
        state: Two-letter state code 
        paths: Project paths dictionary
        
    Returns:
        True if collection was successful, False otherwise
    """
    logger.info(f"Starting finance data collection for {state}, {year}")
    
    # Ensure output directories exist
    finance_dir = paths.get('raw_finance')
    if not finance_dir:
        logger.error("Finance directory not configured in paths")
        return False
        
    finance_year_dir = finance_dir / str(year)
    finance_year_dir.mkdir(parents=True, exist_ok=True)
    
    candidates_file = finance_year_dir / f"candidates_{state}_{year}.json"
    contributions_dir = finance_year_dir / "contributions"
    contributions_dir.mkdir(exist_ok=True)
    
    # Step 1: Get candidates list
    candidates = get_candidates_list(year, state)
    if not candidates:
        logger.error(f"Failed to fetch candidates for {state}, {year}")
        return False
        
    # Save candidates list
    save_json(candidates, candidates_file)
    logger.info(f"Saved {len(candidates)} candidates to {candidates_file}")
    
    # Step 2: Get contributions for each candidate
    success_count = 0
    failure_count = 0
    
    for candidate in tqdm(candidates, desc=f"Fetching contributions ({year}, {state})", unit="candidate"):
        candidate_id = candidate.get('id')
        if not candidate_id:
            logger.warning(f"Skipping candidate without ID: {candidate.get('name', 'Unknown')}")
            continue
            
        contribution_file = contributions_dir / f"contributions_{candidate_id}_{year}.json"
        
        # Skip if already downloaded (unless implementing force_refresh)
        if contribution_file.exists():
            logger.debug(f"Skipping existing contributions file for {candidate_id}")
            success_count += 1
            continue
            
        # Get contributions
        contributions = get_candidate_contributions(candidate_id, year)
        if contributions is not None:
            save_json(contributions, contribution_file)
            logger.debug(f"Saved {len(contributions)} contributions to {contribution_file}")
            success_count += 1
        else:
            logger.warning(f"Failed to fetch contributions for candidate {candidate_id}")
            failure_count += 1
    
    total_candidates = len(candidates)
    logger.info(f"Finance data collection complete: {success_count}/{total_candidates} successful, {failure_count}/{total_candidates} failed")
    
    # Return overall success status
    return failure_count == 0 or (success_count > 0 and failure_count < total_candidates * 0.25)  # Allow up to 25% failure rate

def consolidate_finance_data(years: List[int], state: str, paths: Dict[str, Path]) -> bool:
    """
    Consolidate collected finance data into unified CSVs.
    
    Args:
        years: List of years to consolidate
        state: Two-letter state code
        paths: Project paths dictionary
        
    Returns:
        True if consolidation was successful, False otherwise
    """
    logger.info(f"Consolidating finance data for {state}, years: {years}")
    
    finance_dir = paths.get('raw_finance')
    processed_dir = paths.get('processed')
    
    if not finance_dir or not processed_dir:
        logger.error("Finance or processed directory not configured in paths")
        return False
    
    processed_dir.mkdir(parents=True, exist_ok=True)
    
    # Consolidated dataframes
    all_candidates = []
    all_contributions = []
    
    for year in years:
        logger.info(f"Processing finance data for year {year}")
        finance_year_dir = finance_dir / str(year)
        
        if not finance_year_dir.exists():
            logger.warning(f"No finance data directory found for {year}, skipping")
            continue
        
        # Process candidates
        candidates_file = finance_year_dir / f"candidates_{state}_{year}.json"
        if candidates_file.exists():
            try:
                candidates = load_json(candidates_file)
                if candidates:
                    # Add year to each candidate record
                    for candidate in candidates:
                        candidate['election_year'] = year
                    all_candidates.extend(candidates)
                    logger.info(f"Loaded {len(candidates)} candidates from {year}")
            except Exception as e:
                logger.error(f"Error loading candidates for {year}: {str(e)}", exc_info=True)
        
        # Process contributions
        contributions_dir = finance_year_dir / "contributions"
        if contributions_dir.exists():
            contribution_files = list(contributions_dir.glob(f"contributions_*_{year}.json"))
            logger.info(f"Found {len(contribution_files)} contribution files for {year}")
            
            for contrib_file in tqdm(contribution_files, desc=f"Processing contributions ({year})", unit="file"):
                try:
                    contributions = load_json(contrib_file)
                    if contributions:
                        # Extract candidate_id from filename
                        match = re.search(r'contributions_([^_]+)_', contrib_file.name)
                        candidate_id = match.group(1) if match else "unknown"
                        
                        # Add candidate_id and year to each contribution record
                        for contrib in contributions:
                            contrib['candidate_id'] = candidate_id
                            contrib['election_year'] = year
                        
                        all_contributions.extend(contributions)
                except Exception as e:
                    logger.error(f"Error loading contributions from {contrib_file}: {str(e)}", exc_info=True)
    
    # Convert to DataFrames and save to CSV
    if all_candidates:
        candidates_df = pd.DataFrame(all_candidates)
        candidates_csv = processed_dir / f"finance_candidates_{state}.csv"
        candidates_df.to_csv(candidates_csv, index=False)
        logger.info(f"Saved {len(candidates_df)} consolidated candidate records to {candidates_csv}")
    else:
        logger.warning("No candidate data to consolidate")
    
    if all_contributions:
        contributions_df = pd.DataFrame(all_contributions)
        contributions_csv = processed_dir / f"finance_contributions_{state}.csv"
        contributions_df.to_csv(contributions_csv, index=False)
        logger.info(f"Saved {len(contributions_df)} consolidated contribution records to {contributions_csv}")
    else:
        logger.warning("No contribution data to consolidate")
    
    return len(all_candidates) > 0 or len(all_contributions) > 0

def match_finance_to_legislators(
    finance_candidates_df: pd.DataFrame, 
    legislators_df: pd.DataFrame,
    threshold: int = 85
) -> pd.DataFrame:
    """
    Match finance candidates to legislators based on name similarity.
    
    Args:
        finance_candidates_df: DataFrame of finance candidates
        legislators_df: DataFrame of legislators
        threshold: Fuzzy matching threshold (0-100)
        
    Returns:
        DataFrame with matched candidates and legislator IDs
    """
    logger.info("Starting matching finance candidates to legislators")
    
    if finance_candidates_df.empty or legislators_df.empty:
        logger.error("Cannot match with empty DataFrames")
        return pd.DataFrame()
    
    # Ensure name columns exist
    if 'name' not in finance_candidates_df.columns:
        logger.error("Finance candidates DataFrame missing 'name' column")
        return pd.DataFrame()
    
    if 'name' not in legislators_df.columns:
        logger.error("Legislators DataFrame missing 'name' column")
        return pd.DataFrame()
    
    # Create a dictionary of legislator names for fuzzy matching
    legislator_names = legislators_df['name'].dropna().unique().tolist()
    logger.info(f"Found {len(legislator_names)} unique legislator names for matching")
    
    # Create columns for matched data
    finance_candidates_df['matched_legislator_name'] = None
    finance_candidates_df['match_score'] = None
    finance_candidates_df['legislator_id'] = None
    
    # Match each finance candidate to a legislator
    match_count = 0
    
    for idx, row in tqdm(finance_candidates_df.iterrows(), total=len(finance_candidates_df), desc="Matching candidates", unit="candidate"):
        candidate_name = row['name']
        if not isinstance(candidate_name, str) or not candidate_name.strip():
            continue
            
        # Clean the candidate name for better matching
        clean_candidate_name = clean_name(candidate_name)
        
        # Skip very short names as they likely won't match well
        if len(clean_candidate_name) < 5:
            continue
            
        # Get the best match
        matches = process.extractOne(clean_candidate_name, legislator_names, scorer=fuzz.token_sort_ratio)
        
        if matches and matches[1] >= threshold:
            matched_name = matches[0]
            match_score = matches[1]
            
            # Find the legislator ID(s) for this name
            matching_legislators = legislators_df[legislators_df['name'] == matched_name]
            
            if not matching_legislators.empty:
                # Use the first matching legislator_id (or implement more specific logic if needed)
                legislator_id = matching_legislators.iloc[0]['legislator_id']
                
                finance_candidates_df.at[idx, 'matched_legislator_name'] = matched_name
                finance_candidates_df.at[idx, 'match_score'] = match_score
                finance_candidates_df.at[idx, 'legislator_id'] = legislator_id
                match_count += 1
    
    logger.info(f"Matched {match_count} out of {len(finance_candidates_df)} finance candidates to legislators")
    
    # Create a version with only successful matches
    matched_df = finance_candidates_df.dropna(subset=['legislator_id']).copy()
    
    return matched_df

def main_finance_collection(
    years: List[int],
    state: str,
    paths: Dict[str, Path],
    match_to_legislators: bool = True
) -> bool:
    """
    Main function to perform the complete finance data collection workflow.
    
    Args:
        years: List of years to collect data for
        state: Two-letter state code
        paths: Project paths dictionary
        match_to_legislators: Whether to match finance data to legislators
        
    Returns:
        True if collection was successful, False otherwise
    """
    logger.info(f"Starting finance collection pipeline for {state}, years: {years}")
    
    overall_success = True
    
    # Step 1: Collect finance data for each year
    for year in years:
        success = collect_finance_data(year, state, paths)
        if not success:
            logger.warning(f"Finance data collection failed for {year}")
            overall_success = False
    
    # Step 2: Consolidate data across years
    consolidation_success = consolidate_finance_data(years, state, paths)
    if not consolidation_success:
        logger.warning("Finance data consolidation failed")
        overall_success = False
    
    # Step 3: Match finance data to legislators (if requested)
    if match_to_legislators and consolidation_success:
        processed_dir = paths.get('processed')
        if not processed_dir:
            logger.error("Processed directory not configured in paths")
            return False
            
        finance_candidates_csv = processed_dir / f"finance_candidates_{state}.csv"
        legislators_csv = processed_dir / "legislators.csv"
        
        if not finance_candidates_csv.exists():
            logger.error(f"Finance candidates file not found: {finance_candidates_csv}")
            return False
            
        if not legislators_csv.exists():
            logger.error(f"Legislators file not found: {legislators_csv}")
            return False
            
        try:
            # Load finance candidates and legislators
            finance_candidates_df = pd.read_csv(finance_candidates_csv)
            legislators_df = pd.read_csv(legislators_csv)
            
            # Match finance candidates to legislators
            matched_df = match_finance_to_legislators(finance_candidates_df, legislators_df)
            
            if not matched_df.empty:
                # Save matched data
                matched_csv = processed_dir / f"finance_matched_{state}.csv"
                matched_df.to_csv(matched_csv, index=False)
                logger.info(f"Saved {len(matched_df)} matched finance records to {matched_csv}")
            else:
                logger.warning("No finance records were successfully matched to legislators")
                overall_success = False
        except Exception as e:
            logger.error(f"Error matching finance data to legislators: {str(e)}", exc_info=True)
            overall_success = False
    
    logger.info(f"Finance collection pipeline completed with overall success: {overall_success}")
    return overall_success 