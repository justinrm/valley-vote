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
    LEGISCAN_API_KEY,
    LEGISCAN_BASE_URL,
    LEGISCAN_MAX_RETRIES,
    LEGISCAN_DEFAULT_WAIT_SECONDS,
    DATA_COLLECTION_LOG_FILE,
)
from .utils import (
    setup_logging,
    save_json,
    convert_to_csv,
    fetch_page,
    load_json,
    clean_text,
    ensure_dir,
    setup_project_paths
)
from .legiscan_client import (
    fetch_api_data,
    _fetch_and_save_document, 
    APIRateLimitError,
    APIResourceNotFoundError
)

# --- Configure Logging ---
logger = logging.getLogger(__name__)

# --- Amendment Collection ---
def collect_amendments_for_bill(
    bill_id: int,
    session_id: int,
    amendment_ids: List[int],
    texts_dir: Path,
    amendment_dir: Path
) -> Dict[str, Any]:
    """
    Collect all amendments for a specific bill.
    
    Args:
        bill_id: LegiScan bill ID
        session_id: LegiScan session ID
        amendment_ids: List of amendment document IDs to fetch
        texts_dir: Directory to save bill text files
        amendment_dir: Directory to save amendment files
        
    Returns:
        Dictionary with amendment details and success status
    """
    logger.info(f"Collecting amendments for bill {bill_id} (Session {session_id})")
    
    amendment_results = {
        'bill_id': bill_id,
        'session_id': session_id,
        'amendments_requested': len(amendment_ids),
        'amendments_fetched': 0,
        'status': 'incomplete'
    }
    
    # Ensure directories exist
    amendment_dir.mkdir(parents=True, exist_ok=True)
    
    # First ensure we have the main bill text for comparison
    bill_text_fetched = False
    bill_text_id = None
    bill_text_file = None
    
    # Try to get the bill's text ID
    try:
        bill_params = {'id': bill_id}
        bill_data = fetch_api_data('getBill', bill_params)
        
        if bill_data and bill_data.get('status') == 'OK' and 'bill' in bill_data:
            bill_info = bill_data['bill']
            
            # Check for text document ID
            if 'texts' in bill_info and bill_info['texts']:
                # Get the most recent text document
                texts = bill_info['texts']
                if texts:
                    # Sort by date if available, otherwise use the first one
                    try:
                        texts_sorted = sorted(texts, key=lambda x: x.get('date', ''), reverse=True)
                        bill_text_id = texts_sorted[0].get('doc_id')
                    except Exception:
                        bill_text_id = texts[0].get('doc_id')
            
            if bill_text_id:
                # Fetch the bill text
                bill_text_file = texts_dir / f"bill_{bill_id}_text_{bill_text_id}.json"
                
                if bill_text_file.exists():
                    logger.debug(f"Bill text file already exists for bill {bill_id}: {bill_text_file}")
                    bill_text_fetched = True
                else:
                    bill_text_fetched = _fetch_and_save_document(
                        doc_type='text',
                        doc_id=bill_text_id,
                        bill_id=bill_id,
                        session_id=session_id,
                        api_operation='getText',
                        output_dir=texts_dir
                    )
            else:
                logger.warning(f"No text document ID found for bill {bill_id}")
        else:
            logger.warning(f"Failed to fetch bill data for bill {bill_id}")
            
    except Exception as e:
        logger.error(f"Error fetching bill text for bill {bill_id}: {e}", exc_info=True)
    
    # Fetch each amendment
    amendment_details = []
    successful_fetches = 0
    
    for amendment_id in amendment_ids:
        if not amendment_id:
            logger.warning(f"Invalid amendment ID for bill {bill_id}")
            continue
            
        amendment_file = amendment_dir / f"bill_{bill_id}_amendment_{amendment_id}.json"
        
        try:
            fetch_success = _fetch_and_save_document(
                doc_type='amendment',
                doc_id=amendment_id,
                bill_id=bill_id,
                session_id=session_id,
                api_operation='getAmendment',
                output_dir=amendment_dir
            )
            
            if fetch_success:
                successful_fetches += 1
                
                # Load the saved amendment to extract details
                amendment_data = load_json(amendment_file)
                if amendment_data and 'amendment' in amendment_data:
                    amendment_info = amendment_data['amendment']
                    
                    # Extract relevant details
                    amendment_details.append({
                        'amendment_id': amendment_id,
                        'bill_id': bill_id,
                        'session_id': session_id,
                        'amendment_title': amendment_info.get('title', ''),
                        'amendment_desc': amendment_info.get('description', ''),
                        'amendment_date': amendment_info.get('date', ''),
                        'amendment_type': amendment_info.get('type', ''),
                        'amendment_status': amendment_info.get('status', ''),
                        'file_path': str(amendment_file)
                    })
        except APIResourceNotFoundError:
            logger.warning(f"Amendment {amendment_id} not found for bill {bill_id}")
        except Exception as e:
            logger.error(f"Error fetching amendment {amendment_id} for bill {bill_id}: {e}", exc_info=True)
    
    # Update results
    amendment_results['amendments_fetched'] = successful_fetches
    amendment_results['bill_text_fetched'] = bill_text_fetched
    amendment_results['bill_text_id'] = bill_text_id
    amendment_results['bill_text_file'] = str(bill_text_file) if bill_text_file else None
    amendment_results['amendment_details'] = amendment_details
    
    if successful_fetches == len(amendment_ids):
        amendment_results['status'] = 'complete'
    elif successful_fetches > 0:
        amendment_results['status'] = 'partial'
    else:
        amendment_results['status'] = 'failed'
    
    logger.info(f"Amendment collection for bill {bill_id}: {successful_fetches}/{len(amendment_ids)} amendments fetched")
    return amendment_results

def extract_amendment_content(amendment_file: Path) -> Optional[Dict[str, Any]]:
    """
    Extract content from an amendment file.
    
    Args:
        amendment_file: Path to amendment JSON file
        
    Returns:
        Dictionary with extracted content or None on failure
    """
    try:
        amendment_data = load_json(amendment_file)
        if not amendment_data or 'amendment' not in amendment_data:
            logger.warning(f"Invalid amendment data in file: {amendment_file}")
            return None
        
        amendment_info = amendment_data['amendment']
        doc_id = amendment_info.get('doc_id')
        title = amendment_info.get('title', '')
        description = amendment_info.get('description', '')
        date = amendment_info.get('date', '')
        amendment_body = amendment_info.get('text', {}).get('doc', '')
        
        # Clean amendment body if present
        if amendment_body:
            amendment_body = clean_text(amendment_body)
        
        # Extract amendment number from filename
        amendment_id = None
        match = re.search(r'amendment_(\d+)\.json', amendment_file.name)
        if match:
            amendment_id = int(match.group(1))
        
        # Extract bill ID from filename
        bill_id = None
        match = re.search(r'bill_(\d+)_amendment', amendment_file.name)
        if match:
            bill_id = int(match.group(1))
        
        return {
            'amendment_id': amendment_id,
            'doc_id': doc_id,
            'bill_id': bill_id,
            'title': title,
            'description': description,
            'date': date,
            'amendment_body': amendment_body,
            'file_path': str(amendment_file)
        }
        
    except Exception as e:
        logger.error(f"Error extracting content from amendment file {amendment_file}: {e}", exc_info=True)
        return None

def compare_bill_text_to_amendment(
    bill_text_file: Path,
    amendment_file: Path
) -> Optional[Dict[str, Any]]:
    """
    Compare bill text to amendment to identify changes.
    
    Args:
        bill_text_file: Path to bill text JSON file
        amendment_file: Path to amendment JSON file
        
    Returns:
        Dictionary with comparison results or None on failure
    """
    try:
        # Load bill text
        bill_text_data = load_json(bill_text_file)
        if not bill_text_data or 'text' not in bill_text_data:
            logger.warning(f"Invalid bill text data in file: {bill_text_file}")
            return None
        
        bill_text = bill_text_data['text'].get('doc', '')
        if not bill_text:
            logger.warning(f"No bill text content found in file: {bill_text_file}")
            return None
        
        # Load amendment text
        amendment_data = load_json(amendment_file)
        if not amendment_data or 'amendment' not in amendment_data:
            logger.warning(f"Invalid amendment data in file: {amendment_file}")
            return None
        
        amendment_text = amendment_data['amendment'].get('text', {}).get('doc', '')
        if not amendment_text:
            logger.warning(f"No amendment text content found in file: {amendment_file}")
            return None
        
        # Clean texts for comparison
        bill_text = clean_text(bill_text)
        amendment_text = clean_text(amendment_text)
        
        # Calculate simple difference metrics
        text_length_diff = len(amendment_text) - len(bill_text)
        length_diff_percent = (text_length_diff / len(bill_text)) * 100 if len(bill_text) > 0 else 0
        
        # Extract bill ID and amendment ID from filenames
        bill_id = None
        match = re.search(r'bill_(\d+)_text', bill_text_file.name)
        if match:
            bill_id = int(match.group(1))
        
        amendment_id = None
        match = re.search(r'amendment_(\d+)\.json', amendment_file.name)
        if match:
            amendment_id = int(match.group(1))
        
        # Attempt to extract bill text doc ID from filename
        bill_text_id = None
        match = re.search(r'text_(\d+)\.json', bill_text_file.name)
        if match:
            bill_text_id = int(match.group(1))
        
        # Simple change detection (very basic)
        has_additions = text_length_diff > 0
        has_deletions = text_length_diff < 0
        
        # Create comparison result
        comparison = {
            'bill_id': bill_id,
            'bill_text_id': bill_text_id,
            'amendment_id': amendment_id,
            'bill_text_length': len(bill_text),
            'amendment_text_length': len(amendment_text),
            'text_length_diff': text_length_diff,
            'length_diff_percent': length_diff_percent,
            'has_additions': has_additions,
            'has_deletions': has_deletions,
            'bill_text_file': str(bill_text_file),
            'amendment_file': str(amendment_file)
        }
        
        return comparison
        
    except Exception as e:
        logger.error(f"Error comparing bill text to amendment: {e}", exc_info=True)
        return None

def process_amendments_for_session(
    session_id: int,
    year: int,
    paths: Dict[str, Path],
    bills: Optional[List[Dict[str, Any]]] = None
) -> List[Dict[str, Any]]:
    """
    Process amendments for all bills in a session.
    
    Args:
        session_id: LegiScan session ID
        year: Session year
        paths: Project paths dictionary
        bills: Optional list of bills (to avoid fetching again)
        
    Returns:
        List of amendment processing results
    """
    logger.info(f"Processing amendments for session {session_id} (year {year})")
    
    # Ensure output directories exist
    amendments_dir = paths.get('raw_amendments')
    texts_dir = paths.get('raw_texts')
    
    if not amendments_dir or not texts_dir:
        logger.error("Amendments or texts directory not configured in paths")
        return []
    
    amendments_year_dir = amendments_dir / str(year)
    texts_year_dir = texts_dir / str(year)
    
    amendments_year_dir.mkdir(parents=True, exist_ok=True)
    texts_year_dir.mkdir(parents=True, exist_ok=True)
    
    # Get the bills for this session if not provided
    if not bills:
        try:
            bills_path = paths.get('raw_bills', Path('data/raw/bills')) / str(year) / f"bills_{session_id}.json"
            if bills_path.exists():
                bills = load_json(bills_path)
                if not bills:
                    logger.warning(f"No bills found in file: {bills_path}")
                    return []
            else:
                logger.warning(f"Bills file not found: {bills_path}")
                return []
        except Exception as e:
            logger.error(f"Error loading bills for session {session_id}: {e}", exc_info=True)
            return []
    
    logger.info(f"Processing amendments for {len(bills)} bills in session {session_id}")
    
    # Process each bill's amendments
    amendment_results = []
    
    for bill in tqdm(bills, desc=f"Processing amendments (session {session_id})", unit="bill"):
        bill_id = bill.get('bill_id')
        if not bill_id:
            logger.warning(f"Bill missing ID: {bill}")
            continue
        
        # Extract amendment IDs
        amendment_ids = []
        if 'amendments' in bill and bill['amendments']:
            for amendment in bill['amendments']:
                doc_id = amendment.get('doc_id')
                if doc_id:
                    amendment_ids.append(doc_id)
        
        # Skip if no amendments
        if not amendment_ids:
            logger.debug(f"No amendments found for bill {bill_id}")
            continue
        
        # Collect the amendments
        result = collect_amendments_for_bill(
            bill_id=bill_id,
            session_id=session_id,
            amendment_ids=amendment_ids,
            texts_dir=texts_year_dir,
            amendment_dir=amendments_year_dir
        )
        
        amendment_results.append(result)
    
    # Generate summary
    successful = len([r for r in amendment_results if r['status'] in ('complete', 'partial')])
    failed = len([r for r in amendment_results if r['status'] == 'failed'])
    total_requested = sum(r['amendments_requested'] for r in amendment_results)
    total_fetched = sum(r['amendments_fetched'] for r in amendment_results)
    
    logger.info(f"Amendment processing summary (session {session_id}):")
    logger.info(f"Bills with amendments: {len(amendment_results)}")
    logger.info(f"Successful fetches: {successful}, Failed: {failed}")
    logger.info(f"Total amendments requested: {total_requested}, fetched: {total_fetched}")
    
    return amendment_results

def analyze_amendments(
    paths: Dict[str, Path],
    years: List[int],
    state: str
) -> bool:
    """
    Analyze amendments across multiple years and create consolidated dataset.
    
    Args:
        paths: Project paths dictionary
        years: List of years to analyze
        state: Two-letter state code
        
    Returns:
        True if analysis was successful, False otherwise
    """
    logger.info(f"Analyzing amendments for {state} across years: {years}")
    
    # Ensure processed directory exists
    processed_dir = paths.get('processed')
    if not processed_dir:
        logger.error("Processed directory not configured in paths")
        return False
    
    processed_dir.mkdir(parents=True, exist_ok=True)
    
    # Get all amendment files
    amendments_dir = paths.get('raw_amendments')
    texts_dir = paths.get('raw_texts')
    
    if not amendments_dir or not texts_dir:
        logger.error("Amendments or texts directory not configured in paths")
        return False
    
    # Process amendments for each year
    all_amendment_details = []
    all_amendment_comparisons = []
    
    for year in years:
        logger.info(f"Processing amendments for year {year}")
        
        amendments_year_dir = amendments_dir / str(year)
        texts_year_dir = texts_dir / str(year)
        
        if not amendments_year_dir.exists():
            logger.warning(f"No amendments directory for year {year}: {amendments_year_dir}")
            continue
        
        # Get all amendment files
        amendment_files = list(amendments_year_dir.glob("bill_*_amendment_*.json"))
        logger.info(f"Found {len(amendment_files)} amendment files for year {year}")
        
        if not amendment_files:
            continue
            
        # Process each amendment file
        for amendment_file in tqdm(amendment_files, desc=f"Analyzing amendments ({year})", unit="amendment"):
            # Extract amendment content
            amendment_content = extract_amendment_content(amendment_file)
            if amendment_content:
                all_amendment_details.append(amendment_content)
                
                # Try to find corresponding bill text for comparison
                bill_id = amendment_content.get('bill_id')
                if bill_id:
                    # Find relevant bill text files
                    bill_text_files = list(texts_year_dir.glob(f"bill_{bill_id}_text_*.json"))
                    
                    if bill_text_files:
                        # Use the most recently modified bill text file
                        bill_text_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
                        bill_text_file = bill_text_files[0]
                        
                        # Compare bill text to amendment
                        comparison = compare_bill_text_to_amendment(
                            bill_text_file=bill_text_file,
                            amendment_file=amendment_file
                        )
                        
                        if comparison:
                            all_amendment_comparisons.append(comparison)
    
    # Convert to DataFrames and save
    if all_amendment_details:
        try:
            # Create amendment details DataFrame
            amendment_details_df = pd.DataFrame(all_amendment_details)
            
            # Remove amendment_body column to avoid overly large files
            if 'amendment_body' in amendment_details_df.columns:
                amendment_details_df = amendment_details_df.drop('amendment_body', axis=1)
            
            # Save to CSV
            amendment_details_csv = processed_dir / f"amendments_{state}.csv"
            amendment_details_df.to_csv(amendment_details_csv, index=False)
            logger.info(f"Saved {len(amendment_details_df)} amendment details to {amendment_details_csv}")
            
            # If comparisons available, save those too
            if all_amendment_comparisons:
                comparison_df = pd.DataFrame(all_amendment_comparisons)
                comparison_csv = processed_dir / f"amendment_comparisons_{state}.csv"
                comparison_df.to_csv(comparison_csv, index=False)
                logger.info(f"Saved {len(comparison_df)} amendment comparisons to {comparison_csv}")
            
            return True
        except Exception as e:
            logger.error(f"Error creating amendment DataFrames: {e}", exc_info=True)
            return False
    else:
        logger.warning("No amendment details found")
        return False

def main_amendment_collection(
    years: List[int],
    state: str,
    paths: Dict[str, Path],
    sessions: Optional[List[Dict[str, Any]]] = None
) -> bool:
    """
    Main function to perform the complete amendment collection workflow.
    
    Args:
        years: List of years to collect data for
        state: Two-letter state code
        paths: Project paths dictionary
        sessions: Optional list of sessions (to avoid fetching again)
        
    Returns:
        True if collection was successful, False otherwise
    """
    logger.info(f"Starting amendment collection pipeline for {state}, years: {years}")
    
    # Get sessions if not provided
    if not sessions:
        try:
            sessions_file = paths.get('artifacts') / f"sessions_{state}.json"
            if sessions_file.exists():
                sessions = load_json(sessions_file)
                if not sessions:
                    logger.warning(f"No sessions found in file: {sessions_file}")
                    return False
            else:
                logger.warning(f"Sessions file not found: {sessions_file}")
                return False
        except Exception as e:
            logger.error(f"Error loading sessions for {state}: {e}", exc_info=True)
            return False
    
    # Filter sessions by years
    year_set = set(years)
    year_sessions = [
        session for session in sessions 
        if session.get('year_start') in year_set or session.get('year_end') in year_set
    ]
    
    if not year_sessions:
        logger.warning(f"No sessions found for years: {years}")
        return False
    
    logger.info(f"Found {len(year_sessions)} sessions for years {years}")
    
    # Process each session
    all_amendment_results = []
    for session in year_sessions:
        session_id = session.get('session_id')
        year = session.get('year_start')
        
        if not session_id or not year:
            logger.warning(f"Session missing ID or year: {session}")
            continue
        
        # Process amendments for this session
        results = process_amendments_for_session(
            session_id=session_id,
            year=year,
            paths=paths
        )
        
        all_amendment_results.extend(results)
    
    # Analyze all amendments
    analysis_success = analyze_amendments(
        paths=paths,
        years=years,
        state=state
    )
    
    # Overall success is based on having some successful amendment processing
    has_amendments = len(all_amendment_results) > 0
    overall_success = has_amendments and analysis_success
    
    logger.info(f"Amendment collection pipeline completed with overall success: {overall_success}")
    
    return overall_success 