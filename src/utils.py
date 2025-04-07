# src/utils.py
"""Common utilities used across the Valley Vote project."""

import os
import json
import logging
import sys
import random
import time
import re # <-- Add import for regular expressions
from pathlib import Path
from typing import Dict, Any, Optional, List, Union
import io # For string/bytes IO

import requests
import pandas as pd
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log
)

# --- Configure Root Logger for Tenacity ---
# Get root logger instance (used by tenacity for retry logging)
root_logger = logging.getLogger()
# Ensure root logger has a basic handler if not configured elsewhere initially
if not root_logger.hasHandlers():
    logging.basicConfig(
        level=logging.WARNING, # Be less verbose by default for root
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )

# --- Logging Setup ---
def setup_logging(log_file_name: str, log_dir: Path, level=logging.INFO, mode='w') -> logging.Logger:
    """Configure logging for a specific script/module, saving to a specified directory."""
    log_file_path = log_dir / log_file_name
    log_file_path.parent.mkdir(parents=True, exist_ok=True) # Ensure log directory exists

    # Use file stem for logger name for better identification
    logger = logging.getLogger(log_file_path.stem)
    logger.setLevel(level)

    # Remove existing handlers attached *specifically to this logger*
    # Avoids duplicate logs if setup_logging is called multiple times on the same logger name
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Create handlers (File and Stream)
    # Overwrite log file each run by default (mode='w')
    file_handler = logging.FileHandler(log_file_path, mode=mode, encoding='utf-8')
    stream_handler = logging.StreamHandler(sys.stdout) # Ensure console output

    # Create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)

    # Add the handlers to this specific logger
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    # Prevent logs from propagating to the root logger IF handlers are added here.
    # If set to False, only handlers attached directly to this logger will process its messages.
    logger.propagate = False

    logger.info(f"Logging initialized for '{logger.name}'. Level: {logging.getLevelName(logger.level)}. Log file: {log_file_path}")
    return logger

# --- File Operations ---
def save_json(data: Any, path: Path, indent: int = 4) -> bool:
    """Save data as JSON file, creating parent directories if needed."""
    logger = logging.getLogger(__name__) # Use utils logger
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('w', encoding='utf-8') as f:
            json.dump(data, f, indent=indent, ensure_ascii=False, default=str) # Add default=str for non-serializable types like Path
        logger.debug(f"Saved JSON to {path}")
        return True
    except TypeError as e:
        logger.error(f"TypeError saving JSON to {path}: {str(e)}. Data type: {type(data)}")
        return False
    except Exception as e:
        logger.error(f"Error saving JSON to {path}: {str(e)}", exc_info=True)
        return False

def load_json(path: Path) -> Optional[Any]:
    """Load data from a JSON file."""
    logger = logging.getLogger(__name__)
    if not path.is_file():
        logger.error(f"JSON file not found: {path}")
        return None
    try:
        with path.open('r', encoding='utf-8') as f:
            data = json.load(f)
        logger.debug(f"Loaded JSON from {path}")
        return data
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from {path}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error loading JSON from {path}: {str(e)}", exc_info=True)
        return None


def convert_to_csv(data: List[Dict[str, Any]], csv_path: Path, columns: Optional[List[str]] = None) -> int:
    """Convert list of dicts to CSV with specified columns, handling empty/invalid data."""
    logger = logging.getLogger(__name__)
    num_saved = 0
    try:
        csv_path.parent.mkdir(parents=True, exist_ok=True)

        if not isinstance(data, list):
            logger.error(f"Invalid data type for CSV conversion: expected list, got {type(data)}. Path: {csv_path}")
            df = pd.DataFrame(columns=columns if columns else [])
        elif not data:
            logger.info(f"No data provided to save at {csv_path}. Creating empty file with headers.")
            df = pd.DataFrame(columns=columns if columns else [])
        else:
            # Create DataFrame, handling potential errors during creation
            try:
                df = pd.DataFrame(data)
            except Exception as e_create:
                logger.error(f"Error creating DataFrame for CSV {csv_path}: {e_create}. Saving empty CSV.")
                df = pd.DataFrame(columns=columns if columns else [])

            # Ensure specified columns exist, fill missing with pd.NA
            if columns:
                for col in columns:
                    if col not in df.columns:
                        df[col] = pd.NA  # Use pandas NA
                # Reorder/select only specified columns
                try:
                    df = df[columns]
                except KeyError as e_key:
                    logger.error(f"KeyError selecting columns for CSV {csv_path}: {e_key}. Columns available: {df.columns.tolist()}")
                    # Fallback: Save with available columns matching 'columns' list
                    available_cols = [c for c in columns if c in df.columns]
                    df = df[available_cols]

            # Use inferred columns if none were specified
            elif df.empty and not columns:
                columns = []  # No columns if df is empty and none specified
            elif not columns:
                columns = df.columns.tolist()  # Get columns from df if none provided

        # Save the DataFrame
        df.to_csv(csv_path, index=False, encoding='utf-8')
        num_saved = len(df)
        logger.info(f"Saved {num_saved} rows to CSV: {csv_path}")

    except Exception as e:
        logger.error(f"Error creating or saving CSV {csv_path}: {str(e)}", exc_info=True)
        # Attempt to save an empty placeholder file on error
        try:
            df_empty = pd.DataFrame(columns=columns if columns else [])
            df_empty.to_csv(csv_path, index=False, encoding='utf-8')
            logger.info(f"Saved empty CSV placeholder with headers after error for: {csv_path}")
        except Exception as final_e:
            logger.error(f"Could not even save an empty CSV placeholder for {csv_path}: {final_e}")
        num_saved = 0  # Indicate failure

    return num_saved

# --- String/Text Utilities ---

# Precompile regex for cleaning names (moved from match_finance_to_leg.py)
NAME_CLEANUP_REGEX = re.compile(
    r'(senator|rep\.|representative|judge|governor|dr\.|hon\.)\s+|\s+(jr\.|sr\.|iii|ii|iv)$',
    re.IGNORECASE
)

def clean_name(name: Optional[str]) -> Optional[str]:
    """Cleans common titles and suffixes from a person's name string."""
    if not name or pd.isna(name):
        return None
    try:
        # Ensure it's a string before applying regex
        name_str = str(name)
        cleaned = NAME_CLEANUP_REGEX.sub('', name_str).strip()
        # Additional whitespace cleanup
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        return cleaned if cleaned else None # Return None if cleaning results in empty string
    except Exception as e:
        # Log error if cleaning fails unexpectedly
        logger = logging.getLogger(__name__)
        logger.warning(f"Could not clean name '{name}': {e}")
        return str(name) # Return original string on error

# --- Network Operations ---
DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1', # Common header
    'Sec-Fetch-Dest': 'document', # Common headers for mimicking browser
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
}

@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1.5, min=3, max=45),
    retry=retry_if_exception_type((requests.exceptions.Timeout, requests.exceptions.ConnectionError)),
    before_sleep=before_sleep_log(root_logger, logging.WARNING)
)
def fetch_page(
    url: str,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 40,
    method: str = 'GET',
    params: Optional[Dict[str, Any]] = None,
    data: Optional[Dict[str, Any]] = None,
    return_bytes: bool = False
) -> Optional[Union[str, bytes]]:
    """Fetch content from URL with retries and improved error handling.
    
    Args:
        url: The URL to fetch
        headers: Optional HTTP headers to include in the request
        timeout: Request timeout in seconds
        method: HTTP method to use ('GET' or 'POST')
        params: Optional query parameters for the request
        data: Optional form data for POST requests
        return_bytes: If True, return raw bytes instead of decoded text
        
    Returns:
        The response content as either text or bytes, or None if the request failed
        
    Raises:
        requests.exceptions.RequestException: If the request fails after retries
    """
    logger = logging.getLogger(__name__)
    request_headers = DEFAULT_HEADERS.copy()
    if headers:
        request_headers.update(headers) # Allow overriding/adding headers

    # Mask sensitive params/data if needed for logging
    log_params = params if params else '{}'
    log_data = data if data else '{}'
    logger.info(f"Fetching URL ({method}): {url}")
    logger.debug(f"Params: {log_params}, Data: {log_data}, Headers: {request_headers}")

    try:
        session = requests.Session() # Use session for potential cookie handling, keep-alive
        session.headers.update(request_headers) # Set default headers for session

        if method.upper() == 'GET':
            response = session.get(url, timeout=timeout, allow_redirects=True, params=params, stream=return_bytes)
        elif method.upper() == 'POST':
            response = session.post(url, timeout=timeout, allow_redirects=True, params=params, data=data, stream=return_bytes)
        else:
            logger.error(f"Unsupported HTTP method: {method}")
            return None

        # Check for HTTP errors AFTER checking status code potentially
        if response.status_code >= 400:
            # Log specific error but raise HTTPError to handle different cases below
            logger.error(f"HTTP error {response.status_code} received for {url}. Response text (first 500 chars): {response.text[:500]}")
            response.raise_for_status() # Raise the actual HTTPError

        # Handle content
        if return_bytes:
            content = response.content # Get raw bytes
            logger.debug(f"Fetched {len(content)} bytes from {url}")
            if len(content) < 100: # Small heuristic for bytes
                logger.warning(f"Very small byte response ({len(content)} bytes) from {url}.")
            return content
        else:
            # Decode text carefully
            content_type = response.headers.get('Content-Type', '').lower()
            encoding = response.encoding # Use requests' detected encoding first
            text_content = None
            try:
                text_content = response.text # Accessing .text performs decoding
                logger.debug(f"Decoded text (approx {len(text_content)} chars) from {url} using encoding '{encoding}'.")
            except Exception as decode_err:
                logger.error(f"Error decoding response from {url} with encoding '{encoding}': {decode_err}")
                # Fallback attempt if .text failed? Unlikely but possible.
                try:
                    logger.warning("Trying fallback decoding with utf-8 ignore.")
                    text_content = response.content.decode('utf-8', errors='ignore')
                except Exception as fb_err:
                    logger.error(f"Fallback decoding also failed: {fb_err}")
                    return None # Give up if decoding fails badly

            if text_content is not None and len(text_content) < 200: # Heuristic for small text pages
                logger.warning(f"Small text response received from {url} ({len(text_content)} chars). May indicate error or empty data.")

            return text_content

    # Handle exceptions specifically
    except requests.exceptions.Timeout as e:
        logger.error(f"Timeout ({timeout}s) occurred while fetching {url}.")
        raise  # Re-raise Timeout to trigger tenacity retry
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Connection error occurred while fetching {url}: {str(e)}")
        raise  # Re-raise ConnectionError to trigger tenacity retry
    except requests.exceptions.HTTPError as e:
        # Handle specific HTTP errors after raise_for_status()
        # 404 is common, log as warning, don't retry usually
        if e.response.status_code == 404:
            logger.warning(f"HTTP 404 Not Found for {url}. Resource likely does not exist.")
        # Other client errors (4xx except 429 - handled elsewhere if needed)
        elif 400 <= e.response.status_code < 500 and e.response.status_code != 429:
            logger.error(f"HTTP Client Error {e.response.status_code} for {url}. Check request parameters/headers.")
        # Server errors (5xx) - already logged above, might be retried by tenacity if raised
        elif e.response.status_code >= 500:
            logger.error(f"HTTP Server Error {e.response.status_code} for {url}. Retries might apply.")
        # Don't raise here unless tenacity should retry based on the exception type.
        # Since HTTPError isn't in the retry list, execution stops for this call.
        return None  # Indicate failure for this specific call after HTTPError
    except requests.exceptions.RequestException as e:
        # Catch other general request errors (e.g., invalid URL structure)
        logger.error(f"General request exception fetching {url}: {str(e)}")
        return None  # Don't retry these usually
    except Exception as e:
        # Catch-all for unexpected errors during the request/response handling
        logger.error(f"Unexpected error during fetch_page for {url}: {e}", exc_info=True)
        return None

# --- Path Management ---
def setup_project_paths(base_dir_override: Optional[Union[str, Path]] = None) -> Dict[str, Path]:
    """Setup and return project directory structure, creating directories."""
    # Import config locally to avoid potential circular imports at module level
    from .config import DEFAULT_BASE_DATA_DIR

    if base_dir_override:
        # Use Path object and resolve to absolute path
        base_dir = Path(base_dir_override).resolve()
    else:
        # Use default from config, resolve to absolute path
        base_dir = DEFAULT_BASE_DATA_DIR.resolve()

    # Define main directories
    raw_data_dir = base_dir / 'raw'
    processed_data_dir = base_dir / 'processed'
    artifacts_dir = base_dir / 'artifacts'
    log_dir = base_dir / 'logs'

    # Define subdirectories within 'raw'
    raw_sessions_dir = raw_data_dir / 'sessions'
    raw_legislators_dir = raw_data_dir / 'legislators'
    raw_committees_dir = raw_data_dir / 'committees'
    raw_bills_dir = raw_data_dir / 'bills'
    raw_votes_dir = raw_data_dir / 'votes'
    raw_sponsors_dir = raw_data_dir / 'sponsors'
    # Add new raw directories for documents
    raw_texts_dir = raw_data_dir / 'texts'
    raw_amendments_dir = raw_data_dir / 'amendments'
    raw_supplements_dir = raw_data_dir / 'supplements'

    # Define subdirectories within 'artifacts' (e.g., for monitoring, scraping state)
    monitor_dir = artifacts_dir / 'monitor'
    finance_scraped_dir = artifacts_dir / 'finance_scraped' # Raw files from finance scraping

    # Define subdirectories within 'processed' (e.g., for yearly consolidated files)
    processed_committee_memberships_dir = processed_data_dir / 'committee_memberships'
    processed_finance_dir = processed_data_dir / 'finance_matched' # Matched finance data

    # Create all directories
    dirs_to_create = [
        raw_data_dir, processed_data_dir, artifacts_dir, log_dir,
        raw_sessions_dir, raw_legislators_dir, raw_committees_dir,
        raw_bills_dir, raw_votes_dir, raw_sponsors_dir,
        raw_texts_dir, raw_amendments_dir, raw_supplements_dir, # Ensure new dirs are created
        monitor_dir, finance_scraped_dir,
        processed_committee_memberships_dir, processed_finance_dir
    ]
    for dir_path in dirs_to_create:
        dir_path.mkdir(parents=True, exist_ok=True)

    # --- Path Dictionary ---
    paths = {
        'base': base_dir,
        'raw': raw_data_dir,
        'processed': processed_data_dir,
        'artifacts': artifacts_dir,
        'log': log_dir,
        # Raw Subdirs
        'raw_sessions': raw_sessions_dir,
        'raw_legislators': raw_legislators_dir,
        'raw_committees': raw_committees_dir,
        'raw_bills': raw_bills_dir,
        'raw_votes': raw_votes_dir,
        'raw_sponsors': raw_sponsors_dir,
        'raw_texts': raw_texts_dir,
        'raw_amendments': raw_amendments_dir,
        'raw_supplements': raw_supplements_dir,
        # Raw Committee Membership (scraped, needs matching)
        'raw_committee_memberships': monitor_dir, # Store raw scraped JSON/HTML here
        # Artifact Subdirs
        'monitor': monitor_dir,
        'finance_scraped': finance_scraped_dir,
        # Processed Subdirs
        'processed_committee_memberships': processed_committee_memberships_dir,
        'processed_finance': processed_finance_dir,
    }

    # Log the created paths (optional, but helpful for debugging)
    # logger = logging.getLogger(__name__)
    # logger.info(f"Project paths initialized. Base: {base_dir}")
    # for key, path in paths.items():
    #     logger.debug(f"Path '{key}': {path}")

    return paths
