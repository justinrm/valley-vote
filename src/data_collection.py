# Standard library imports
import json
import time
import random
import logging
import argparse
from datetime import datetime
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional, Iterable
import re # Added for regex operations

# Third-party imports
# Requirements: pandas, requests, tenacity, tqdm, beautifulsoup4, thefuzz, python-Levenshtein
import requests
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
from tqdm import tqdm
from bs4 import BeautifulSoup
from thefuzz import process, fuzz

# --- Project Imports ---
from .config import (
    LEGISCAN_API_KEY, LEGISCAN_BASE_URL, LEGISCAN_MAX_RETRIES, LEGISCAN_DEFAULT_WAIT_SECONDS,
    DEFAULT_YEARS_START, COMMITTEE_MEMBER_MATCH_THRESHOLD,
    ID_HOUSE_COMMITTEES_URL, ID_SENATE_COMMITTEES_URL,
    ID_COMMITTEE_HEADING_SELECTORS, ID_COMMITTEE_CONTENT_SELECTORS, # Import selectors
    DATA_COLLECTION_LOG_FILE
)
from .utils import (
    setup_logging, save_json, convert_to_csv, fetch_page, load_json, clean_name # Use utils functions
)

# --- Configure Logging ---
# Logger setup is expected to be done by the calling script (e.g., main.py)
# Get logger instance for this module
logger = logging.getLogger(Path(DATA_COLLECTION_LOG_FILE).stem)

# --- Ensure API Key is Set ---
# Check is also done in config.py, but good to have here too for clarity
if not LEGISCAN_API_KEY:
    logger.critical("FATAL: LEGISCAN_API_KEY environment variable not set. API calls will fail.")
    # Depending on setup, throwing an error might be better:
    # raise ValueError("FATAL: LEGISCAN_API_KEY environment variable not set.")

# --- Constants & Mappings (from API Manual/Observation) ---
STATUS_CODES = {
    0: 'N/A', 1: 'Introduced', 2: 'Engrossed', 3: 'Enrolled', 4: 'Passed',
    5: 'Vetoed', 6: 'Failed', 7: 'Override', 8: 'Chaptered', 9: 'Refer',
    10: 'Report Pass', 11: 'Report DNP', 12: 'Draft', 13: 'Committee Process',
    14: 'Calendars', 15: 'Failed Vote', 16: 'Veto Override Pass', 17: 'Veto Override Fail'
    # Add more specific codes if observed or needed
}
VOTE_TEXT_MAP = {
    'yea': 1, 'aye': 1, 'yes': 1, 'pass': 1, 'y': 1,
    'nay': 0, 'no': 0, 'fail': 0, 'n': 0,
    'not voting': -1, 'abstain': -1, 'present': -1, 'nv': -1, 'av': -1,
    'absent': -2, 'excused': -2, 'abs': -2, 'exc': -2,
}
SPONSOR_TYPES = {
    0: 'Sponsor (Generic / Unspecified)', 1: 'Primary Sponsor', 2: 'Co-Sponsor', 3: 'Joint Sponsor'
}

# --- Custom Exceptions ---
class APIRateLimitError(Exception):
    """Custom exception for API rate limiting (HTTP 429)."""
    pass

class APIResourceNotFoundError(Exception):
    """Custom exception for resources not found (404 or specific API message)."""
    pass

class ScrapingStructureError(Exception):
    """Custom exception for unexpected website structure during scraping."""
    pass

# --- API Fetching Logic ---
# This uses tenacity for retries and handles common API errors.
@retry(
    stop=stop_after_attempt(LEGISCAN_MAX_RETRIES),
    wait=wait_exponential(multiplier=1.5, min=2, max=60), # Standard backoff
    # Retry only on potentially transient request exceptions or our custom rate limit error
    retry=retry_if_exception_type((requests.exceptions.RequestException, APIRateLimitError)),
    # Log before sleeping on retry using the root logger from utils
    before_sleep=before_sleep_log(logging.getLogger(), logging.WARNING) # Use root logger for tenacity sleep log
)
def fetch_api_data(operation: str, params: Dict[str, Any], wait_time: Optional[float] = None) -> Optional[Dict]:
    """
    Fetch data from LegiScan API with robust retry logic and error handling.

    Args:
        operation: API operation name (e.g., 'getSessionList').
        params: API parameters specific to the operation (excluding key and op).
        wait_time: Optional override for wait time before this specific call.

    Returns:
        Dictionary containing the JSON response data, or None on significant failure.

    Raises:
        APIRateLimitError: If HTTP 429 is received (triggers retry).
        APIResourceNotFoundError: If API indicates resource not found (404 or specific message).
        requests.exceptions.RequestException: For severe network issues after retries fail.
    """
    if not LEGISCAN_API_KEY: # Check again before making the call
        logger.error("Cannot fetch API data: LEGISCAN_API_KEY is not set.")
        return None

    # Prepare request parameters
    request_params = params.copy()
    request_params['key'] = LEGISCAN_API_KEY
    request_params['op'] = operation
    # Use 'id' if present, otherwise 'N/A' for logging clarity
    request_id_log = request_params.get('id', 'N/A')

    # Calculate sleep duration with jitter
    base_wait = wait_time if wait_time is not None else LEGISCAN_DEFAULT_WAIT_SECONDS
    sleep_duration = max(0.1, base_wait + random.uniform(-0.2, 0.4)) # Ensure non-negative
    logger.debug(f"Sleeping for {sleep_duration:.2f}s before LegiScan API request (op: {operation}, id: {request_id_log})")
    time.sleep(sleep_duration)

    try:
        logger.info(f"Fetching LegiScan API: op={operation}, id={request_id_log}")
        # Log parameters excluding the API key
        log_params = {k: v for k, v in request_params.items() if k != 'key'}
        logger.debug(f"Request params: {log_params}")

        # Use requests directly here for more control over API-specific errors.
        response = requests.get(LEGISCAN_BASE_URL, params=request_params, timeout=45, headers={'Accept': 'application/json'}) # Ensure JSON accept header

        # 1. Check for Rate Limit specifically (HTTP 429)
        if response.status_code == 429:
            logger.warning(f"LegiScan Rate limit hit (HTTP 429) for op={operation}, id={request_id_log}. Backing off...")
            # Raise custom error to potentially trigger tenacity retry based on decorator
            raise APIRateLimitError("Rate limit exceeded")

        # 2. Check for other HTTP errors using raise_for_status()
        # This will raise requests.exceptions.HTTPError for 4xx/5xx codes other than 429
        response.raise_for_status()

        # 3. Decode JSON carefully
        try:
            data = response.json()
        except json.JSONDecodeError:
            response_text_preview = response.text[:200] if response and hasattr(response, 'text') else "N/A"
            logger.error(f"Invalid JSON response from LegiScan op={operation} (id: {request_id_log}). Status: {response.status_code}. Preview: {response_text_preview}...")
            return None # Cannot process non-JSON

        # 4. Check LegiScan application-level status ('OK' or 'ERROR')
        status = data.get('status')
        if status == 'ERROR':
            error_msg = data.get('alert', {}).get('message', 'Unknown LegiScan API error')
            logger.error(f"LegiScan API error response for op={operation} (id: {request_id_log}): {error_msg}")
            # Distinguish "not found" errors from other API errors
            if "not found" in error_msg.lower() or \
               "invalid id" in error_msg.lower() or \
               "does not exist" in error_msg.lower() or \
               "no data" in error_msg.lower():
                logger.warning(f"LegiScan resource likely not found for op={operation}, id={request_id_log}.")
                # Raise specific error for calling function to handle
                raise APIResourceNotFoundError(f"Resource not found for {operation} id {request_id_log}: {error_msg}")
            # Treat other API 'ERROR' statuses as failures for this specific call
            return None

        elif status != 'OK':
             # Handle cases where status is missing or unexpected, but not explicitly 'ERROR'
             error_msg = data.get('alert', {}).get('message', f'Unexpected LegiScan API status: {status}')
             logger.error(f"Unexpected LegiScan API status for op={operation} (id: {request_id_log}): {error_msg}. Full response status: {status}")
             return None # Treat as failure for this call

        # 5. Success
        logger.debug(f"Successfully fetched LegiScan op={operation}, id={request_id_log}")
        return data

    # Handle specific exceptions during the request/response process
    except requests.exceptions.HTTPError as e:
        # This catches errors raised by response.raise_for_status() (excluding 429 handled above)
        status_code = e.response.status_code if e.response is not None else 'N/A'
        # Handle 404 Not Found specifically
        if status_code == 404:
            logger.warning(f"LegiScan HTTP 404 Not Found for op={operation}, id={request_id_log}. Assuming resource does not exist.")
            raise APIResourceNotFoundError(f"HTTP 404 Not Found for {operation} id {request_id_log}") from e
        # Client errors (4xx excluding 404, 429) usually mean bad request, don't retry
        elif 400 <= status_code < 500:
             logger.error(f"LegiScan Client error {status_code} fetching op={operation} (id: {request_id_log}): {e}. Check parameters.")
             return None # Signal failure for this call
        # Server errors (5xx) - log and let tenacity handle potential retry if RequestException is raised
        elif status_code >= 500:
            logger.error(f"LegiScan Server error {status_code} fetching op={operation} (id: {request_id_log}): {e}. Might retry.")
            # Re-raise the underlying RequestException to potentially trigger retry
            raise requests.exceptions.RequestException(f"Server error {status_code}") from e
        else:
             # Unexpected HTTPError code
             logger.error(f"Unhandled LegiScan HTTP error {status_code} fetching op={operation} (id: {request_id_log}): {e}", exc_info=True)
             raise requests.exceptions.RequestException(f"Unhandled HTTP error {status_code}") from e # Allow potential retry

    # Tenacity handles retries for Timeout and ConnectionError based on @retry decorator
    except requests.exceptions.RequestException as e:
        # Catches final RequestException after retries (Timeout, ConnectionError) or others raised above
        logger.error(f"Final LegiScan Request exception after retries for op={operation} (id: {request_id_log}): {str(e)}.")
        # Do not return None here, re-raise the final exception to signal failure clearly
        raise

# --- LegiScan Data Collection Functions ---

def get_session_list(state: str, years: Iterable[int], paths: Dict[str, Path]) -> List[Dict[str, Any]]:
    """Get list of LegiScan sessions for the state and year range, saving raw response."""
    logger.info(f"Fetching session list for {state} covering years {min(years)}-{max(years)}...")
    params = {'state': state}
    session_list = []
    # Define path for saving the raw response
    raw_sessions_path = paths['raw'] / f"legiscan_sessions_{state}_{min(years)}-{max(years)}.json"

    try:
        # Fetch data using the robust fetch_api_data function
        data = fetch_api_data('getSessionList', params)

        # Handle fetch failure or invalid response structure
        if not data or data.get('status') != 'OK' or 'sessions' not in data:
            logger.error(f"Failed to retrieve valid session list for state {state}. API response status: {data.get('status') if data else 'No response'}")
            # Save the potentially problematic response if it exists
            if data: save_json(data, raw_sessions_path.with_suffix('.error.json'))
            return [] # Return empty list on failure

        # Save the successful raw response using utils.save_json
        save_json(data, raw_sessions_path)

        target_years = set(years)
        api_sessions = data.get('sessions', [])

        if not isinstance(api_sessions, list):
            logger.error(f"LegiScan API returned unexpected format for sessions: {type(api_sessions)}. Expected list.")
            return []

        # Process each session entry
        for session in api_sessions:
            if not isinstance(session, dict) or 'session_id' not in session:
                logger.warning(f"Skipping invalid session entry in API response: {session}")
                continue
            try:
                session_id = session.get('session_id')
                year_start = int(session.get('year_start', 0))
                # Default end year to start year if missing or invalid
                year_end_str = session.get('year_end')
                year_end = int(year_end_str) if year_end_str and str(year_end_str).isdigit() else year_start

                if year_start == 0: # Skip sessions with invalid start year
                    logger.warning(f"Skipping session with invalid year_start 0: {session.get('session_name')}")
                    continue

                # Check if the session's year range overlaps with the target years
                session_years = set(range(year_start, year_end + 1))
                if not session_years.isdisjoint(target_years):
                    # Append relevant session details
                    session_list.append({
                        'session_id': session_id,
                        'state_id': session.get('state_id'),
                        'year_start': year_start,
                        'year_end': year_end,
                        'prefile': session.get('prefile'),
                        'sine_die': session.get('sine_die'),
                        'prior': session.get('prior'),
                        'special': session.get('special'),
                        'session_tag': session.get('session_tag'),
                        'session_title': session.get('session_title'),
                        'session_name': session.get('session_name'),
                        'dataset_hash': session.get('dataset_hash') # Include dataset hash
                    })
            except (ValueError, TypeError) as e:
                logger.warning(f"Skipping session due to invalid year data '{session.get('year_start')}-{session.get('year_end')}': {session.get('session_name')}. Error: {e}")
                continue

        if session_list:
             logger.info(f"Found {len(session_list)} relevant LegiScan sessions for {state} in specified years.")
             # Sort sessions by start year, most recent first
             session_list.sort(key=lambda s: s.get('year_start', 0), reverse=True)
        else:
             logger.warning(f"No relevant LegiScan sessions found for {state} covering {min(years)}-{max(years)}.")

    # Handle specific API errors during fetch
    except APIResourceNotFoundError:
         logger.error(f"Could not find state '{state}' via LegiScan API. Check state abbreviation.")
         return []
    except APIRateLimitError:
         logger.error(f"Hit LegiScan rate limit fetching session list for {state}. Try again later.")
         # Depending on main script logic, might need to re-raise or return specific code
         return []
    # Handle final RequestException after retries or other unexpected errors
    except Exception as e:
        logger.error(f"Unhandled exception fetching session list for {state}: {e}", exc_info=True)
        return []

    return session_list


def collect_legislators(state: str, sessions: List[Dict[str, Any]], paths: Dict[str, Path]):
    """
    Fetch legislator data using getSessionPeople for relevant sessions, deduplicate,
    and save raw individual JSONs and consolidated JSON/CSV outputs.
    """
    logger.info(f"Collecting legislator data for {state} across {len(sessions)} sessions...")
    # Use dict for deduplication by people_id (legislator_id)
    legislators_data: Dict[int, Dict[str, Any]] = {}
    # Get directories from the paths dictionary
    raw_legislators_dir = paths['raw_legislators']
    processed_data_dir = paths['processed']

    if not sessions:
        logger.warning("No sessions provided to collect_legislators. Cannot proceed.")
        return

    # Iterate through provided sessions to fetch people lists
    for session in tqdm(sessions, desc=f"Fetching legislators ({state})", unit="session"):
        session_id = session.get('session_id')
        session_name = session.get('session_name', f'ID: {session_id}')
        if not session_id:
            logger.warning(f"Session missing session_id: {session_name}. Skipping.")
            continue

        params = {'id': session_id}
        try:
            # Fetch data for the session
            data = fetch_api_data('getSessionPeople', params)
            # Check if fetch was successful and data structure is valid
            if not data or data.get('status') != 'OK' or 'sessionpeople' not in data:
                logger.warning(f"Failed to get valid people list for session {session_name} (ID: {session_id}). Status: {data.get('status', 'N/A')}")
                continue # Skip to next session

            # Extract the list of people from the response
            # Structure: {'status': 'OK', 'sessionpeople': {'session': {...}, 'people': [...]}}
            session_people_list = data.get('sessionpeople', {}).get('people', [])

            if not isinstance(session_people_list, list):
                logger.warning(f"No 'people' list found or invalid format in sessionpeople for session {session_id}.")
                continue
            if not session_people_list:
                 logger.info(f"No people found (empty list) for session {session_name} (ID: {session_id}).")
                 continue

            # Process each person found in the session list
            for person in session_people_list:
                if not isinstance(person, dict):
                    logger.warning(f"Skipping invalid person entry (not a dict): {person}")
                    continue

                legislator_id = person.get('people_id')
                # Add only if legislator_id is valid and not already stored (deduplication)
                if legislator_id and legislator_id not in legislators_data:
                    # Structure the legislator record based on API documentation
                    legislators_data[legislator_id] = {
                        'legislator_id': legislator_id, # Renamed from people_id for clarity
                        'person_hash': person.get('person_hash'),
                        'state_id': person.get('state_id'),
                        'name': person.get('name', ''), # Full name
                        'first_name': person.get('first_name', ''),
                        'middle_name': person.get('middle_name', ''),
                        'last_name': person.get('last_name', ''),
                        'suffix': person.get('suffix', ''),
                        'nickname': person.get('nickname', ''),
                        'party_id': person.get('party_id', ''), # '1', '2', '3', ...
                        'party': person.get('party', ''), # 'R', 'D', 'I', ...
                        'role_id': person.get('role_id'), # 1=Rep, 2=Sen, ...
                        'role': person.get('role', ''), # "Rep", "Sen", ...
                        'district': person.get('district', ''), # e.g., "HD001", "SD002"
                        'committee_sponsor': person.get('committee_sponsor', 0), # 0=No, 1=Yes
                        'committee_id': person.get('committee_id', 0), # If committee sponsor
                        'state': state.upper(), # Ensure state abbreviation is uppercase
                        # External IDs
                        'ftm_eid': person.get('ftm_eid'),
                        'votesmart_id': person.get('votesmart_id'),
                        'opensecrets_id': person.get('opensecrets_id'),
                        'knowwho_pid': person.get('knowwho_pid'),
                        'ballotpedia': person.get('ballotpedia'),
                        # These fields are NOT part of getSessionPeople response, explicitly set NA
                        'state_link': pd.NA,
                        'legiscan_url': pd.NA,
                        'active': 1 # Placeholder - activity might depend on session status or specific role checks later
                    }
                    # Save individual raw JSON file for this person
                    raw_leg_path = raw_legislators_dir / f"legislator_{legislator_id}.json"
                    save_json(person, raw_leg_path) # Use utils.save_json

        # Handle specific API errors for this session
        except APIResourceNotFoundError:
            logger.warning(f"Session people not found via API for session {session_name} (ID: {session_id}). Skipping.")
            continue
        except APIRateLimitError:
             logger.error(f"Hit LegiScan rate limit fetching people for session {session_id}. Consider pausing.")
             # Depending on script design, might want to break or sys.exit here
             break # Stop processing sessions for now if rate limited
        except Exception as e:
            logger.error(f"Unhandled exception fetching people for session {session_name} (ID: {session_id}): {e}", exc_info=True)
            continue # Skip to next session

    # --- Save Consolidated Legislator Data ---
    if legislators_data:
        legislator_list = list(legislators_data.values())
        logger.info(f"Collected {len(legislator_list)} unique legislators for {state} across relevant sessions.")

        # Define paths for consolidated files using the processed directory
        all_json_path = raw_legislators_dir / f'all_legislators_{state}.json' # Keep raw consolidated here for now
        processed_csv_path = processed_data_dir / f'legislators_{state}.csv'

        # Save consolidated JSON list
        save_json(legislator_list, all_json_path)

        # Define columns for processed CSV output
        csv_columns = [
            'legislator_id', 'person_hash', 'name', 'first_name', 'middle_name', 'last_name',
            'suffix', 'nickname', 'party_id', 'party', 'role_id', 'role',
            'district', 'state_id', 'state', 'active', 'committee_sponsor', 'committee_id',
            'ftm_eid', 'votesmart_id', 'opensecrets_id', 'knowwho_pid', 'ballotpedia',
            'state_link', 'legiscan_url' # Keep defined even if always NA from this source
        ]
        # Save processed CSV using utils function
        convert_to_csv(legislator_list, processed_csv_path, columns=csv_columns)
    else:
        logger.warning(f"No legislator data collected for state {state}. Creating empty placeholder files.")
        # Create empty placeholders
        processed_csv_path = processed_data_dir / f'legislators_{state}.csv'
        all_json_path = raw_legislators_dir / f'all_legislators_{state}.json'
        csv_columns = [ # Define columns even for empty file
            'legislator_id', 'person_hash', 'name', 'first_name', 'middle_name', 'last_name',
            'suffix', 'nickname', 'party_id', 'party', 'role_id', 'role',
            'district', 'state_id', 'state', 'active', 'committee_sponsor', 'committee_id',
            'ftm_eid', 'votesmart_id', 'opensecrets_id', 'knowwho_pid', 'ballotpedia',
            'state_link', 'legiscan_url'
        ]
        convert_to_csv([], processed_csv_path, columns=csv_columns)
        save_json([], all_json_path)


def collect_committee_definitions(session: Dict[str, Any], paths: Dict[str, Path]):
    """Fetch committee definitions for a single session using getCommittee."""
    session_id = session.get('session_id')
    session_name = session.get('session_name', f'ID: {session_id}')
    year = session.get('year_start')
    # Get raw committees directory from paths dict
    raw_committees_dir = paths['raw_committees']

    if not session_id or not year:
        logger.warning(f"Session missing ID or valid start year: {session_name}. Skipping committee definition collection.")
        return

    # Define year-specific subdirectory and ensure it exists
    year_dir = raw_committees_dir / str(year)
    year_dir.mkdir(parents=True, exist_ok=True)
    # Path for the processed list of committees for *this specific session*
    session_committees_json_path = year_dir / f'committees_{session_id}.json'

    logger.info(f"Collecting committee definitions for {year} session: {session_name} (ID: {session_id})...")
    params = {'id': session_id}
    processed_committees = [] # List to hold cleaned committee data for this session

    try:
        data = fetch_api_data('getCommittee', params) # API Operation: getCommittee
        # Handle fetch failure or invalid response
        if not data or data.get('status') != 'OK' or 'committees' not in data:
            logger.warning(f"Failed to retrieve valid committee definitions for session {session_id}. Status: {data.get('status', 'N/A')}")
            save_json([], session_committees_json_path) # Save empty list to indicate processed
            return

        # Structure: {'status':'OK', 'committees': [...] or { comité dict } }
        committees_data = data.get('committees', [])

        # Handle if API returns a single committee as a dict instead of a list
        if isinstance(committees_data, dict):
            if 'committee_id' in committees_data:
                 committees_data = [committees_data] # Convert single dict to list
            else:
                 logger.warning(f"Unexpected dict format for committees in session {session_id} (no committee_id): {str(committees_data)[:100]}")
                 committees_data = [] # Treat as invalid data

        if not isinstance(committees_data, list):
            logger.error(f"Unexpected committee data format for session {session_id}: {type(committees_data)}. Expected list or dict.")
            save_json([], session_committees_json_path)
            return

        # Process each committee dictionary
        for committee in committees_data:
            if not isinstance(committee, dict):
                logger.warning(f"Skipping invalid committee entry (not a dict): {committee}")
                continue
            committee_id = committee.get('committee_id')
            if committee_id:
                processed_committees.append({
                    'committee_id': committee_id,
                    'name': committee.get('committee_name', committee.get('name', '')), # Try both possible names
                    'chamber': committee.get('chamber', ''), # 'H', 'S', 'J'
                    'chamber_id': committee.get('chamber_id'), # Body ID (1=House, 2=Senate, 3=Joint)
                    'session_id': session_id, # Link back to session
                    'year': year # Denormalize year for easier access
                })
                # Save individual raw committee JSON data
                indiv_committee_path = year_dir / f"committee_{committee_id}.json"
                save_json(committee, indiv_committee_path) # Save the original dict
            else:
                logger.warning(f"Committee entry missing committee_id in session {session_id}: {committee}")

    # Handle specific API errors
    except APIResourceNotFoundError:
         logger.warning(f"No committees found via API for session {session_name} (ID: {session_id}). Saving empty list.")
         save_json([], session_committees_json_path)
         return
    except APIRateLimitError:
        logger.error(f"Hit LegiScan rate limit fetching committees for session {session_id}.")
        raise # Re-raise to signal the issue upstream
    except Exception as e:
        logger.error(f"Unhandled exception fetching committees for session {session_name} (ID: {session_id}): {e}", exc_info=True)
        # Save empty list on other errors to mark session as attempted
        save_json([], session_committees_json_path)
        return

    # Save the list of processed committees found for this session
    if processed_committees:
        logger.info(f"Collected {len(processed_committees)} committee definitions for session {session_id} ({year})")
    else:
        logger.warning(f"No valid committee definitions collected for session {session_id} ({year})")
    # Save the processed list (even if empty)
    save_json(processed_committees, session_committees_json_path)


def map_vote_value(vote_text: Optional[str]) -> int:
    """Map vote text to numeric values (1: Yea, 0: Nay, -1: Abstain/Present/NV, -2: Absent/Excused, -9: Other/Unknown)."""
    if vote_text is None: return -9
    # Standardize by lowercasing and stripping whitespace
    vt = str(vote_text).strip().lower()
    return VOTE_TEXT_MAP.get(vt, -9) # Default to -9 if not found in map


def collect_bills_votes_sponsors(session: Dict[str, Any], paths: Dict[str, Path]):
    """
    Fetch and save bills, associated votes (detailed roll calls), and sponsors
    for a single LegiScan session.
    Uses getMasterList, getBill, getRollCall.
    """
    session_id = session.get('session_id')
    session_name = session.get('session_name', f'ID: {session_id}')
    year = session.get('year_start')
    # Get relevant directories from paths dict
    bills_dir = paths['raw_bills']
    votes_dir = paths['raw_votes']
    sponsors_dir = paths['raw_sponsors']

    if not session_id or not year:
        logger.warning(f"Session missing ID or valid start year: {session_name}. Skipping bill/vote/sponsor collection.")
        return

    # Ensure year-specific subdirectories exist
    bills_year_dir = bills_dir / str(year); bills_year_dir.mkdir(parents=True, exist_ok=True)
    votes_year_dir = votes_dir / str(year); votes_year_dir.mkdir(parents=True, exist_ok=True)
    sponsors_year_dir = sponsors_dir / str(year); sponsors_year_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Collecting bills, votes, sponsors for {year} session: {session_name} (ID: {session_id})...")

    # 1. Get Master List of Bills for the session
    logger.info("Fetching master list...")
    params = {'id': session_id}
    bill_ids_to_fetch = []
    # Define paths for saving session-level processed lists (even if empty)
    session_bills_json_path = bills_year_dir / f'bills_{session_id}.json'
    session_sponsors_json_path = sponsors_year_dir / f'sponsors_{session_id}.json'
    session_votes_json_path = votes_year_dir / f'votes_{session_id}.json'

    try:
        master_list_data = fetch_api_data('getMasterList', params)
        # Handle fetch failure or invalid response
        if not master_list_data or master_list_data.get('status') != 'OK' or 'masterlist' not in master_list_data:
            logger.error(f"Failed to retrieve valid master bill list for session {session_id}. Status: {master_list_data.get('status', 'N/A')}")
            # Save empty lists to mark session as attempted
            save_json([], session_bills_json_path)
            save_json([], session_sponsors_json_path)
            save_json([], session_votes_json_path)
            return

        # Save the raw masterlist response
        save_json(master_list_data, bills_year_dir / f"masterlist_{session_id}.json")

        # Structure: {'status':'OK', 'masterlist': {'0':{bill_stub}, '1':{bill_stub}, ..., 'session':{...}}}
        masterlist = master_list_data.get('masterlist', {})
        if not isinstance(masterlist, dict) or not masterlist:
            logger.warning(f"Masterlist for session {session_id} is empty or not a dictionary ({type(masterlist)}). No bills to process.")
            save_json([], session_bills_json_path) # Save empty lists
            save_json([], session_sponsors_json_path)
            save_json([], session_votes_json_path)
            return

        # Extract bill_id from each entry in the masterlist dict (keys are usually numeric strings)
        for key, bill_stub in masterlist.items():
             # Check if value is a dict containing 'bill_id'
             if isinstance(bill_stub, dict) and 'bill_id' in bill_stub:
                  bill_ids_to_fetch.append(bill_stub['bill_id'])
             # else: logger.debug(f"Skipping non-standard entry in masterlist: key='{key}'") # Avoid logging 'session' block

        if not bill_ids_to_fetch:
            logger.warning(f"Masterlist for session {session_id} contained no valid bill entries. No bills processed.")
            save_json([], session_bills_json_path)
            save_json([], session_sponsors_json_path)
            save_json([], session_votes_json_path)
            return

        logger.info(f"Found {len(bill_ids_to_fetch)} bills in masterlist for session {session_id}. Fetching full details...")

    # Handle API errors during master list fetch
    except APIResourceNotFoundError:
         logger.warning(f"Master bill list not found via API for session {session_name} (ID: {session_id}). Skipping.")
         save_json([], session_bills_json_path)
         save_json([], session_sponsors_json_path)
         save_json([], session_votes_json_path)
         return
    except APIRateLimitError:
        logger.error(f"Hit LegiScan rate limit fetching master list for session {session_id}.")
        raise # Halt processing for this session
    except Exception as e:
        logger.error(f"Unhandled exception fetching master list for session {session_name} (ID: {session_id}): {e}", exc_info=True)
        return # Stop processing this session


    # 2. Fetch Details for Each Bill (getBill) and associated Roll Calls (getRollCall)
    session_bills = []      # List to hold processed bill data for this session
    session_votes = []      # List to hold processed vote data for this session
    session_sponsors = []   # List to hold processed sponsor data for this session
    bill_fetch_errors = 0
    vote_fetch_errors = 0

    # Process each bill_id found in the master list
    for bill_id in tqdm(bill_ids_to_fetch, desc=f"Processing bills for session {session_id} ({year})", unit="bill"):
        bill_params = {'id': bill_id}
        try:
            bill_data = fetch_api_data('getBill', bill_params)
            # Handle fetch failure or invalid response for this bill
            if not bill_data or bill_data.get('status') != 'OK' or 'bill' not in bill_data:
                logger.warning(f"Failed to retrieve valid full data for bill {bill_id}. Status: {bill_data.get('status', 'N/A')}")
                bill_fetch_errors += 1
                continue # Skip to next bill

            # Structure: {'status':'OK', 'bill': {bill_details...}}
            bill = bill_data['bill']
            if not isinstance(bill, dict):
                 logger.warning(f"Invalid bill data format for bill {bill_id} (not a dict): {type(bill)}")
                 bill_fetch_errors += 1
                 continue

            # Save raw bill JSON (includes 'status' and 'bill' object)
            save_json(bill_data, bills_year_dir / f"bill_{bill_id}.json")

            # --- Process Bill Information ---
            # Extract core bill details
            status_code = int(bill.get('status', 0)) # Ensure status is int
            bill_record = {
                'bill_id': bill.get('bill_id'),
                'change_hash': bill.get('change_hash'), # Important for delta updates
                'session_id': bill.get('session_id'), # Use session_id from bill data
                'year': year, # Denormalize year
                'state': bill.get('state', '').upper(), 'state_id': bill.get('state_id'),
                'url': bill.get('url'), # Legiscan URL for the bill
                'state_link': bill.get('state_link'), # Link to state legislature site
                'number': bill.get('bill_number', ''), # e.g., "HB 101"
                'type': bill.get('bill_type', ''), # e.g., 'B', 'R'
                'type_id': bill.get('bill_type_id'), # e.g., 1=Bill, 2=Resolution
                'body': bill.get('body', ''), # Originating body ('H', 'S', 'J')
                'body_id': bill.get('body_id'), # 1=House, 2=Senate, 3=Joint
                'current_body': bill.get('current_body', ''), # Current body holding the bill
                'current_body_id': bill.get('current_body_id'),
                'title': bill.get('title', ''),
                'description': bill.get('description', ''), # Often the same as title or longer summary
                'status': status_code, # Numeric status ID
                'status_desc': STATUS_CODES.get(status_code, 'Unknown'), # Map ID to description
                'status_date': bill.get('status_date', ''), # Date of last status change
                'pending_committee_id': bill.get('pending_committee_id', 0), # 0 if not in committee
            }

            # Extract subjects (list of dicts) into semicolon-separated strings
            subjects = bill.get('subjects', [])
            if isinstance(subjects, list):
                 bill_record['subjects'] = ';'.join(str(subj.get('subject_name', '')) for subj in subjects if isinstance(subj, dict))
                 bill_record['subject_ids'] = ';'.join(str(subj.get('subject_id', '')) for subj in subjects if isinstance(subj, dict))
            else: bill_record['subjects'] = ''; bill_record['subject_ids'] = ''

            # Extract SASTs (Same As/Similar To relations) into JSON string
            sasts = bill.get('sasts', [])
            sast_records = []
            if isinstance(sasts, list):
                for sast in sasts:
                    if isinstance(sast, dict): sast_records.append({k: sast.get(k) for k in ['type_id', 'type', 'sast_bill_number', 'sast_bill_id']})
            # Store complex list/dict structures as JSON strings in the main bill record
            bill_record['sast_relations'] = json.dumps(sast_records)

            # Extract Text stubs (references to bill text documents) into JSON string
            texts = bill.get('texts', [])
            text_stubs = []
            if isinstance(texts, list):
                for text in texts:
                    if isinstance(text, dict): text_stubs.append({k: text.get(k) for k in ['doc_id', 'date', 'type', 'type_id', 'mime', 'mime_id']})
            bill_record['text_stubs'] = json.dumps(text_stubs)

            # Extract Amendment stubs into JSON string
            amendments = bill.get('amendments', [])
            amendment_stubs = []
            if isinstance(amendments, list):
                for amd in amendments:
                    if isinstance(amd, dict): amendment_stubs.append({k: amd.get(k) for k in ['amendment_id', 'adopted', 'chamber', 'chamber_id', 'date', 'title']})
            bill_record['amendment_stubs'] = json.dumps(amendment_stubs)

            # Extract Supplement stubs (e.g., Fiscal Notes) into JSON string
            supplements = bill.get('supplements', [])
            supplement_stubs = []
            if isinstance(supplements, list):
                for supp in supplements:
                    if isinstance(supp, dict): supplement_stubs.append({k: supp.get(k) for k in ['supplement_id', 'date', 'type', 'type_id', 'title']})
            bill_record['supplement_stubs'] = json.dumps(supplement_stubs)

            # Add the processed bill record to the session list
            session_bills.append(bill_record)

            # --- Process Sponsors ---
            sponsors_list = bill.get('sponsors', [])
            if isinstance(sponsors_list, list):
                for sponsor in sponsors_list:
                    if isinstance(sponsor, dict):
                         sponsor_type_id = sponsor.get('sponsor_type_id') # 1=Primary, 2=Cosponsor
                         session_sponsors.append({
                             'bill_id': bill.get('bill_id'),
                             'legislator_id': sponsor.get('people_id'), # Link to legislator
                             'sponsor_type_id': sponsor_type_id,
                             'sponsor_type': SPONSOR_TYPES.get(sponsor_type_id, 'Unknown'),
                             'sponsor_order': sponsor.get('sponsor_order', 0),
                             'committee_sponsor': sponsor.get('committee_sponsor', 0), # Is it a committee sponsor?
                             'committee_id': sponsor.get('committee_id', 0), # If committee sponsor
                             'session_id': bill.get('session_id'), # Link to session
                             'year': year # Denormalize year
                         })
                    else: logger.warning(f"Invalid sponsor entry in bill {bill_id}: {sponsor}")
            else: logger.warning(f"Unexpected format for sponsors in bill {bill_id}: {type(sponsors_list)}")

            # --- Process Votes (fetch Roll Call details for each vote stub) ---
            # The 'votes' array in getBill response contains stubs (roll_call_id)
            votes_list_stubs = bill.get('votes', [])
            if isinstance(votes_list_stubs, list):
                for vote_stub in votes_list_stubs:
                     if not isinstance(vote_stub, dict):
                          logger.warning(f"Invalid vote stub entry in bill {bill_id}: {vote_stub}"); continue
                     # Correct identifier from vote stub is 'roll_call_id'
                     vote_id = vote_stub.get('roll_call_id')
                     if not vote_id:
                          logger.warning(f"Vote stub in bill {bill_id} missing roll_call_id: {vote_stub}"); continue

                     # Fetch detailed roll call data using getRollCall
                     roll_params = {'id': vote_id}
                     try:
                         roll_data = fetch_api_data('getRollCall', roll_params)
                         # Handle fetch failure or invalid response for this roll call
                         if not roll_data or roll_data.get('status') != 'OK' or 'roll_call' not in roll_data:
                             logger.warning(f"Failed to retrieve valid roll call data for vote {vote_id} (from bill {bill_id}). Status: {roll_data.get('status', 'N/A')}")
                             vote_fetch_errors += 1; continue

                         # Structure: {'status':'OK', 'roll_call': {vote_details...}}
                         roll_call = roll_data['roll_call']
                         if not isinstance(roll_call, dict):
                              logger.warning(f"Invalid roll_call data format for vote {vote_id} (not a dict): {type(roll_call)}")
                              vote_fetch_errors += 1; continue

                         # Save raw roll call JSON response
                         save_json(roll_data, votes_year_dir / f"vote_{vote_id}.json")

                         # Extract individual votes from the 'votes' array within the roll call object
                         individual_votes = roll_call.get('votes', [])
                         if isinstance(individual_votes, list):
                              for vote in individual_votes:
                                  if isinstance(vote, dict):
                                       # Link to legislator using 'people_id'
                                       legislator_id = vote.get('people_id')
                                       if legislator_id: # Only record votes linked to a legislator
                                           vote_record = {
                                               'vote_id': vote_id, # The roll_call_id
                                               'bill_id': roll_call.get('bill_id'), # Bill this vote belongs to
                                               'legislator_id': legislator_id, # Link to legislator
                                               'vote_id_type': vote.get('vote_id'), # API's numeric vote type (1=Yea, 2=Nay, 3=NV, 4=Absent)
                                               'vote_text': vote.get('vote_text', ''), # Raw text (e.g., "Yea", "Nay")
                                               'vote_value': map_vote_value(vote.get('vote_text')), # Mapped numeric value
                                               # Details from the roll call itself
                                               'date': roll_call.get('date', ''),
                                               'description': roll_call.get('desc', ''), # Description of the vote action
                                               'yea': roll_call.get('yea', 0), # Summary counts
                                               'nay': roll_call.get('nay', 0),
                                               'nv': roll_call.get('nv', 0),
                                               'absent': roll_call.get('absent', 0),
                                               'total': roll_call.get('total', 0),
                                               'passed': int(roll_call.get('passed', 0)), # Did the measure pass on this vote? (0=No, 1=Yes)
                                               'chamber': roll_call.get('chamber', ''), # 'H' or 'S'
                                               'chamber_id': roll_call.get('chamber_id'), # 1 or 2
                                               # Link back to the bill's session and year
                                               'session_id': bill.get('session_id'),
                                               'year': year,
                                           }
                                           session_votes.append(vote_record)
                                       else:
                                           logger.debug(f"Vote record in roll call {vote_id} missing legislator ID (people_id): {vote}")
                                  else:
                                       logger.warning(f"Invalid individual vote entry in roll call {vote_id}: {vote}")
                         else:
                              logger.warning(f"Unexpected format for 'votes' array within roll call {vote_id}: {type(individual_votes)}")

                     # Handle API errors specifically for getRollCall
                     except APIResourceNotFoundError:
                         logger.warning(f"Roll Call ID {vote_id} (from bill {bill_id}) not found via API. Skipping.")
                         vote_fetch_errors += 1; continue
                     except APIRateLimitError:
                          logger.error(f"Hit LegiScan rate limit fetching roll call {vote_id}.")
                          raise # Halt processing for this bill/session
                     except Exception as e_vote:
                         logger.error(f"Unhandled exception fetching/processing roll call {vote_id} (from bill {bill_id}): {e_vote}", exc_info=True)
                         vote_fetch_errors += 1; continue
            else:
                # This means the 'votes' key in the getBill response wasn't a list
                logger.warning(f"Unexpected format for votes list stub in bill {bill_id}: {type(votes_list_stubs)}")

        # Handle API errors specifically for getBill
        except APIResourceNotFoundError:
            logger.warning(f"Bill ID {bill_id} not found via API. Skipping.")
            bill_fetch_errors += 1
            continue
        except APIRateLimitError:
             logger.error(f"Hit LegiScan rate limit fetching bill {bill_id}.")
             raise # Halt processing for this session
        except Exception as e_bill:
            logger.error(f"Unhandled exception processing bill {bill_id}: {e_bill}", exc_info=True)
            bill_fetch_errors += 1
            continue # Skip to next bill_id

    # --- Save consolidated processed lists for the entire session ---
    logger.info(f"Finished processing session {session_id}. Results: Bills={len(session_bills)}, Sponsors={len(session_sponsors)}, Votes={len(session_votes)}.")
    if bill_fetch_errors > 0: logger.warning(f"Bill fetch errors encountered: {bill_fetch_errors}")
    if vote_fetch_errors > 0: logger.warning(f"Vote fetch errors encountered: {vote_fetch_errors}")

    # Save the processed lists (JSON format) for this session
    save_json(session_bills, session_bills_json_path)
    save_json(session_sponsors, session_sponsors_json_path)
    save_json(session_votes, session_votes_json_path)

    # Yearly consolidation into CSV happens in the consolidate_yearly_data function


def consolidate_yearly_data(data_type: str, years: Iterable[int], columns: List[str], state_abbr: str, paths: Dict[str, Path]):
    """Consolidates individual session JSON files into yearly JSON and CSV files."""
    logger.info(f"Consolidating {data_type} data for {state_abbr}, years {min(years)}-{max(years)}...")
    # Determine raw data directory based on data_type (e.g., 'raw_bills')
    raw_base_dir = paths.get(f'raw_{data_type}')
    processed_base_dir = paths.get('processed') # Consolidated CSVs go here

    if not raw_base_dir or not processed_base_dir:
        logger.error(f"Cannot consolidate: Invalid data_type '{data_type}' or missing directories ('raw_{data_type}', 'processed') in paths dict.")
        return

    # Define primary keys for deduplication (can be single string or list for composite)
    primary_keys = {
        'legislators': 'legislator_id', # Should already be unique from collect_legislators
        'committees': 'committee_id',
        'bills': 'bill_id',
        'sponsors': ['bill_id', 'legislator_id', 'sponsor_type_id', 'committee_id'], # Composite key
        'votes': ['vote_id', 'legislator_id'] # Composite key for individual votes
        # Add keys for other types if consolidated (texts, amendments, etc.)
    }
    primary_key = primary_keys.get(data_type)
    if primary_key:
         logger.info(f"Will attempt deduplication for {data_type} using key(s): {primary_key}")

    # Process each year in the specified range
    for year in tqdm(years, desc=f"Consolidating {data_type} ({state_abbr})", unit="year"):
        year_dir = raw_base_dir / str(year)
        all_year_data = [] # Accumulate data for the entire year

        if not year_dir.is_dir():
            logger.debug(f"Year directory not found, skipping consolidation for {data_type}, year {year}: {year_dir}")
            continue

        logger.debug(f"Scanning {year_dir} for session files matching pattern '{data_type}_<session_id>.json'")
        files_processed = 0
        # Iterate through files in the year directory
        for filepath in year_dir.glob(f"{data_type}_*.json"):
            # Basic check to ensure it looks like a session file (e.g., committees_1234.json)
            # Avoid accidentally consolidating 'all_...' files or other mismatches.
            filename_match = re.match(rf"^{data_type}_(\d+)\.json$", filepath.name)
            if not filename_match:
                if not filepath.name.startswith('all_'): # Don't warn about the 'all_' file we create
                     logger.debug(f"Skipping file, does not match expected session pattern '{data_type}_<session_id>.json': {filepath.name}")
                continue

            session_id_from_file = filename_match.group(1)
            logger.debug(f"Reading session file: {filepath} (Session ID: {session_id_from_file})")
            files_processed += 1
            try:
                # Use utils.load_json to load data from the session file
                session_data = load_json(filepath)
                # Check if loading was successful and data is a list
                if isinstance(session_data, list):
                    all_year_data.extend(session_data) # Add records to the yearly list
                elif session_data is None: # load_json returns None on error or empty file
                    logger.warning(f"Session file {filepath} was empty or failed to load.")
                else:
                    logger.warning(f"Expected list in session file {filepath}, got {type(session_data)}. Skipping content.")
            except Exception as e: # Catch unexpected errors during file loading/processing
                logger.error(f"Error processing session file {filepath}: {e}. Skipping file.", exc_info=True)

        if files_processed == 0:
            logger.debug(f"No session files matching pattern found in {year_dir}.")

        # Define output paths for the consolidated yearly data
        # Consolidated JSON stays in the raw year directory
        year_json_path = year_dir / f'all_{data_type}_{year}_{state_abbr}.json'
        # Consolidated CSV goes to the main processed directory
        year_csv_path = processed_base_dir / f'{data_type}_{year}_{state_abbr}.csv'

        # Process the accumulated data for the year (deduplication, saving)
        if all_year_data:
            original_count = len(all_year_data)
            unique_data = all_year_data
            # --- Deduplication Logic ---
            if primary_key:
                seen_ids = set()
                unique_data_list = []
                duplicates_found = 0
                logger.debug(f"Deduplicating {original_count} {data_type} records for {year} using key(s): {primary_key}")

                for item in all_year_data:
                    # Ensure item is a dictionary before attempting key access
                    if not isinstance(item, dict):
                         duplicates_found +=1 # Treat non-dicts as items to remove
                         logger.warning(f"Skipping non-dictionary item found in {data_type} data for {year}: {item}")
                         continue

                    item_id = None
                    key_complete = True
                    # Handle composite keys (list of key names)
                    if isinstance(primary_key, list):
                        try:
                            # Create tuple of values for the composite key
                            id_tuple = tuple(item.get(pk) for pk in primary_key) # Use get for safety
                            # Check if any part of the composite key is None/missing (use pd.isna for broad check)
                            if any(pd.isna(val) for val in id_tuple):
                                key_complete = False
                            else:
                                item_id = id_tuple
                        except Exception as e_key: # Catch potential errors if keys don't exist (though .get prevents KeyError)
                             logger.warning(f"Error accessing composite key {primary_key} in {data_type} for {year}: {e_key}. Record: {str(item)[:200]}...")
                             key_complete = False
                             # Decide policy: keep or discard records with incomplete composite keys? Keeping for now.
                    # Handle single primary key (string)
                    else:
                        item_id = item.get(primary_key)
                        if pd.isna(item_id): # Check if single key is missing
                             key_complete = False

                    # If key is incomplete, decide whether to keep or discard
                    if not key_complete:
                         logger.debug(f"Record has incomplete/missing key '{primary_key}'. Keeping.")
                         unique_data_list.append(item)
                         continue # Keep record, don't add to seen_ids

                    # If key is complete and valid
                    if item_id is not None:
                       if item_id not in seen_ids:
                           unique_data_list.append(item)
                           seen_ids.add(item_id) # Add the unique ID (or tuple) to the set
                       else:
                           duplicates_found += 1
                           # logger.debug(f"Duplicate found for key {item_id}: {item}") # Can be verbose
                    else: # Should be covered by key_complete check now
                         logger.warning(f"Record has None primary key '{primary_key}' after checks. Keeping.")
                         unique_data_list.append(item)

                if duplicates_found > 0:
                     logger.info(f"Removed {duplicates_found} duplicate/invalid {data_type} records based on key '{primary_key}' for {year}.")
                unique_data = unique_data_list # Use the deduplicated list

            final_count = len(unique_data)
            logger.info(f"Consolidated {final_count} unique {data_type} records for {year} (from {original_count} raw).")
            # Save consolidated yearly data using utils
            save_json(unique_data, year_json_path)
            convert_to_csv(unique_data, year_csv_path, columns=columns)
        else:
            logger.warning(f"No {data_type} data found or consolidated for {year}. Creating empty output files.")
            # Create empty files to signify year was processed
            save_json([], year_json_path)
            convert_to_csv([], year_csv_path, columns=columns)


# --- Web Scraping Functions (Idaho Specific Committee Memberships) ---

def parse_idaho_committee_page(html: str, chamber: str, current_year: int) -> List[Dict[str, Any]]:
    """Parse committee data from Idaho Legislature HTML (fragile)."""
    # This function remains largely the same as the previous refactored version,
    # ensuring it uses config for selectors and utils.clean_name if applicable.
    if not html:
        logger.warning(f"No HTML content provided for parsing {chamber} committees.")
        return []

    soup = BeautifulSoup(html, 'html.parser')
    committees = []
    logger.info(f"Parsing {chamber} committee page for year {current_year}...")

    # Selectors imported from config
    potential_headings = soup.find_all(ID_COMMITTEE_HEADING_SELECTORS)
    ignore_headings = ['search', 'legislative services office', 'contact information',
                       'about committees', 'committee schedules', 'meeting notices', 'standing committees'] # Add more ignore terms
    parsed_committee_names = set()

    for heading_tag in potential_headings:
        committee_name = heading_tag.get_text(strip=True)
        # Improve skipping logic
        committee_name_lower = committee_name.lower()
        if (not committee_name or len(committee_name) < 5 or
                any(ignore in committee_name_lower for ignore in ignore_headings) or
                committee_name in parsed_committee_names or
                "committee assignments" in committee_name_lower): # Skip assignment lists
             logger.debug(f"Skipping potential non-committee or duplicate section: '{committee_name}'")
             continue

        logger.info(f"Processing potential committee: '{committee_name}'")
        members = []
        found_members = False

        # Look for members in subsequent content tags until the next heading
        next_element = heading_tag.find_next_sibling()
        potential_member_tags = []
        search_depth = 0
        max_search_depth = 10 # Look deeper for content tags

        while next_element and next_element.name not in ID_COMMITTEE_HEADING_SELECTORS and search_depth < max_search_depth:
            if next_element.name in ID_COMMITTEE_CONTENT_SELECTORS:
                if next_element.name in ['ul', 'ol']:
                    # Get direct children 'li' tags that contain some text
                    items = [li for li in next_element.find_all('li', recursive=False) if li.get_text(strip=True)]
                    potential_member_tags.extend(items)
                elif next_element.get_text(strip=True): # Only consider non-empty paragraphs etc.
                    potential_member_tags.append(next_element)
            # Look inside containers too (divs, etc.)
            elif hasattr(next_element, 'find_all'):
                 # Use recursive=True here to find content within nested tags if needed
                 contained_content = next_element.find_all(ID_COMMITTEE_CONTENT_SELECTORS, recursive=True)
                 for tag in contained_content:
                      if tag.name in ['ul', 'ol']:
                           items = [li for li in tag.find_all('li', recursive=False) if li.get_text(strip=True)]
                           potential_member_tags.extend(items)
                      elif tag.name == 'p' and tag.get_text(strip=True): # Only non-empty paragraphs
                           potential_member_tags.append(tag)

            next_element = next_element.find_next_sibling()
            search_depth += 1


        if not potential_member_tags:
             logger.warning(f"Could not find expected member list tags ({ID_COMMITTEE_CONTENT_SELECTORS}) following committee heading '{committee_name}'. Structure might have changed.")
             continue # Skip this committee if no member list found

        # Extract names from potential tags
        processed_texts = set() # Avoid processing exact same text block twice
        for tag in potential_member_tags:
            # Get text, joining pieces with spaces, stripping whitespace
            text = tag.get_text(separator=' ', strip=True)
            if not text or len(text) < 3 or text in processed_texts:
                 if text in processed_texts: logger.debug(f"Skipping duplicate text block: {text[:50]}...")
                 continue
            processed_texts.add(text)

            # Simple split by newline or multiple spaces if it looks like a list within one tag
            parts_to_process = [text]
            if '\n' in text:
                 parts_to_process = [p.strip() for p in text.split('\n') if p.strip()]
            elif '  ' in text and text.count('  ') > 2: # Heuristic for space-separated list
                 parts_to_process = [p.strip() for p in text.split('  ') if len(p.strip()) > 2]


            for part in parts_to_process:
                 role = 'Member' # Default role
                 name_part = part.strip()

                 # Check for roles (case-insensitive, more robust)
                 role_patterns = {
                     # Order matters: check longer titles first
                     'Vice Chair': r'^(Vice Chair|Vice-Chair)\s*:?\s*',
                     'Chair': r'^(Chair|Chairman)\s*:?\s*',
                     # Add Secretary, etc. if needed, decide if they should be skipped
                     # 'Secretary': r'^Secretary\s*:?\s*',
                 }
                 matched_role = False
                 for r, pattern in role_patterns.items():
                     match = re.match(pattern, name_part, re.IGNORECASE)
                     if match:
                         role = r
                         name_part = name_part[match.end():].strip() # Get text after the role
                         # Check if remaining part is empty, indicating just a title was found
                         if not name_part:
                              logger.debug(f"Found role '{r}' but no name followed in: '{part}'")
                              name_part = None # Mark as invalid name part
                         matched_role = True
                         break

                 if name_part is None: continue # Skip if only role was found

                 # Skip entries that look like addresses, phone numbers, or just titles
                 if re.match(r'^\d+\s|P\.?O\.?\sBox|Room\s\d+|Phone:|Fax:', name_part, re.IGNORECASE):
                      logger.debug(f"Skipping likely non-name entry: '{name_part}'")
                      continue
                 if name_part.lower() in ['representative', 'senator']: # Skip if only title remains
                      logger.debug(f"Skipping likely title-only entry: '{name_part}'")
                      continue


                 # Clean name using utility function + specific removals
                 cleaned_name = clean_name(name_part) # General cleaning (titles like Rep./Sen., suffixes like Jr./Sr.)
                 # Remove bracketed info like (R-District 5) or (City)
                 cleaned_name = re.split(r'\s+\(', cleaned_name)[0].strip()
                 # Handle potential "LastName, FirstName" format, check for common middle initials
                 name_parts_comma = cleaned_name.split(',')
                 if len(name_parts_comma) == 2:
                     last, first = name_parts_comma[0].strip(), name_parts_comma[1].strip()
                     # Check if 'first' looks like a first name (and optional middle initial)
                     if re.match(r"^[A-Za-z\-'.]+(\s[A-Z]\.?)?$", first) and len(last) > 1: # Allow ' and . in names
                         possible_name = f"{first} {last}"
                         # Basic validation it looks like a name
                         if not any(char.isdigit() for char in possible_name) and len(possible_name) > 4:
                             cleaned_name = possible_name
                             logger.debug(f"Reformatted name from 'Last, First': {cleaned_name}")

                 # Final cleanup and validation
                 final_name = cleaned_name.strip(',- ')
                 if final_name and len(final_name) > 3 and not final_name.isdigit() and '<' not in final_name:
                      # Avoid adding duplicates within the same committee
                      if not any(m['name'] == final_name for m in members):
                           members.append({'name': final_name, 'role': role})
                           logger.debug(f"  Found member: Name='{final_name}', Role='{role}'")
                           found_members = True
                      else: logger.debug(f"  Skipping duplicate member: '{final_name}' in committee '{committee_name}'")
                 # else: logger.debug(f"Skipping invalid/short name part: '{part}' -> '{final_name}'")


        if found_members:
             # Generate a somewhat stable ID based on name, chamber, year
             c_name_slug = re.sub(r'\W+', '_', committee_name).lower().strip('_')[:50] # Sanitize for ID
             committee_id_scraped = f"ID_{chamber.lower()}_{c_name_slug}_{current_year}"
             committees.append({
                 'committee_name_scraped': committee_name,
                 'committee_id_scraped': committee_id_scraped, # Generated ID
                 'chamber': chamber,
                 'members': members, # List of {'name': ..., 'role': ...} dicts
                 'year': current_year,
                 'source_url': f"Idaho Legislature Website ({chamber})" # Source info
             })
             parsed_committee_names.add(committee_name) # Mark as successfully parsed
             logger.info(f"Successfully parsed committee: '{committee_name}' with {len(members)} members.")
        elif committee_name not in parsed_committee_names:
             # Only warn if we expected to parse but didn't find members
             logger.warning(f"Found committee title '{committee_name}' but failed to parse any valid members.")

    logger.info(f"Finished parsing {chamber} page. Found {len(committees)} committees with members.")
    # Raise error if headings found but no committees parsed, indicating structure change
    if not committees and potential_headings:
         logger.error(f"Found {len(potential_headings)} potential committee headings but failed to parse any committees with members on {chamber} page.")
         raise ScrapingStructureError(f"No committees parsed from {chamber} page despite finding headings. Structure likely changed.")
    return committees


def scrape_committee_memberships(state_abbr: str, paths: Dict[str, Path]) -> Optional[Path]:
    """Scrape committee memberships from State Legislature website (Idaho specific)."""
    # Currently only implemented for Idaho
    if state_abbr != 'ID':
        logger.error(f"Web scraping for committee memberships is only implemented for ID, not {state_abbr}.")
        return None

    # URLs from config
    state_configs = {
        'ID': {
            'base_name': 'Idaho Legislature',
            'urls': { 'House': ID_HOUSE_COMMITTEES_URL, 'Senate': ID_SENATE_COMMITTEES_URL },
            'parser': parse_idaho_committee_page
        }
    }
    config = state_configs[state_abbr]
    urls = config['urls']
    parser_func = config['parser']
    source_name = config['base_name']

    all_scraped_committees = [] # Holds dicts for each committee found
    current_year = datetime.now().year
    # Use paths dictionary for raw membership data
    scraped_memberships_dir = paths['raw_committee_memberships'] / str(current_year)
    scraped_memberships_dir.mkdir(parents=True, exist_ok=True)
    # Directory for saving raw HTML artifacts
    monitor_artifacts_dir = paths['artifacts'] / 'monitor' # Use main artifacts dir now
    monitor_artifacts_dir.mkdir(parents=True, exist_ok=True)


    logger.info(f"--- Starting Web Scraping for {state_abbr} ({source_name}) Committee Memberships ({current_year}) ---")
    logger.warning(f"Web scraping depends on the '{source_name}' website structure and is FRAGILE.")

    scrape_completely_failed = True # Assume failure until success
    for chamber, url in urls.items():
        logger.info(f"Scraping {chamber} committees from: {url}")
        raw_html_path = monitor_artifacts_dir / f"{state_abbr}_{chamber}_committees_{current_year}.html" # Save HTML for audit
        try:
            # Use utils.fetch_page with retries
            html = fetch_page(url)
            if not html:
                logger.error(f"Failed to fetch HTML content for {chamber} from {url} after retries. Skipping.")
                # Don't mark as success, loop continues
                continue

            # Save raw HTML for debugging/auditing
            try:
                 raw_html_path.write_text(html, encoding='utf-8', errors='replace')
                 logger.info(f"Saved raw HTML for {chamber} to {raw_html_path}")
            except Exception as e_write:
                 logger.warning(f"Could not save raw HTML to {raw_html_path}: {e_write}")

            # Parse the HTML using the state-specific parser
            committees = parser_func(html, chamber, current_year) # Can raise ScrapingStructureError

            # If parser returns empty list (but didn't raise error), log warning
            if not committees:
                logger.warning(f"Parser did not return any committees for {chamber} from {url}, though fetch succeeded.")
                # This might still be a structure issue, but less severe than a raised error
            else:
                 all_scraped_committees.extend(committees)
                 scrape_completely_failed = False # Mark success if at least one chamber yields data

                 # Save individual raw committee JSON (includes member list)
                 for committee in committees:
                      c_id = committee['committee_id_scraped']
                      json_path = scraped_memberships_dir / f"{c_id}_raw_scraped.json"
                      save_json(committee, json_path) # Use utils

        except ScrapingStructureError as e_struct: # Catch specific structure errors raised by parser
            logger.error(f"SCRAPING STRUCTURE ERROR for {chamber} at {url}: {e_struct}")
            logger.error("Halting further scraping for this run due to likely website change.")
            return None # Stop all scraping if structure error is detected
        except Exception as e:
             logger.error(f"Unhandled error during scraping/parsing for {chamber} at {url}: {e}", exc_info=True)
             # Continue to next chamber unless it's critical

    # --- Process and Save Results ---
    consolidated_raw_path = None
    if all_scraped_committees:
        logger.info(f"Total committees scraped with members across all chambers for {current_year}: {len(all_scraped_committees)}")
        # Flatten the data: one row per member per committee
        flat_memberships = []
        for committee in all_scraped_committees:
            committee_id = committee.get('committee_id_scraped')
            committee_name = committee.get('committee_name_scraped')
            chamber = committee.get('chamber')
            year = committee.get('year')
            for member in committee.get('members', []):
                 flat_memberships.append({
                     'committee_id_scraped': committee_id,
                     'committee_name_scraped': committee_name,
                     'chamber': chamber,
                     'year': year,
                     'legislator_name_scraped': member.get('name'), # Name already cleaned by parser
                     'role_scraped': member.get('role'),
                     # Placeholders to be filled by matching function
                     'legislator_id': None,
                     'match_score': None,
                     'matched_api_name': None
                 })
        logger.info(f"Generated {len(flat_memberships)} flat membership records.")

        # Save consolidated flat list (raw, before matching) as JSON
        consolidated_raw_path = scraped_memberships_dir / f'scraped_memberships_raw_{state_abbr}_{current_year}.json'
        save_json(flat_memberships, consolidated_raw_path) # Use utils

        # Save a CSV version of the raw scraped data (before matching)
        raw_csv_path = scraped_memberships_dir / f'raw_scraped_memberships_{state_abbr}_{current_year}.csv'
        raw_csv_columns = [ # Only include fields available directly from scraping
            'committee_id_scraped', 'committee_name_scraped', 'chamber', 'year',
            'legislator_name_scraped', 'role_scraped'
        ]
        convert_to_csv(flat_memberships, raw_csv_path, columns=raw_csv_columns) # Use utils

        logger.info(f"--- Finished Web Scraping Phase for {state_abbr} Committees ({current_year}) ---")
        return consolidated_raw_path # Return Path object to the raw JSON file

    elif not scrape_completely_failed:
         # Scraping ran, fetched pages, but parser found nothing (logged warnings earlier)
         logger.warning(f"Web scraping completed but no committee memberships were successfully extracted for {state_abbr} ({current_year}). Check parser logic and website structure.")
         return None
    else:
        # Scraping failed entirely (e.g., fetch errors for all URLs)
        logger.error(f"Web scraping failed to retrieve any data for {state_abbr} ({current_year}).")
        return None


def match_scraped_legislators(scraped_memberships_json_path: Path, legislators_json_path: Path, output_csv_path: Path, paths: Dict[str, Path]) -> bool:
    """Match scraped committee member names to official legislator IDs using fuzzy matching."""
    # Check inputs
    if not scraped_memberships_json_path or not scraped_memberships_json_path.is_file():
        logger.error(f"Scraped memberships JSON file not found or not provided: {scraped_memberships_json_path}. Cannot perform matching.")
        return False
    if not legislators_json_path or not legislators_json_path.is_file():
        logger.error(f"Consolidated legislators JSON file not found: {legislators_json_path}. Cannot perform matching.")
        return False

    logger.info(f"--- Starting Scraped Member Matching Phase ---")
    logger.info(f"Matching: {scraped_memberships_json_path.name}")
    logger.info(f"Against: {legislators_json_path.name}")
    logger.info(f"Output CSV: {output_csv_path}")

    try:
        # Load data using utils.load_json
        legislators_list = load_json(legislators_json_path)
        if not isinstance(legislators_list, list): # Checks for None return or wrong type
            logger.error(f"Legislators file {legislators_json_path} is empty or invalid. Cannot perform matching.")
            return False

        scraped_memberships = load_json(scraped_memberships_json_path) # Should be the flat list
        if not isinstance(scraped_memberships, list):
             logger.error(f"Scraped memberships file {scraped_memberships_json_path} is empty or invalid. Matching aborted.")
             # Create empty output files if input was invalid/empty but existed
             if scraped_memberships is not None: # Only create if file existed but was bad format/empty
                 csv_columns = ['committee_id_scraped', 'committee_name_scraped', 'chamber', 'year','legislator_name_scraped', 'role_scraped', 'legislator_id','matched_api_name', 'match_score']
                 convert_to_csv([], output_csv_path, columns=csv_columns)
                 matched_json_output_path = scraped_memberships_json_path.with_suffix('.matched.json')
                 save_json([], matched_json_output_path)
             return False # Indicate failure if data invalid

    except Exception as e:
        logger.error(f"Unexpected error loading data for matching: {str(e)}", exc_info=True)
        return False

    # Prepare legislator data for matching: Use 'name' field (Full Name)
    # Filter for valid entries with both name and ID
    valid_legislators = [l for l in legislators_list if isinstance(l, dict) and l.get('name') and l.get('legislator_id')]
    if len(valid_legislators) != len(legislators_list):
        logger.warning(f"Filtered out {len(legislators_list) - len(valid_legislators)} invalid legislator entries (missing name or ID) from {legislators_json_path}")

    if not valid_legislators:
         logger.error(f"No valid legislator entries with 'name' and 'legislator_id' found in {legislators_json_path}. Matching aborted.")
         return False

    # Create choices list (names) and mapping (name to full legislator dict for details)
    legislator_names_choices = [l['name'] for l in valid_legislators]
    # Handle potential duplicate names - store list of legislators per name?
    # For simplicity now, assume last one wins if names collide, but log warning.
    legislator_name_to_data = {l['name']: l for l in valid_legislators}
    if len(legislator_name_to_data) != len(valid_legislators):
         logger.warning("Duplicate legislator names found in API data. Matching will use the last encountered legislator for that name.")


    # --- Perform Matching ---
    matched_count = 0
    unmatched_count = 0
    updated_memberships = [] # List to store results

    # Use threshold from config
    threshold = COMMITTEE_MEMBER_MATCH_THRESHOLD
    logger.info(f"Using fuzzy match threshold: {threshold}")

    for membership in tqdm(scraped_memberships, desc="Matching scraped names", unit="record"):
        if not isinstance(membership, dict):
             logger.warning(f"Skipping invalid membership record (not a dict): {membership}")
             unmatched_count += 1
             continue

        # Start with a copy, ensure required fields exist, default match fields to NA/0
        updated_record = membership.copy()
        updated_record['legislator_id'] = pd.NA # Use pandas NA for consistency in CSV
        updated_record['match_score'] = 0
        updated_record['matched_api_name'] = pd.NA

        scraped_name = membership.get('legislator_name_scraped')
        # Parser should provide cleaned name, but validate anyway
        if not scraped_name or not isinstance(scraped_name, str) or len(scraped_name) < 3:
            logger.debug(f"Membership record missing valid 'legislator_name_scraped': {str(membership)[:100]}")
            unmatched_count += 1
            updated_memberships.append(updated_record)
            continue

        # Perform fuzzy matching using Weighted Ratio for better results with name variations
        # process.extractOne returns (choice, score) or None if below cutoff
        match_result = process.extractOne(scraped_name, legislator_names_choices, scorer=fuzz.WRatio, score_cutoff=threshold)

        if match_result:
            best_match_name, score = match_result
            matched_leg_data = legislator_name_to_data.get(best_match_name)
            if matched_leg_data: # Should always be found if name was in choices
                updated_record['legislator_id'] = matched_leg_data['legislator_id']
                updated_record['match_score'] = score
                updated_record['matched_api_name'] = best_match_name
                # Optionally add more details from matched legislator for verification
                # updated_record['matched_party'] = matched_leg_data.get('party')
                # updated_record['matched_district'] = matched_leg_data.get('district')
                logger.debug(f"Matched '{scraped_name}' -> '{best_match_name}' (ID: {matched_leg_data['legislator_id']}, Score: {score})")
                matched_count += 1
            else:
                # This indicates an internal logic error (name in choices but not in map)
                logger.error(f"Internal error: Matched name '{best_match_name}' not found in name-to-data map.")
                unmatched_count += 1
        else:
            # No match found above the threshold
            logger.warning(f"No match found for scraped name: '{scraped_name}' (Threshold: {threshold})")
            unmatched_count += 1

        updated_memberships.append(updated_record)

    logger.info(f"Legislator matching complete: {matched_count} matched, {unmatched_count} unmatched.")

    # --- Save Matched Results ---
    # Define columns for the final matched CSV output
    csv_columns = [
        'committee_id_scraped', 'committee_name_scraped', 'chamber', 'year',
        'legislator_name_scraped', 'role_scraped', 'legislator_id',
        'matched_api_name', 'match_score'
        # Add 'matched_party', 'matched_district' here if included above
    ]
    # Save the final matched data to the specified CSV path
    convert_to_csv(updated_memberships, output_csv_path, columns=csv_columns)

    # Save the updated memberships list (with matching info) back to JSON
    # Determine output name, replacing '_raw.json' or adding '_matched.json'
    if scraped_memberships_json_path.name.endswith('_raw.json'):
         # e.g., scraped_memberships_raw_ID_2023.json -> scraped_memberships_matched_ID_2023.json
         matched_json_name = scraped_memberships_json_path.name.replace('_raw.json', '_matched.json')
    else:
         # Fallback if input name doesn't match pattern (add suffix before extension)
         matched_json_name = f"{scraped_memberships_json_path.stem}_matched{scraped_memberships_json_path.suffix}"
    # Place matched JSON alongside the raw file it came from
    matched_json_output_path = scraped_memberships_json_path.with_name(matched_json_name)

    # Convert pd.NA to None for JSON compatibility before saving
    json_safe_list = [{k: (None if pd.isna(v) else v) for k, v in record.items()} for record in updated_memberships]
    save_json(json_safe_list, matched_json_output_path)
    logger.info(f"Saved matched membership JSON to {matched_json_output_path}")

    return True


def consolidate_membership_data(years: Iterable[int], state_abbr: str, paths: Dict[str, Path]):
    """Consolidates *matched* scraped committee membership CSVs from multiple years into one CSV and one JSON."""
    logger.info(f"--- Consolidating Matched Scraped Committee Memberships ({state_abbr}) ---")
    all_memberships_dfs = []
    processed_files_found = 0
    # Source directory for yearly matched CSVs
    processed_dir = paths.get('processed')
    # Target directory for consolidated raw JSON (represents combined matched data)
    raw_memberships_dir = paths.get('raw_committee_memberships')

    if not processed_dir:
        logger.error("Processed directory path not found in paths dict. Cannot consolidate memberships.")
        return
    if not raw_memberships_dir:
         logger.warning("Raw committee memberships directory not found. Cannot save consolidated JSON there.")
         # Continue to consolidate CSV only

    # Define expected final columns for the consolidated file (should match match_scraped_legislators output)
    final_cols = [
        'committee_id_scraped', 'committee_name_scraped', 'chamber', 'year',
        'legislator_name_scraped', 'role_scraped', 'legislator_id',
        'matched_api_name', 'match_score'
    ]

    years_list = sorted(list(years)) # Process in order
    min_year = min(years_list) if years_list else 'N/A'
    max_year = max(years_list) if years_list else 'N/A'
    logger.info(f"Scanning for matched files from {min_year} to {max_year}")

    for year in years_list:
        # Construct path to the *matched* CSV file for the year
        # Naming convention from match_scraped_legislators output CSV
        matched_csv_path = processed_dir / f'committee_memberships_scraped_matched_{state_abbr}_{year}.csv'
        if matched_csv_path.exists():
            try:
                logger.info(f"Loading matched memberships from: {matched_csv_path}")
                # Specify dtypes for potentially problematic columns
                # Use Int64 for legislator_id to handle NAs correctly
                # Ensure match_score is float
                # low_memory=False can help with mixed types if warnings occur
                df = pd.read_csv(
                    matched_csv_path,
                    dtype={'legislator_id': 'Int64', 'match_score': 'float'},
                    low_memory=False,
                    encoding='utf-8'
                )

                if not df.empty:
                    # Verify/add expected columns, filling missing with NA
                    for col in final_cols:
                        if col not in df.columns:
                             logger.warning(f"Column '{col}' missing in {matched_csv_path}, adding as NA.")
                             df[col] = pd.NA

                    # Append DataFrame with selected/ordered columns
                    all_memberships_dfs.append(df[final_cols])
                    processed_files_found += 1
                else:
                    logger.info(f"Matched membership file is empty, skipping: {matched_csv_path}")

            except pd.errors.EmptyDataError:
                logger.warning(f"Matched membership file is empty (Pandas EmptyDataError), skipping: {matched_csv_path}")
            except Exception as e:
                logger.error(f"Error loading matched membership CSV {matched_csv_path}: {str(e)}", exc_info=True)
        else:
             logger.debug(f"Matched membership file not found for year {year}: {matched_csv_path}")

    # --- Consolidate and Save ---
    if all_memberships_dfs:
        consolidated_df = pd.concat(all_memberships_dfs, ignore_index=True)
        logger.info(f"Consolidated {len(consolidated_df)} matched committee membership records from {processed_files_found} yearly files ({min_year}-{max_year}).")

        # Final check and reordering of columns (should be redundant but safe)
        for col in final_cols:
            if col not in consolidated_df.columns: consolidated_df[col] = pd.NA
        consolidated_df = consolidated_df[final_cols] # Ensure final order/selection

        # Define output paths for the final consolidated data
        output_csv = processed_dir / f'committee_memberships_scraped_consolidated_{state_abbr}_{min_year}-{max_year}.csv'
        # Save consolidated JSON to the raw memberships directory
        output_json = raw_memberships_dir / f'all_memberships_scraped_consolidated_{state_abbr}_{min_year}-{max_year}.json' if raw_memberships_dir else None

        # Save consolidated CSV
        consolidated_df.to_csv(output_csv, index=False, encoding='utf-8')
        logger.info(f"Saved consolidated matched memberships CSV to {output_csv}")

        # Save consolidated JSON if path available
        if output_json:
            # Convert DataFrame to list of dicts, replacing pd.NA with None for JSON compatibility
            # Use where method for cleaner NA replacement
            json_safe_list = consolidated_df.where(pd.notna(consolidated_df), None).to_dict('records')
            save_json(json_safe_list, output_json)
            logger.info(f"Saved consolidated matched memberships JSON to {output_json}")

    else:
        # No matched files found across the specified year range
        logger.warning(f"No *matched* scraped committee membership files found to consolidate for {state_abbr} in years {min_year}-{max_year}.")
        # Optionally create empty consolidated files
        output_csv = processed_dir / f'committee_memberships_scraped_consolidated_{state_abbr}_{min_year}-{max_year}.csv'
        convert_to_csv([], output_csv, columns=final_cols)
        if raw_memberships_dir:
             output_json = raw_memberships_dir / f'all_memberships_scraped_consolidated_{state_abbr}_{min_year}-{max_year}.json'
             save_json([], output_json)


# --- Stub Functions for Future Data Sources ---
# These remain placeholders as their implementation is outside the current scope.
def collect_campaign_finance(state_abbr: str, years: Iterable[int], paths: Dict[str, Path]):
    """(STUB) Collect campaign finance data via alternative methods if needed."""
    logger.warning(f"--- STUB FUNCTION CALLED: collect_campaign_finance({state_abbr}, {list(years)}) ---")
    logger.warning("This function is NOT IMPLEMENTED within data_collection.py.")
    logger.warning("For Idaho finance data, see 'scrape_finance_idaho.py'.")

def collect_district_demographics(state_abbr: str, paths: Dict[str, Path]):
    """(STUB) Collect demographic data for legislative districts."""
    logger.warning(f"--- STUB FUNCTION CALLED: collect_district_demographics({state_abbr}) ---")
    logger.warning("This function is NOT IMPLEMENTED.")

def collect_election_history(state_abbr: str, years: Iterable[int], paths: Dict[str, Path]):
    """(STUB) Collect historical election results for legislative races."""
    logger.warning(f"--- STUB FUNCTION CALLED: collect_election_history({state_abbr}, {list(years)}) ---")
    logger.warning("This function is NOT IMPLEMENTED.")


# --- Main Execution Block (for standalone testing) ---
if __name__ == "__main__":
    # This block allows testing parts of data_collection.py directly.
    # It sets up basic logging and path handling for the test run.
    parser = argparse.ArgumentParser(description="LegiScan & Scraper Data Collection Module Runner")
    parser.add_argument('--state', type=str.upper, default='ID', help='State abbreviation')
    parser.add_argument('--start-year', type=int, default=datetime.now().year - 1, help='Start year')
    parser.add_argument('--end-year', type=int, default=datetime.now().year, help='End year')
    parser.add_argument('--data-dir', type=str, default=None, help='Override base data directory')
    parser.add_argument('--run', type=str, choices=['sessions', 'legislators', 'committees', 'bills', 'scrape_members', 'match_members', 'consolidate_api', 'consolidate_members'], required=True, help='Specific function group to run')

    args = parser.parse_args()

    # Setup paths and logging for standalone run using utils
    from .utils import setup_project_paths # Local import for standalone run
    paths = setup_project_paths(args.data_dir)
    # Use the central util to set up logging for this module
    test_logger = setup_logging(DATA_COLLECTION_LOG_FILE, paths['log'])

    # --- Validate API Key for API-dependent actions ---
    api_actions = ['sessions', 'legislators', 'committees', 'bills', 'consolidate_api']
    if args.run in api_actions and not LEGISCAN_API_KEY:
         test_logger.critical("LEGISCAN_API_KEY not set. Cannot run API-dependent actions. Exiting.")
         sys.exit(1)

    test_logger.info(f"--- Running data_collection.py standalone for {args.state} ({args.start_year}-{args.end_year}) ---")
    test_logger.info(f"Action requested: {args.run}")

    years = range(args.start_year, args.end_year + 1)
    sessions = []

    # Fetch sessions if needed by the requested action
    # Exclude actions that don't need session list first
    if args.run not in ['scrape_members', 'match_members', 'consolidate_members']:
        sessions = get_session_list(args.state, years, paths)
        if not sessions and args.run != 'sessions': # Allow 'sessions' run to finish if none found
            test_logger.error("No relevant sessions found via API. Halting API-dependent actions.")
            sys.exit(1)
        if args.run == 'sessions':
             test_logger.info(f"Fetched {len(sessions)} sessions.")
             # Exit after fetching sessions if that was the only action requested
             logging.shutdown()
             sys.exit(0)

    # Execute specific actions based on --run argument
    try:
        if args.run == 'legislators':
            collect_legislators(args.state, sessions, paths)

        elif args.run == 'committees':
             for session in sessions:
                 collect_committee_definitions(session, paths)

        elif args.run == 'bills':
            for session in sessions:
                collect_bills_votes_sponsors(session, paths)

        elif args.run == 'consolidate_api':
            # Define columns for each consolidated file (ensure these match processing logic)
            committee_cols = ['committee_id', 'name', 'chamber', 'chamber_id', 'session_id', 'year']
            bill_cols = ['bill_id', 'change_hash', 'session_id', 'year', 'state', 'state_id', 'url', 'state_link', 'number', 'type', 'type_id', 'body', 'body_id', 'current_body', 'current_body_id', 'title', 'description', 'status', 'status_desc', 'status_date', 'pending_committee_id', 'subjects', 'subject_ids', 'sast_relations', 'text_stubs', 'amendment_stubs', 'supplement_stubs']
            sponsor_cols = ['bill_id', 'legislator_id', 'sponsor_type_id', 'sponsor_type', 'sponsor_order', 'committee_sponsor', 'committee_id', 'session_id', 'year']
            vote_cols = ['vote_id', 'bill_id', 'legislator_id', 'vote_id_type', 'vote_text', 'vote_value', 'date', 'description', 'yea', 'nay', 'nv', 'absent', 'total', 'passed', 'chamber', 'chamber_id', 'session_id', 'year']
            legislator_cols = ['legislator_id', 'person_hash', 'name', 'first_name', 'middle_name', 'last_name','suffix', 'nickname', 'party_id', 'party', 'role_id', 'role','district', 'state_id', 'state', 'active', 'committee_sponsor', 'committee_id','ftm_eid', 'votesmart_id', 'opensecrets_id', 'knowwho_pid', 'ballotpedia','state_link', 'legiscan_url']

            test_logger.info("Consolidating yearly API data...")
            # Consolidate legislators first if not done yet - assumes collect_legislators ran before or exists
            # Might be better to consolidate within collect_legislators? For now, separate.
            # consolidate_yearly_data('legislators', years, legislator_cols, args.state, paths)
            consolidate_yearly_data('committees', years, committee_cols, args.state, paths)
            consolidate_yearly_data('bills', years, bill_cols, args.state, paths)
            consolidate_yearly_data('sponsors', years, sponsor_cols, args.state, paths)
            consolidate_yearly_data('votes', years, vote_cols, args.state, paths)

        elif args.run == 'scrape_members':
            # Note: This scrapes only the *current* year, regardless of args.start/end_year
            scraped_file = scrape_committee_memberships(args.state, paths)
            if scraped_file:
                 test_logger.info(f"Scraped memberships raw file created: {scraped_file}")
            else:
                 test_logger.error("Membership scraping failed or produced no output.")
                 sys.exit(1) # Exit with error if scraping failed

        elif args.run == 'match_members':
             # Assumes scraping ran previously or file exists for the *current* year
             current_year = datetime.now().year
             scraped_file_path = paths['raw_committee_memberships'] / str(current_year) / f'scraped_memberships_raw_{args.state}_{current_year}.json'
             # Assumes legislators were collected and consolidated
             leg_file_path = paths['raw_legislators'] / f'all_legislators_{args.state}.json'
             # Output path for the matched CSV
             output_csv_path = paths['processed'] / f'committee_memberships_scraped_matched_{args.state}_{current_year}.csv'

             if not leg_file_path.exists():
                  test_logger.error(f"Legislator file needed for matching not found: {leg_file_path}. Run legislator collection first.")
                  sys.exit(1)
             if not scraped_file_path.exists():
                  test_logger.error(f"Raw scraped membership file not found: {scraped_file_path}. Run scraping first.")
                  sys.exit(1)

             success = match_scraped_legislators(scraped_file_path, leg_file_path, output_csv_path, paths)
             if not success:
                 test_logger.error("Membership matching failed.")
                 sys.exit(1)

        elif args.run == 'consolidate_members':
             # Consolidates matched membership data across the specified year range
             test_logger.info("Consolidating matched scraped membership data...")
             consolidate_membership_data(years, args.state, paths)

    except APIRateLimitError:
         test_logger.critical("Halting run due to LegiScan API Rate Limit Error.")
         sys.exit(2) # Exit with a specific code for rate limit
    except ScrapingStructureError as e:
        test_logger.critical(f"Halting run due to Scraping Structure Error: {e}")
        sys.exit(3) # Exit with specific code for scraping structure failure
    except Exception as e:
         test_logger.critical(f"An unexpected error occurred during execution: {e}", exc_info=True)
         sys.exit(1) # General error exit code


    test_logger.info(f"--- Standalone run for action '{args.run}' finished. ---")
    logging.shutdown() # Ensure logs are flushed before exit
```

---

**6. `src/scrape_finance_idaho.py` (Completed)**

This includes the implemented (but needs testing/verification) `search_for_finance_data_link` function.

```python
#!/usr/bin/env python3
"""Scrape campaign finance data from Idaho SOS Sunshine Portal."""
import argparse
import csv
import json
import re
import time
import random
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union, Any
import io # For reading content directly

import requests # Need requests here
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm
from urllib.parse import urljoin, urlparse, parse_qs

# --- Project Imports ---
from .config import (
    ID_FINANCE_BASE_URL, ID_FINANCE_SEARCH_PATH, ID_FINANCE_DOWNLOAD_WAIT_SECONDS,
    FINANCE_SCRAPE_LOG_FILE
)
from .utils import setup_logging, save_json, convert_to_csv, fetch_page, setup_project_paths # fetch_page NOT used directly for POST logic below

# --- Configure Logging ---
# Logger setup expected from calling script (e.g., main.py)
logger = logging.getLogger(Path(FINANCE_SCRAPE_LOG_FILE).stem)

# --- Constants ---
# Column maps for standardization
CONTRIBUTION_COLUMN_MAP = {
    'donor_name': ['donor name', 'contributor name', 'name', 'from', 'contributor'],
    'contribution_date': ['date', 'contribution date', 'received date'],
    'contribution_amount': ['amount', 'contribution amount', '$', 'receipt amount'],
    'donor_address': ['address', 'donor address', 'contributor address', 'addr 1'],
    'donor_city': ['city', 'donor city', 'contributor city'],
    'donor_state': ['state', 'st', 'donor state', 'contributor state'],
    'donor_zip': ['zip', 'zip code', 'donor zip', 'contributor zip'],
    'donor_employer': ['employer', 'donor employer', 'contributor employer'],
    'donor_occupation': ['occupation', 'donor occupation', 'contributor occupation'],
    'contribution_type': ['type', 'contribution type', 'receipt type'],
    'committee_name': ['committee name', 'recipient committee', 'filer name'], # Often the committee being searched for
    'report_name': ['report name', 'report title', 'report'],
    'transaction_id': ['transaction id', 'tran id', 'transactionid']
}
EXPENDITURE_COLUMN_MAP = {
    'expenditure_date': ['date', 'expenditure date', 'payment date'],
    'payee_name': ['payee', 'paid to', 'name', 'payee name', 'vendor name'],
    'expenditure_amount': ['amount', 'expenditure amount', '$', 'payment amount'],
    'expenditure_purpose': ['purpose', 'description', 'expenditure purpose', 'memo'],
    'payee_address': ['address', 'payee address', 'vendor address', 'addr 1'],
    'payee_city': ['city', 'payee city', 'vendor city'],
    'payee_state': ['state', 'st', 'payee state', 'vendor state'],
    'payee_zip': ['zip', 'zip code', 'payee zip', 'vendor zip'],
    'expenditure_type': ['type', 'expenditure type', 'payment type', 'expenditure code'],
    'committee_name': ['committee name', 'paying committee', 'filer name'], # Often the committee being searched for
    'report_name': ['report name', 'report title', 'report'],
    'transaction_id': ['transaction id', 'tran id', 'transactionid']
}

# --- Helper Functions ---
def standardize_columns(df: pd.DataFrame, column_map: Dict[str, List[str]]) -> pd.DataFrame:
    """Standardizes DataFrame columns based on a mapping."""
    df.columns = df.columns.str.lower().str.strip().str.replace(':', '') # Clean column names
    rename_dict = {}
    found_standard_names = set()

    for standard_name, variations in column_map.items():
        for var in variations:
            if var in df.columns:
                if standard_name not in found_standard_names:
                    rename_dict[var] = standard_name
                    found_standard_names.add(standard_name)
                    logger.debug(f"Mapping column '{var}' to '{standard_name}'")
                    break # Use first matching variation
            # else: logger.debug(f"Variation '{var}' not found in columns: {df.columns.tolist()}")

    original_cols = set(df.columns)
    mapped_source_cols = set(rename_dict.keys())
    unmapped_cols = original_cols - mapped_source_cols
    if unmapped_cols:
         logger.warning(f"Unmapped columns found: {list(unmapped_cols)}. Check column_map or source data.")

    df = df.rename(columns=rename_dict)
    for standard_name in column_map.keys():
        if standard_name not in df.columns:
            df[standard_name] = pd.NA
            logger.debug(f"Added missing standard column: '{standard_name}'")

    final_columns = list(column_map.keys())
    # Return only the standard columns that exist in the dataframe after mapping/adding
    return df[[col for col in final_columns if col in df.columns]]

# --- Website Interaction & Parsing Functions ---

def get_hidden_form_fields(soup: BeautifulSoup) -> Dict[str, str]:
    """Extracts hidden input fields commonly used in ASP.NET forms."""
    fields = {}
    # Be more specific to find the main form if multiple exist
    main_form = soup.find('form', id=re.compile("form", re.I)) # Common ID for main form
    if not main_form:
         main_form = soup.find('form') # Fallback to first form
         if not main_form:
              logger.warning("Could not find <form> tag on page.")
              return {}

    hidden_inputs = main_form.find_all('input', {'type': 'hidden'})
    for input_tag in hidden_inputs:
        name = input_tag.get('name')
        value = input_tag.get('value', '') # Default to empty string
        if name:
            # Prioritize known ASP.NET state fields
            if name in ['__VIEWSTATE', '__VIEWSTATEGENERATOR', '__EVENTVALIDATION']:
                 fields[name] = value
            # Consider other hidden fields if needed, but be cautious
            # elif name.startswith('ctl00$'): fields[name] = value
            else:
                 logger.debug(f"Ignoring potentially irrelevant hidden field: {name}")

    logger.debug(f"Extracted hidden fields: {list(fields.keys())}")
    if '__VIEWSTATE' not in fields: logger.warning("ViewState hidden field ('__VIEWSTATE') not found, form submission might fail.")
    return fields

def find_export_link(soup: BeautifulSoup, data_type: str) -> Optional[str]:
    """Finds the CSV/Excel export link/button in the search results HTML."""
    # --- THIS REQUIRES INSPECTION OF THE ACTUAL SEARCH RESULTS PAGE ---
    # Common patterns: Link (<a>), Button (<input>/<button>), JavaScript
    logger.debug(f"Searching results page for '{data_type}' export link/button...")
    export_link = None

    # Pattern 1: Direct Link (Prioritize CSV)
    # Look for specific IDs or attributes if possible, otherwise use regex on text/title/href
    # Example Selectors (ADJUST THESE BASED ON INSPECTION):
    possible_links = soup.select('a[id*="Export"], a[id*="Download"], a[title*="Export"], a[title*="Download"]')
    if not possible_links:
         possible_links = soup.find_all('a', string=re.compile(r'\b(Export|Download|CSV|Excel)\b', re.I))

    csv_link = None
    excel_link = None
    for link in possible_links:
        href = link.get('href')
        if not href: continue
        link_text = link.get_text(strip=True).lower()
        href_lower = href.lower()

        # Check for CSV indicators
        if 'csv' in link_text or 'format=csv' in href_lower or '.csv' in href_lower:
             csv_link = href
             logger.info(f"Found potential CSV export link: {href}")
             break # Found preferred format, stop searching links
        # Check for Excel indicators (only if CSV not found yet)
        elif not csv_link and ('excel' in link_text or 'xls' in link_text or 'format=xls' in href_lower or '.xls' in href_lower):
             excel_link = href
             logger.info(f"Found potential Excel export link: {href}")
             # Continue searching in case a CSV link appears later

    export_link = csv_link or excel_link # Prioritize CSV

    # Pattern 2: Submit Button (More Complex - Requires knowing button's name/value)
    if not export_link:
        # Example Selectors (ADJUST THESE BASED ON INSPECTION):
        possible_buttons = soup.select('input[type="submit"][value*="Export"], input[type="submit"][value*="Download"], button[id*="Export"], button[id*="Download"]')
        if not possible_buttons:
             possible_buttons = soup.find_all(['input', 'button'], string=re.compile(r'\b(Export|Download|CSV|Excel)\b', re.I))

        for button in possible_buttons:
             button_name = button.get('name')
             button_value = button.get('value', button.get_text(strip=True))
             # This indicates the download might require another POST, including this button's name/value.
             # Handling this requires modifying the POST logic significantly.
             logger.warning(f"Found potential export BUTTON: Name='{button_name}', Value/Text='{button_value}'.")
             logger.warning("Download via button click is not implemented. Requires specific handling.")
             # Cannot return a simple link here. Set export_link to None or a special value?
             export_link = None # Indicate link not found if only button exists

    # Pattern 3: JavaScript (Generally requires browser automation - Selenium/Playwright)
    if not export_link:
         js_triggers = soup.find_all(onclick=re.compile(r'export|download|csv|excel', re.I))
         if js_triggers:
              logger.warning(f"Found potential JavaScript export triggers: {[tag.get('onclick') for tag in js_triggers[:3]]}. Cannot handle with requests.")
              export_link = None

    # Final processing of found link
    if export_link:
        # Ensure the link is absolute, joining with the base URL if relative
        absolute_link = urljoin(ID_FINANCE_BASE_URL, export_link)
        logger.info(f"Resolved export URL: {absolute_link}")
        return absolute_link
    else:
        logger.warning(f"Could not find unambiguous download link/button for '{data_type}' on results page.")
        return None


def search_for_finance_data_link(search_term: str, year: int, data_type: str) -> Optional[str]:
    """
    Searches the Idaho Sunshine Portal for a finance data download link.
    Handles ASP.NET form submission with ViewState.

    Args:
        search_term: Name of legislator or committee.
        year: Election year or reporting year.
        data_type: 'contributions' or 'expenditures'.

    Returns:
        Absolute URL string for the download link, or None if not found or error.
    """
    search_page_url = urljoin(ID_FINANCE_BASE_URL, ID_FINANCE_SEARCH_PATH)
    logger.info(f"Initiating finance search: Term='{search_term}', Year={year}, Type={data_type}")

    # Use a session object to persist cookies across requests
    session = requests.Session()
    session.headers.update({ # Base headers, fetch_page might add more specific ones
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Referer': ID_FINANCE_BASE_URL, # Often important
    })

    try:
        # --- Step 1: Initial GET to load the search page ---
        logger.debug(f"Fetching initial search page to get form state: {search_page_url}")
        # Use requests directly for session handling
        initial_response = session.get(search_page_url, timeout=40, allow_redirects=True)
        initial_response.raise_for_status() # Check for immediate errors loading page
        logger.debug(f"Initial GET successful (Status: {initial_response.status_code})")

        initial_soup = BeautifulSoup(initial_response.text, 'html.parser')
        hidden_fields = get_hidden_form_fields(initial_soup)
        if not hidden_fields or '__VIEWSTATE' not in hidden_fields:
            logger.error("Could not extract necessary hidden form fields (__VIEWSTATE) from search page. Cannot submit search.")
            return None

        # --- Step 2: Construct the POST data payload ---
        # Field names below are GUESSES based on common ASP.NET patterns and need VERIFICATION
        # Inspect the <form> element and its <input>, <select> tags in browser dev tools.
        form_data = hidden_fields.copy() # Start with ViewState etc.

        # --- VERIFY THESE FIELD NAMES ---
        # Candidate/Committee Name Input: Look for <input type="text" name="....txtName...">
        form_data['ctl00$DefaultContent$CampaignSearch$txtName'] = search_term
        # Year Input: Look for <input type="text" name="....txtYear..."> or <select name="....ddlYear...">
        form_data['ctl00$DefaultContent$CampaignSearch$txtYear'] = str(year)
        # Search Type (Candidate/Committee): Look for <input type="radio" name="....SearchType..."> or dropdown
        # form_data['ctl00$DefaultContent$CampaignSearch$SearchType'] = 'Candidate' # Example, adjust if needed
        # Search Button: Look for <input type="submit" name="....btnSearch..." value="Search">
        form_data['ctl00$DefaultContent$CampaignSearch$btnSearch'] = 'Search' # Use the 'value' attribute

        # Data Type Selection (Contributions/Expenditures): This might be selected *after* the initial search results.
        # If so, this function needs to find the link/button for the *specific data type* on the results page.
        # For now, assume the initial search leads to a page containing links for both.

        logger.debug(f"Constructed POST data (ViewState and key fields): {{'__VIEWSTATE': '...', 'txtName': '{search_term}', 'txtYear': '{year}', 'btnSearch': 'Search'}}")

        # --- Step 3: Make the POST request ---
        logger.info(f"Submitting search POST request to {search_page_url}...")
        post_response = session.post(
            search_page_url,
            data=form_data,
            timeout=60, # Longer timeout for search submission
            allow_redirects=True,
             headers={'Referer': search_page_url} # Explicitly set Referer for POST
        )
        post_response.raise_for_status() # Check for errors during POST
        logger.debug(f"Search POST request successful (Status: {post_response.status_code})")

        # Check final URL after potential redirects
        final_url = post_response.url
        logger.debug(f"Final URL after POST: {final_url}")
        # Optional: Check if URL indicates an error page?

        # --- Step 4: Parse the response HTML for the download link ---
        results_soup = BeautifulSoup(post_response.text, 'html.parser')

        # Check for explicit error messages
        # Look for common ASP.NET validation summary controls or specific error divs
        error_div = results_soup.find('div', id=re.compile("ValidationSummary|error", re.I))
        if error_div:
            error_text = error_div.get_text(" ", strip=True)
            if "no records found" in error_text.lower():
                 logger.info(f"Search successful but returned 'No Records Found' for '{search_term}', {year}.")
                 return None # Valid search, but no data
            else:
                 logger.warning(f"Found error/validation message on results page: {error_text[:200]}...")
                 # Continue trying to find link, might be unrelated error

        # Attempt to find the specific export link (CSV preferred)
        export_url = find_export_link(results_soup, data_type)

        if export_url:
             logger.info(f"Successfully found export URL for {data_type}: {export_url}")
             return export_url
        else:
             # Save the search results HTML for debugging if link not found
             debug_path = paths['artifacts'] / 'debug' # Use 'artifacts' dir
             debug_path.mkdir(parents=True, exist_ok=True)
             safe_term = "".join(c if c.isalnum() else '_' for c in search_term)[:50]
             debug_file = debug_path / f"search_results_{safe_term}_{year}_{data_type}_{time.strftime('%Y%m%d%H%M%S')}.html"
             try:
                  debug_file.write_text(post_response.text, encoding='utf-8', errors='replace')
                  logger.warning(f"Saved search results HTML (link not found) for debugging to: {debug_file}")
             except Exception as e_save:
                  logger.error(f"Failed to save debug HTML: {e_save}")
             return None

    except requests.exceptions.RequestException as e:
        logger.error(f"Network error during finance search for '{search_term}' ({year}, {data_type}): {e}", exc_info=False) # Less verbose logging for network errors
        return None
    except Exception as e:
        logger.error(f"Unexpected error during finance search for '{search_term}' ({year}, {data_type}): {e}", exc_info=True)
        return None


# --- Data Downloading and Processing ---
def download_and_extract_finance_data(
    download_url: str,
    source_search_term: str, # e.g., legislator name used in search
    search_year: int,
    data_type: str, # 'contributions' or 'expenditures'
    paths: Dict[str, Path]
) -> Optional[pd.DataFrame]:
    """Downloads, processes, and standardizes finance data (CSV assumed)."""
    logger.info(f"Attempting download: Type='{data_type}', Term='{source_search_term}', Year={search_year}")
    logger.debug(f"Download URL: {download_url}")

    # Use utils.fetch_page to get the content (returns string)
    # If Excel is possible, might need fetch_page(..., return_bytes=True) and Excel reading logic
    file_content = fetch_page(download_url, timeout=90) # Longer timeout for downloads

    if file_content is None: # Check if fetch_page failed
        logger.error(f"Failed to download content from {download_url} after retries.")
        return None
    if len(file_content) < 50: # Check for suspiciously small files
        logger.warning(f"Downloaded file from {download_url} is very small ({len(file_content)} chars). May be empty or an error page.")
        # Check if it looks like HTML (potential error page)
        if file_content.strip().lower().startswith(('<!doctype html', '<html')):
             logger.error(f"Downloaded content appears to be HTML, not data. URL: {download_url}")
             # Save HTML error page for debugging?
             error_html_path = paths['artifacts'] / 'debug' / f"download_error_{datetime.now().strftime('%Y%m%d%H%M%S')}.html"
             error_html_path.parent.mkdir(parents=True, exist_ok=True)
             try: error_html_path.write_text(file_content, encoding='utf-8', errors='replace')
             except Exception as e_save: logger.error(f"Failed to save error HTML: {e_save}")
             return None

    # Define raw file path within state/year structure
    raw_dir = paths['raw_campaign_finance'] / 'idaho' / str(search_year)
    raw_dir.mkdir(parents=True, exist_ok=True)
    safe_search_term = "".join(c if c.isalnum() else '_' for c in source_search_term)[:50]
    # Include timestamp in raw filename to prevent overwrites
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    # Assume CSV extension for now, could check Content-Type header if available
    raw_path = raw_dir / f"{data_type}_{safe_search_term}_{search_year}_{timestamp}.raw.csv"

    # Save raw content before processing
    try:
        raw_path.write_text(file_content, encoding='utf-8', errors='replace')
        logger.info(f"Saved raw downloaded content to {raw_path}")
    except Exception as e:
        logger.error(f"Error saving raw content to {raw_path}: {e}")
        # Don't necessarily stop, try processing anyway

    # Attempt to process the data as CSV using pandas
    df = None
    try:
        # Use io.StringIO to read the string content as a file object
        csv_file = io.StringIO(file_content)
        # Try reading CSV, be robust to common issues
        df = pd.read_csv(
            csv_file,
            encoding='utf-8',
            low_memory=False, # Helps with mixed types
            on_bad_lines='warn', # Log problematic lines instead of failing
            # quoting=csv.QUOTE_MINIMAL, # Might help if quoting is inconsistent
            # escapechar='\\' # If escape characters are used
        )
        logger.debug(f"Successfully read CSV data using utf-8 for {raw_path.name}")
    except (pd.errors.ParserError, UnicodeDecodeError, Exception) as e_utf8:
        logger.warning(f"UTF-8 CSV parsing failed for {raw_path.name}: {e_utf8}. Trying latin-1...")
        try:
            csv_file = io.StringIO(file_content) # Reset buffer position might be needed
            df = pd.read_csv(csv_file, encoding='latin-1', low_memory=False, on_bad_lines='warn')
            logger.debug(f"Successfully read CSV data using latin-1 for {raw_path.name}")
        except (pd.errors.ParserError, Exception) as e_latin1:
             logger.error(f"All CSV parsing attempts failed for {raw_path.name}: {e_latin1}")
             # --- Placeholder for Excel Reading ---
             # Consider implementing this if CSV fails often and Excel is an option
             # logger.info("Excel reading not implemented yet.")
             # --- End Placeholder ---
             return None # Give up if CSV fails and Excel not implemented/failed

    if df is None or df.empty: # Check if DataFrame is empty after reading
        logger.warning(f"Downloaded file {raw_path.name} resulted in an empty DataFrame.")
        return None # Treat empty file as failure for this record

    # Standardize columns based on data type
    logger.debug(f"Standardizing {len(df)} rows for {data_type}...")
    if data_type == 'contributions':
        df_standardized = standardize_columns(df.copy(), CONTRIBUTION_COLUMN_MAP)
    elif data_type == 'expenditures':
        df_standardized = standardize_columns(df.copy(), EXPENDITURE_COLUMN_MAP)
    else:
        logger.error(f"Invalid data_type '{data_type}' for column standardization.")
        return None

    # Add metadata columns
    df_standardized['source_search_term'] = source_search_term
    df_standardized['data_source_url'] = download_url
    df_standardized['scrape_year'] = search_year
    df_standardized['raw_file_path'] = str(raw_path)
    df_standardized['scrape_timestamp'] = datetime.now().isoformat()
    df_standardized['data_type'] = data_type # Clarify if contribution or expenditure

    # --- Data Cleaning Steps ---
    # Example: Convert amount columns to numeric, handling errors
    amount_col = 'contribution_amount' if data_type == 'contributions' else 'expenditure_amount'
    if amount_col in df_standardized.columns:
        # Remove currency symbols, commas, handle potential parentheses for negatives
        df_standardized[amount_col] = df_standardized[amount_col].astype(str).str.replace(r'[$,()]', '', regex=True)
        df_standardized[amount_col] = pd.to_numeric(df_standardized[amount_col], errors='coerce')
        # Fill NaNs introduced by coercion if needed (e.g., with 0)
        # df_standardized[amount_col] = df_standardized[amount_col].fillna(0)

    # Example: Convert date columns to datetime objects
    date_col = 'contribution_date' if data_type == 'contributions' else 'expenditure_date'
    if date_col in df_standardized.columns:
        df_standardized[date_col] = pd.to_datetime(df_standardized[date_col], errors='coerce')
        # Format date for consistency if needed
        # df_standardized[date_col] = df_standardized[date_col].dt.strftime('%Y-%m-%d')

    # Add other cleaning steps as needed (e.g., standardizing state names, zip codes)

    logger.info(f"Successfully processed and cleaned {len(df_standardized)} {data_type} records for '{source_search_term}' ({search_year}).")
    return df_standardized


# --- Main Orchestration Function ---
def main(start_year: Optional[int] = None, end_year: Optional[int] = None, data_dir: Optional[Union[str, Path]] = None) -> Optional[Path]:
    """Main function to orchestrate scraping Idaho campaign finance data."""
    # Setup paths using the provided base directory or default from config
    paths = setup_project_paths(data_dir)
    # Ensure logger is set up correctly for this module
    global logger
    logger = setup_logging(FINANCE_SCRAPE_LOG_FILE, paths['log'])

    # Determine year range
    current_year = datetime.now().year
    if start_year is None: start_year = current_year - 2 # Default to last 2 years + current
    if end_year is None: end_year = current_year
    if start_year > end_year:
        logger.error("Start year cannot be after end year.")
        return None

    logger.info(f"=== Starting Idaho Campaign Finance Scraping ===")
    logger.info(f"Data Source: Idaho Sunshine Portal ({ID_FINANCE_BASE_URL})")
    logger.info(f"Target Years: {start_year}-{end_year}")
    logger.info(f"Base Data Directory: {paths['base']}")
    logger.warning("Finance scraping relies on website structure and form fields - VERIFY field names in search_for_finance_data_link.")

    # --- Load Legislator Names for Searching ---
    legislators_file = paths['processed'] / 'legislators_ID.csv'
    if not legislators_file.is_file():
        logger.error(f"Processed legislators file not found: {legislators_file}")
        logger.error("Run the main data collection script (including LegiScan legislator collection) first.")
        return None

    try:
        legislators_df = pd.read_csv(legislators_file, usecols=['name'])
        search_targets = legislators_df['name'].dropna().unique().tolist()
        logger.info(f"Loaded {len(search_targets)} unique legislator names to search for.")
        if not search_targets:
             logger.error("No legislator names loaded. Cannot proceed.")
             return None
    except KeyError:
        logger.error(f"Column 'name' not found in legislators file: {legislators_file}.")
        return None
    except Exception as e:
        logger.error(f"Error reading legislators file {legislators_file}: {e}", exc_info=True)
        return None

    # --- Iterate and Scrape ---
    all_finance_data_dfs = [] # List to hold successfully processed DataFrames
    search_attempts = 0
    download_successes = 0
    download_failures = 0

    # Loop through years, then legislators
    for year in range(start_year, end_year + 1):
        logger.info(f"--- Processing Year: {year} ---")
        for target_name in tqdm(search_targets, desc=f"Searching Finance Data ({year})", unit="target"):
            # Search for both contributions and expenditures for each target
            for data_type in ['contributions', 'expenditures']:
                search_attempts += 1
                logger.debug(f"Attempting search for {data_type}, {target_name}, {year}")
                try:
                    # Call the implemented search function
                    download_link = search_for_finance_data_link(target_name, year, data_type)

                    if download_link:
                        # Add a polite wait time before hitting the download link
                        time.sleep(random.uniform(0.5, ID_FINANCE_DOWNLOAD_WAIT_SECONDS))

                        # Download and process the data file
                        df_processed = download_and_extract_finance_data(
                            download_link, target_name, year, data_type, paths
                        )

                        if df_processed is not None and not df_processed.empty:
                            all_finance_data_dfs.append(df_processed)
                            download_successes += 1
                        else:
                             logger.warning(f"No {data_type} data extracted for '{target_name}' ({year}) from link: {download_link}")
                             download_failures += 1
                    else:
                         logger.info(f"No download link found for {data_type}, '{target_name}', {year}.")
                         # No download failure here, just no link found

                except Exception as e_scrape:
                     logger.error(f"Unhandled error during scrape attempt for {data_type}, '{target_name}', {year}: {e_scrape}", exc_info=True)
                     download_failures += 1 # Count errors as failures

                # Small delay between contribution/expenditure searches for the same person/year
                time.sleep(random.uniform(0.3, 0.9))

            # Wait a bit longer between different legislators/committees
            time.sleep(random.uniform(0.6, 1.8))


    # --- Consolidate and Save Results ---
    logger.info(f"--- Idaho Finance Scraping Finished ({start_year}-{end_year}) ---")
    logger.info(f"Total search attempts: {search_attempts}")
    logger.info(f"Successful downloads/extractions: {download_successes}")
    logger.info(f"Failed/empty downloads or errors: {download_failures}")

    if not all_finance_data_dfs:
        logger.warning("No campaign finance data was successfully collected.")
        return None # Return None if nothing was collected

    try:
        # Concatenate all collected DataFrames
        logger.info(f"Consolidating {len(all_finance_data_dfs)} collected dataframes...")
        # Handle potential schema differences during concat if needed (e.g., using join='outer')
        consolidated_df = pd.concat(all_finance_data_dfs, ignore_index=True, sort=False)
        total_records = len(consolidated_df)
        logger.info(f"Consolidated a total of {total_records} finance records.")

        if total_records == 0:
             logger.warning("Consolidation resulted in an empty DataFrame.")
             return None

        # Define output file path in the 'processed' directory
        # Include year range in filename for clarity
        output_file = paths['processed'] / f'finance_idaho_consolidated_{start_year}-{end_year}.csv'

        # Save the consolidated data using utils.convert_to_csv
        num_saved = convert_to_csv(consolidated_df.to_dict('records'), output_file)

        if num_saved == total_records:
            logger.info(f"Successfully saved {num_saved} consolidated finance records to: {output_file}")
            return output_file # Return path to the saved file
        else:
            logger.error(f"Mismatch saving consolidated data. Expected {total_records}, saved {num_saved}. Check CSV file and logs.")
            # File might exist but be incomplete or empty if convert_to_csv had issues.
            return output_file if output_file.exists() else None

    except Exception as e_concat:
        logger.error(f"Error consolidating or saving finance data: {e_concat}", exc_info=True)
        return None

# --- Main Execution Block (for standalone testing) ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scrape campaign finance data from Idaho Secretary of State website (Sunshine Portal)."
    )
    parser.add_argument('--start-year', type=int, default=None, # Default handled in main
                        help='Start year for data collection (default: current year - 2)')
    parser.add_argument('--end-year', type=int, default=None, # Default handled in main
                        help='End year for data collection (default: current year)')
    parser.add_argument('--data-dir', type=str, default=None,
                        help='Override base data directory (default: ./data)')

    args = parser.parse_args()

    # Setup basic logging/paths for standalone run
    # Note: Logger setup might happen twice if called by main.py, but utils.setup_logging handles it.
    paths = setup_project_paths(args.data_dir)
    logger = setup_logging(FINANCE_SCRAPE_LOG_FILE, paths['log'])

    try:
        output_path = main(start_year=args.start_year, end_year=args.end_year, data_dir=args.data_dir)
        if output_path:
            print(f"\nFinance scraping finished. Output file: {output_path}")
            sys.exit(0) # Success
        else:
            print("\nFinance scraping finished but produced no output or failed.")
            sys.exit(1) # Failure
    except Exception as e:
        logger.critical(f"Critical error during finance scraping main execution: {e}", exc_info=True)
        sys.exit(2) # Critical failure
    finally:
        logging.shutdown() # Ensure logs flushed
```
