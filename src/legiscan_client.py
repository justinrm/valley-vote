# Standard library imports
import json
import time
import random
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Iterable

# Third-party imports
import requests
import pandas as pd # For pd.NA in collect_legislators
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log
)
from tqdm import tqdm

# Local imports
from .config import (
    LEGISCAN_API_KEY,
    LEGISCAN_BASE_URL,
    LEGISCAN_MAX_RETRIES,
    LEGISCAN_DEFAULT_WAIT_SECONDS,
    SPONSOR_TYPES # Needed for collect_legislators
)
from .utils import (
    save_json,
    convert_to_csv,
    load_json,
    # clean_name, # Not used in these functions
    # map_vote_value, # Not used in these functions
    # fetch_page, # Not used here
    # setup_logging, # Logger setup happens in main scripts
    # ensure_dir, # Not directly used here, used by callers
)

logger = logging.getLogger(__name__)

# --- Custom Exceptions ---
class APIRateLimitError(Exception):
    """Custom exception for API rate limiting (HTTP 429)."""
    pass

class APIResourceNotFoundError(Exception):
    """Custom exception for resources not found (404 or specific API message)."""
    pass

# --- API Fetching Logic ---
@retry(
    stop=stop_after_attempt(LEGISCAN_MAX_RETRIES),
    wait=wait_exponential(multiplier=1.5, min=2, max=60), # Standard backoff
    retry=retry_if_exception_type((requests.exceptions.RequestException, APIRateLimitError)),
    before_sleep=before_sleep_log(logging.getLogger(), logging.WARNING)
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
    if not LEGISCAN_API_KEY:
        logger.error("Cannot fetch API data: LEGISCAN_API_KEY is not set.")
        return None

    request_params = params.copy()
    request_params['key'] = LEGISCAN_API_KEY
    request_params['op'] = operation
    request_id_log = request_params.get('id', 'N/A')

    base_wait = wait_time if wait_time is not None else LEGISCAN_DEFAULT_WAIT_SECONDS
    sleep_duration = max(0.1, base_wait + random.uniform(-0.2, 0.4))
    logger.debug(f"Sleeping for {sleep_duration:.2f}s before LegiScan API request (op: {operation}, id: {request_id_log})")
    time.sleep(sleep_duration)

    try:
        logger.info(f"Fetching LegiScan API: op={operation}, id={request_id_log}")
        log_params = {k: v for k, v in request_params.items() if k != 'key'}
        logger.debug(f"Request params: {log_params}")

        response = requests.get(LEGISCAN_BASE_URL, params=request_params, timeout=45, headers={'Accept': 'application/json'})

        if response.status_code == 429:
            logger.warning(f"LegiScan Rate limit hit (HTTP 429) for op={operation}, id={request_id_log}. Backing off...")
            raise APIRateLimitError("Rate limit exceeded")

        response.raise_for_status()

        try:
            data = response.json()
        except json.JSONDecodeError:
            response_text_preview = response.text[:200] if response and hasattr(response, 'text') else "N/A"
            logger.error(f"Invalid JSON response from LegiScan op={operation} (id: {request_id_log}). Status: {response.status_code}. Preview: {response_text_preview}...")
            return None

        status = data.get('status')
        if status == 'ERROR':
            error_msg = data.get('alert', {}).get('message', 'Unknown LegiScan API error')
            logger.error(f"LegiScan API error response for op={operation} (id: {request_id_log}): {error_msg}")
            if "not found" in error_msg.lower() or \
               "invalid id" in error_msg.lower() or \
               "does not exist" in error_msg.lower() or \
               "no data" in error_msg.lower():
                logger.warning(f"LegiScan resource likely not found for op={operation}, id={request_id_log}.")
                raise APIResourceNotFoundError(f"Resource not found for {operation} id {request_id_log}: {error_msg}")
            return None
        elif status != 'OK':
             error_msg = data.get('alert', {}).get('message', f'Unexpected LegiScan API status: {status}')
             logger.error(f"Unexpected LegiScan API status for op={operation} (id: {request_id_log}): {error_msg}. Full response status: {status}")
             return None

        logger.debug(f"Successfully fetched LegiScan op={operation}, id={request_id_log}")
        return data

    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code if e.response is not None else 'N/A'
        if status_code == 404:
            logger.warning(f"LegiScan HTTP 404 Not Found for op={operation}, id={request_id_log}. Assuming resource does not exist.")
            raise APIResourceNotFoundError(f"HTTP 404 Not Found for {operation} id {request_id_log}") from e
        elif 400 <= status_code < 500:
             logger.error(f"LegiScan Client error {status_code} fetching op={operation} (id: {request_id_log}): {e}. Check parameters.")
             return None
        elif status_code >= 500:
            logger.error(f"LegiScan Server error {status_code} fetching op={operation} (id: {request_id_log}): {e}. Might retry.")
            raise requests.exceptions.RequestException(f"Server error {status_code}") from e
        else:
             logger.error(f"Unhandled LegiScan HTTP error {status_code} fetching op={operation} (id: {request_id_log}): {e}", exc_info=True)
             raise requests.exceptions.RequestException(f"Unhandled HTTP error {status_code}") from e

    except requests.exceptions.RequestException as e:
        logger.error(f"Final LegiScan Request exception after retries for op={operation} (id: {request_id_log}): {str(e)}.")
        raise

# --- Helper Function for Document Fetching ---
def _fetch_and_save_document(
    doc_type: str,
    doc_id: Optional[int],
    bill_id: int,
    session_id: int,
    api_operation: str,
    output_dir: Path
):
    """Fetches a single document (text, amendment, supplement) and saves it."""
    if not doc_id:
        logger.warning(f"Missing ID for {doc_type} in bill {bill_id}. Cannot fetch.")
        return False

    params = {'id': doc_id}
    filename = output_dir / f"bill_{bill_id}_{doc_type}_{doc_id}.json"

    if filename.exists():
        logger.debug(f"{doc_type.capitalize()} document {doc_id} for bill {bill_id} already downloaded. Skipping.")
        return True

    try:
        logger.info(f"Fetching {doc_type} document ID: {doc_id} for bill {bill_id} (Session: {session_id})")
        doc_data = fetch_api_data(api_operation, params) # Calls the main fetch function

        if not doc_data or doc_data.get('status') != 'OK':
            logger.warning(f"Failed to retrieve valid {doc_type} document ID {doc_id} for bill {bill_id}. Status: {doc_data.get('status', 'N/A') if doc_data else 'None'}.")
            return False

        # Specific checks for expected keys in document types might be needed here if API varies
        # e.g., if api_operation == 'getText' and 'text' not in doc_data: ...

        save_json(doc_data, filename)
        logger.debug(f"Saved {doc_type} document {doc_id} for bill {bill_id} to {filename}")
        return True

    except APIResourceNotFoundError:
        logger.warning(f"{doc_type.capitalize()} document ID {doc_id} (bill {bill_id}) not found via API. Skipping.")
        return False
    except APIRateLimitError:
        logger.error(f"Hit LegiScan rate limit fetching {doc_type} document {doc_id} (bill {bill_id}).")
        raise
    except Exception as e:
        logger.error(f"Unhandled exception fetching {doc_type} document {doc_id} (bill {bill_id}): {e}", exc_info=True)
        return False

# --- LegiScan Dataset Info Fetching ---
@retry(
    stop=stop_after_attempt(LEGISCAN_MAX_RETRIES),
    wait=wait_exponential(multiplier=1.5, min=2, max=60),
    retry=retry_if_exception_type((requests.exceptions.RequestException, APIRateLimitError)),
    before_sleep=before_sleep_log(logging.getLogger(), logging.WARNING)
)
def get_session_dataset_info(session_id: int) -> Optional[Dict[str, str]]:
    """
    Calls getDatasetList for a specific session_id to get its dataset hash and access key.
    """
    logger.info(f"Fetching dataset info for session_id: {session_id}")
    params = {'id': session_id}
    try:
        data = fetch_api_data('getDatasetList', params)

        if not data or data.get('status') != 'OK' or 'datasetlist' not in data:
            logger.error(f"Failed to retrieve valid dataset list for session {session_id}. Status: {data.get('status', 'N/A')}")
            return None

        dataset_list = data.get('datasetlist', [])
        if not isinstance(dataset_list, list):
            logger.error(f"Unexpected format for datasetlist response for session {session_id}: {type(dataset_list)}")
            return None

        found_dataset_info = None
        for item in dataset_list:
            if isinstance(item, dict) and item.get('session_id') == session_id:
                found_dataset_info = item
                break

        if found_dataset_info:
            info = {
                'session_id': found_dataset_info.get('session_id'),
                'dataset_hash': found_dataset_info.get('dataset_hash'),
                'dataset_date': found_dataset_info.get('dataset_date'),
                'dataset_size': found_dataset_info.get('dataset_size'),
                'access_key': found_dataset_info.get('access_key'),
            }
            if info['dataset_hash'] and info['access_key']:
                logger.info(f"Found dataset info for session {session_id}: hash={info['dataset_hash']}, date={info['dataset_date']}")
                return info
            else:
                 logger.warning(f"Found dataset entry for session {session_id} but missing hash or access key: {found_dataset_info}")
                 return None
        elif len(dataset_list) > 0:
            logger.warning(f"Dataset list returned, but no entry found matching session {session_id}. Available sessions in response: {[d.get('session_id') for d in dataset_list if isinstance(d, dict)]}")
            return None
        else:
            logger.info(f"No dataset found listed for session {session_id} via getDatasetList.")
            return None

    except APIResourceNotFoundError:
        logger.info(f"LegiScan API reported no dataset found for session {session_id} (Resource Not Found).")
        return None
    except APIRateLimitError:
        logger.error(f"Hit LegiScan rate limit fetching dataset list for session {session_id}.")
        raise
    except Exception as e:
        logger.error(f"Unhandled exception fetching dataset list for session {session_id}: {e}", exc_info=True)
        return None

# --- LegiScan Data Collection Functions ---
def get_session_list(state: str, years: Iterable[int], paths: Dict[str, Path]) -> List[Dict[str, Any]]:
    """Get list of LegiScan sessions for the state and year range, saving raw response."""
    logger.info(f"Fetching session list for {state} covering years {min(years)}-{max(years)}...")
    params = {'state': state}
    session_list = []
    raw_sessions_path = paths['raw'] / f"legiscan_sessions_{state}_{min(years)}-{max(years)}.json"

    try:
        data = fetch_api_data('getSessionList', params)

        if not data or data.get('status') != 'OK' or 'sessions' not in data:
            logger.error(f"Failed to retrieve valid session list for state {state}. API response status: {data.get('status') if data else 'No response'}")
            if data: save_json(data, raw_sessions_path.with_suffix('.error.json'))
            return []

        save_json(data, raw_sessions_path)

        target_years = set(years)
        api_sessions = data.get('sessions', [])

        if not isinstance(api_sessions, list):
            logger.error(f"LegiScan API returned unexpected format for sessions: {type(api_sessions)}. Expected list.")
            return []

        for session in api_sessions:
            if not isinstance(session, dict) or 'session_id' not in session:
                logger.warning(f"Skipping invalid session entry in API response: {session}")
                continue
            try:
                session_id = session.get('session_id')
                year_start = int(session.get('year_start', 0))
                year_end_str = session.get('year_end')
                year_end = int(year_end_str) if year_end_str and str(year_end_str).isdigit() else year_start

                if year_start == 0:
                    logger.warning(f"Skipping session with invalid year_start 0: {session.get('session_name')}")
                    continue

                session_years = set(range(year_start, year_end + 1))
                if not session_years.isdisjoint(target_years):
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
                        'dataset_hash': session.get('dataset_hash')
                    })
            except (ValueError, TypeError) as e:
                logger.warning(f"Skipping session due to invalid year data '{session.get('year_start')}-{session.get('year_end')}': {session.get('session_name')}. Error: {e}")
                continue

        if session_list:
             logger.info(f"Found {len(session_list)} relevant LegiScan sessions for {state} in specified years.")
             session_list.sort(key=lambda s: s.get('year_start', 0), reverse=True)
        else:
             logger.warning(f"No relevant LegiScan sessions found for {state} covering {min(years)}-{max(years)}.")

    except APIResourceNotFoundError:
         logger.error(f"Could not find state '{state}' via LegiScan API. Check state abbreviation.")
         return []
    except APIRateLimitError:
         logger.error(f"Hit LegiScan rate limit fetching session list for {state}. Try again later.")
         return []
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
    legislators_data: Dict[int, Dict[str, Any]] = {}
    raw_legislators_dir = paths['raw_legislators']
    processed_data_dir = paths['processed']

    if not sessions:
        logger.warning("No sessions provided to collect_legislators. Cannot proceed.")
        return

    for session in tqdm(sessions, desc=f"Fetching legislators ({state})", unit="session"):
        session_id = session.get('session_id')
        session_name = session.get('session_name', f'ID: {session_id}')
        if not session_id:
            logger.warning(f"Session missing session_id: {session_name}. Skipping.")
            continue

        params = {'id': session_id}
        try:
            data = fetch_api_data('getSessionPeople', params)
            if not data or data.get('status') != 'OK' or 'sessionpeople' not in data:
                logger.warning(f"Failed to get valid people list for session {session_name} (ID: {session_id}). Status: {data.get('status', 'N/A')}")
                continue

            session_people_list = data.get('sessionpeople', {}).get('people', [])

            if not isinstance(session_people_list, list):
                logger.warning(f"No 'people' list found or invalid format in sessionpeople for session {session_id}.")
                continue
            if not session_people_list:
                 logger.info(f"No people found (empty list) for session {session_name} (ID: {session_id}).")
                 continue

            for person in session_people_list:
                if not isinstance(person, dict):
                    logger.warning(f"Skipping invalid person entry (not a dict): {person}")
                    continue

                legislator_id = person.get('people_id')
                if legislator_id and legislator_id not in legislators_data:
                    legislators_data[legislator_id] = {
                        'legislator_id': legislator_id,
                        'person_hash': person.get('person_hash'),
                        'state_id': person.get('state_id'),
                        'name': person.get('name', ''),
                        'first_name': person.get('first_name', ''),
                        'middle_name': person.get('middle_name', ''),
                        'last_name': person.get('last_name', ''),
                        'suffix': person.get('suffix', ''),
                        'nickname': person.get('nickname', ''),
                        'party_id': person.get('party_id', ''),
                        'party': person.get('party', ''),
                        'role_id': person.get('role_id'),
                        'role': person.get('role', ''),
                        'district': person.get('district', ''),
                        'committee_sponsor': person.get('committee_sponsor', 0),
                        'committee_id': person.get('committee_id', 0),
                        'state': state.upper(),
                        'ftm_eid': person.get('ftm_eid'),
                        'votesmart_id': person.get('votesmart_id'),
                        'opensecrets_id': person.get('opensecrets_id'),
                        'knowwho_pid': person.get('knowwho_pid'),
                        'ballotpedia': person.get('ballotpedia'),
                        'state_link': pd.NA,
                        'legiscan_url': pd.NA,
                        'active': 1
                    }
                    raw_leg_path = raw_legislators_dir / f"legislator_{legislator_id}.json"
                    save_json(person, raw_leg_path)

        except APIResourceNotFoundError:
            logger.warning(f"Session people not found via API for session {session_name} (ID: {session_id}). Skipping.")
            continue
        except APIRateLimitError:
             logger.error(f"Hit LegiScan rate limit fetching people for session {session_id}. Consider pausing.")
             break
        except Exception as e:
            logger.error(f"Unhandled exception fetching people for session {session_name} (ID: {session_id}): {e}", exc_info=True)
            continue

    if legislators_data:
        legislator_list = list(legislators_data.values())
        logger.info(f"Collected {len(legislator_list)} unique legislators for {state} across relevant sessions.")

        all_json_path = raw_legislators_dir / f'all_legislators_{state}.json'
        processed_csv_path = processed_data_dir / f'legislators_{state}.csv'

        save_json(legislator_list, all_json_path)

        csv_columns = [
            'legislator_id', 'person_hash', 'name', 'first_name', 'middle_name', 'last_name',
            'suffix', 'nickname', 'party_id', 'party', 'role_id', 'role',
            'district', 'state_id', 'state', 'active', 'committee_sponsor', 'committee_id',
            'ftm_eid', 'votesmart_id', 'opensecrets_id', 'knowwho_pid', 'ballotpedia',
            'state_link', 'legiscan_url'
        ]
        convert_to_csv(legislator_list, processed_csv_path, columns=csv_columns)
    else:
        logger.warning(f"No legislator data collected for state {state}. Creating empty placeholder files.")
        processed_csv_path = processed_data_dir / f'legislators_{state}.csv'
        all_json_path = raw_legislators_dir / f'all_legislators_{state}.json'
        csv_columns = [
            'legislator_id', 'person_hash', 'name', 'first_name', 'middle_name', 'last_name',
            'suffix', 'nickname', 'party_id', 'party', 'role_id', 'role',
            'district', 'state_id', 'state', 'active', 'committee_sponsor', 'committee_id',
            'ftm_eid', 'votesmart_id', 'opensecrets_id', 'knowwho_pid', 'ballotpedia',
            'state_link', 'legiscan_url'
        ]
        convert_to_csv([], processed_csv_path, columns=csv_columns)
        save_json([], all_json_path)


def collect_committee_definitions(session: Dict[str, Any], paths: Dict[str, Path]):
    """(Currently STUBBED) Fetch committee definitions for a single session."""
    session_id = session.get('session_id')
    session_name = session.get('session_name', f'ID: {session_id}')
    year = session.get('year_start')
    raw_committees_dir = paths['raw_committees']

    if not session_id or not year:
        logger.warning(f"Session missing ID or valid start year: {session_name}. Skipping committee definition collection.")
        return

    year_dir = raw_committees_dir / str(year)
    year_dir.mkdir(parents=True, exist_ok=True)
    session_committees_json_path = year_dir / f'committees_{session_id}.json'

    logger.info(f"Collecting committee definitions for {year} session: {session_name} (ID: {session_id})...")
    processed_committees = []

    # --- Skip API Call ---
    # The 'getSessionCommittees' operation appears invalid or deprecated.
    # Committee data might be derived later from bill information or scraped memberships.
    logger.warning(f"Skipping direct API call for committee definitions for session {session_id} (getSessionCommittees is likely invalid).")
    logger.warning("Committee data will need to be sourced from bill data or other means later.")
    # --- End Skip ---

    # Save empty list
    logger.warning(f"No committee definitions collected via direct API call for session {session_id} ({year}). Saving empty list.")
    save_json(processed_committees, session_committees_json_path) 