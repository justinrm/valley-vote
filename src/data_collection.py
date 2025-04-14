# Standard library imports
import json
import time
import random
import logging
import argparse
from datetime import datetime
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional, Iterable, Union
import re

# Third-party imports
import requests
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup
from thefuzz import process, fuzz

# Local imports
from .config import (
    LEGISCAN_API_KEY,
    DEFAULT_YEARS_START,
    COMMITTEE_MEMBER_MATCH_THRESHOLD,
    ID_HOUSE_COMMITTEES_URL,
    ID_SENATE_COMMITTEES_URL,
    ID_COMMITTEE_HEADING_SELECTORS,
    ID_COMMITTEE_CONTENT_SELECTORS,
    DATA_COLLECTION_LOG_FILE,
    STATUS_CODES,
    SPONSOR_TYPES,
    VOTE_TEXT_MAP,
)
from .utils import (
    setup_logging,
    save_json,
    convert_to_csv,
    fetch_page,
    load_json,
    clean_name,
    map_vote_value,
    setup_project_paths
)
# Import the new client functions and exceptions
from .legiscan_client import (
    get_session_dataset_info,
    get_session_list,
    collect_legislators,
    collect_committee_definitions,
    fetch_api_data,
    _fetch_and_save_document,
    APIRateLimitError,
    APIResourceNotFoundError
)
# Import the new dataset handler functions
from .legiscan_dataset_handler import (
    _load_dataset_hashes,
    _save_dataset_hashes,
    download_and_extract_dataset
)

# --- Configure Logging ---
logger = logging.getLogger(Path(DATA_COLLECTION_LOG_FILE).stem)

# --- Ensure API Key is Set (Initial Check) ---
if not LEGISCAN_API_KEY:
    logger.critical("FATAL: LEGISCAN_API_KEY environment variable not set. API calls will fail.")

# --- Custom Exceptions (Only those NOT related to API directly) ---
class ScrapingStructureError(Exception):
    """Custom exception for unexpected website structure during scraping."""
    pass

# --- LegiScan Bulk Dataset Helpers (REMOVED - Moved to legiscan_dataset_handler.py) ---

# --- LegiScan Dataset Download/Extraction (REMOVED - Moved to legiscan_dataset_handler.py) ---

# --- Combined Bill/Vote/Sponsor Collection (Uses client & handler functions) ---
def collect_bills_votes_sponsors(
    session: Dict[str, Any],
    paths: Dict[str, Path],
    dataset_hashes: Dict[int, str], # Use int keys
    fetch_flags: Optional[Dict[str, bool]] = None,
    force_download: bool = False
):
    """
    Fetch and save bills using the LegiScan Bulk Dataset API (via helpers),
    fetch associated votes (getRollCall via client), and optionally full texts,
    amendments, and supplements (via client) for a single LegiScan session.
    Updates dataset hashes state (using handler functions).
    """
    fetch_flags = fetch_flags or {}
    fetch_texts_flag = fetch_flags.get('fetch_texts', False)
    fetch_amendments_flag = fetch_flags.get('fetch_amendments', False)
    fetch_supplements_flag = fetch_flags.get('fetch_supplements', False)

    session_id = session.get('session_id')
    session_name = session.get('session_name', f'ID: {session_id}')
    year = session.get('year_start')

    # Get relevant output directories from paths dict
    bills_dir = paths['raw_bills']
    votes_dir = paths['raw_votes']
    sponsors_dir = paths['raw_sponsors']
    texts_dir = paths.get('raw_texts')
    amendments_dir = paths.get('raw_amendments')
    supplements_dir = paths.get('raw_supplements')
    dataset_storage_base = paths.get('artifacts') / 'legiscan_datasets'
    dataset_storage_base.mkdir(parents=True, exist_ok=True)

    if not session_id or not year:
        logger.warning(f"Session missing ID or valid start year: {session_name}. Skipping bill/vote/sponsor collection.")
        return

    # Ensure year-specific output subdirectories exist
    bills_year_dir = bills_dir / str(year); bills_year_dir.mkdir(parents=True, exist_ok=True)
    votes_year_dir = votes_dir / str(year); votes_year_dir.mkdir(parents=True, exist_ok=True)
    sponsors_year_dir = sponsors_dir / str(year); sponsors_year_dir.mkdir(parents=True, exist_ok=True)

    texts_year_dir = None
    if fetch_texts_flag and texts_dir:
        texts_year_dir = texts_dir / str(year); texts_year_dir.mkdir(parents=True, exist_ok=True)
    elif fetch_texts_flag: logger.warning("Cannot fetch texts: 'raw_texts' path not configured.")

    amendments_year_dir = None
    if fetch_amendments_flag and amendments_dir:
        amendments_year_dir = amendments_dir / str(year); amendments_year_dir.mkdir(parents=True, exist_ok=True)
    elif fetch_amendments_flag: logger.warning("Cannot fetch amendments: 'raw_amendments' path not configured.")

    supplements_year_dir = None
    if fetch_supplements_flag and supplements_dir:
        supplements_year_dir = supplements_dir / str(year); supplements_year_dir.mkdir(parents=True, exist_ok=True)
    elif fetch_supplements_flag: logger.warning("Cannot fetch supplements: 'raw_supplements' path not configured.")

    logger.info(f"Collecting bills via Bulk Dataset for {year} session: {session_name} (ID: {session_id})...")
    if any(fetch_flags.values()):
        logger.info(f"Document Fetching: Texts={fetch_texts_flag}, Amendments={fetch_amendments_flag}, Supplements={fetch_supplements_flag}")

    session_bills_json_path = bills_year_dir / f'bills_{session_id}.json'
    session_sponsors_json_path = sponsors_year_dir / f'sponsors_{session_id}.json'
    session_votes_json_path = votes_year_dir / f'votes_{session_id}.json'

    dataset_bill_dir = None
    needs_download = False
    current_hash = "unknown"
    access_key = ""
    extracted_bill_path_check = dataset_storage_base / f"session_{session_id}" / "bill"

    # --- 1. Check Dataset Status (uses imported client function) ---
    try:
        logger.info(f"Checking dataset status for session {session_id}...")
        dataset_info = get_session_dataset_info(session_id)

        if not dataset_info:
            logger.warning(f"No dataset information found for session {session_id}. Cannot proceed with bulk download.")
            save_json([], session_bills_json_path); save_json([], session_sponsors_json_path); save_json([], session_votes_json_path)
            return

        current_hash = dataset_info['dataset_hash']
        access_key = dataset_info['access_key']
        stored_hash = dataset_hashes.get(session_id)

        if force_download:
            logger.info(f"Forcing dataset download for session {session_id} due to flag.")
            needs_download = True
        elif stored_hash != current_hash:
            logger.info(f"Dataset hash mismatch for session {session_id} (Stored: {stored_hash}, API: {current_hash}). Download needed.")
            needs_download = True
        elif stored_hash is None:
             logger.info(f"Stored hash not found for session {session_id}. Download needed.")
             needs_download = True

        if not needs_download and not extracted_bill_path_check.is_dir():
            logger.warning(f"Dataset hash matches ({current_hash}), but extracted data missing: {extracted_bill_path_check}. Download needed.")
            needs_download = True

    except (APIResourceNotFoundError, APIRateLimitError) as e:
         logger.error(f"API error preventing dataset check for session {session_id}: {e}")
         return
    except Exception as e:
        logger.error(f"Unhandled exception during dataset check for session {session_id}: {e}", exc_info=True)
        return

    # --- 2. Download/Extract if Needed (uses imported handler function) ---
    if needs_download:
        try:
             # Use the imported download function from the handler
             dataset_bill_dir = download_and_extract_dataset(
                 session_id, access_key, dataset_storage_base, expected_hash=current_hash
             )
        except (APIRateLimitError, requests.exceptions.RequestException) as e: # Need to import requests for this
            logger.error(f"Download failed after retries for session {session_id} due to: {e}. Halting session processing.")
            return
        except Exception as e:
             logger.error(f"Non-retryable error during dataset download/extraction for session {session_id}: {e}. Halting session processing.", exc_info=True)
             return

        if dataset_bill_dir:
            # Update hash store using the imported handler function
            dataset_hashes[session_id] = current_hash
            _save_dataset_hashes(dataset_hashes, paths)
            logger.info(f"Updated stored hash for session {session_id} to {current_hash}.")
        else:
            logger.error(f"Failed to download or extract dataset for session {session_id}. Cannot process bills.")
            save_json([], session_bills_json_path); save_json([], session_sponsors_json_path); save_json([], session_votes_json_path)
            return
    else:
        logger.info(f"Dataset hash matches stored hash ({current_hash}) and extracted data exists. Using existing data.")
        dataset_bill_dir = extracted_bill_path_check

    # --- 3. Process Bills from Dataset Files ---
    # ... (rest of the bill processing logic remains the same, using loaded JSONs) ...
    # ... Make sure SPONSOR_TYPES is available (imported from config at top level)
    # ... Make sure map_vote_value is available (imported from utils) ...
    # ... Inside the loop:
    # --- 4. Fetch Votes & Documents (uses imported API client functions) ---
    # ... Use _fetch_and_save_document for texts, amendments, supplements
    # ... Use fetch_api_data for getRollCall ...

    # The rest of the function (processing loops, saving final lists) remains largely unchanged,
    # but now relies on imported functions for API calls and dataset management.
    # Ensure correct imports for fetch_api_data and _fetch_and_save_document are used
    # within the loops where votes and documents are fetched.

    # --- Process Bills from Dataset Files (Continuing from above) ---
    if not dataset_bill_dir or not dataset_bill_dir.is_dir():
         logger.error(f"Bill dataset directory is invalid or missing: {dataset_bill_dir}. Cannot process bills.")
         save_json([], session_bills_json_path); save_json([], session_sponsors_json_path); save_json([], session_votes_json_path)
         return

    session_bills = []
    session_sponsors = [] # Sponsors collected directly from bill files now
    bill_process_errors = 0

    logger.info(f"Processing bill JSON files from dataset directory: {dataset_bill_dir}")
    bill_files = list(dataset_bill_dir.glob("*.json"))
    if not bill_files:
        logger.warning(f"No bill JSON files found in dataset directory: {dataset_bill_dir}")
        save_json([], session_bills_json_path); save_json([], session_sponsors_json_path); save_json([], session_votes_json_path)
        return

    for bill_file_path in tqdm(bill_files, desc=f"Processing dataset bills {session_id} ({year})", unit="file") :
        try:
            bill_data = load_json(bill_file_path)
            if not bill_data or not isinstance(bill_data, dict):
                logger.warning(f"Failed to load valid JSON data from dataset file: {bill_file_path}. Skipping.")
                bill_process_errors += 1; continue
            
            # Access the actual bill data nested under the 'bill' key
            bill = bill_data.get('bill')
            if not bill or not isinstance(bill, dict):
                logger.warning(f"JSON file {bill_file_path} missing top-level 'bill' key or it's not a dictionary. Skipping.")
                bill_process_errors += 1; continue
                
            bill_id = bill.get('bill_id')
            if not bill_id: logger.warning(f"Bill data in {bill_file_path} (under 'bill' key) missing 'bill_id'. Skipping."); bill_process_errors += 1; continue

            status_code = int(bill.get('status', 0))
            bill_record = { 'bill_id': bill_id, 'change_hash': bill.get('change_hash'), 'session_id': bill.get('session_id'), 'year': year, 'state': bill.get('state', '').upper(), 'state_id': bill.get('state_id'), 'url': bill.get('url'), 'state_link': bill.get('state_link'), 'number': bill.get('bill_number', ''), 'type': bill.get('bill_type', ''), 'type_id': bill.get('bill_type_id'), 'body': bill.get('body', ''), 'body_id': bill.get('body_id'), 'current_body': bill.get('current_body', ''), 'current_body_id': bill.get('current_body_id'), 'title': bill.get('title', ''), 'description': bill.get('description', ''), 'status': status_code, 'status_desc': STATUS_CODES.get(status_code, 'Unknown'), 'status_date': bill.get('status_date', ''), 'pending_committee_id': bill.get('pending_committee_id', 0) }

            subjects = bill.get('subjects', []); bill_record['subjects'] = ';'.join(str(s.get('subject_name', '')) for s in subjects if isinstance(s, dict)); bill_record['subject_ids'] = ';'.join(str(s.get('subject_id', '')) for s in subjects if isinstance(s, dict))
            sasts = bill.get('sasts', []); sast_recs = [{k: s.get(k) for k in ['type_id', 'type', 'sast_bill_number', 'sast_bill_id']} for s in sasts if isinstance(s, dict)]; bill_record['sast_relations'] = json.dumps(sast_recs)
            texts = bill.get('texts', []); text_stubs = [{k: t.get(k) for k in ['doc_id', 'date', 'type', 'type_id', 'mime', 'mime_id']} for t in texts if isinstance(t, dict)]; bill_record['text_stubs'] = json.dumps(text_stubs)
            amends = bill.get('amendments', []); amend_stubs = [{k: a.get(k) for k in ['amendment_id', 'adopted', 'chamber', 'chamber_id', 'date', 'title']} for a in amends if isinstance(a, dict)]; bill_record['amendment_stubs'] = json.dumps(amend_stubs)
            supps = bill.get('supplements', []); supp_stubs = [{k: s.get(k) for k in ['supplement_id', 'date', 'type', 'type_id', 'title']} for s in supps if isinstance(s, dict)]; bill_record['supplement_stubs'] = json.dumps(supp_stubs)

            sponsors_list = bill.get('sponsors', [])
            if isinstance(sponsors_list, list):
                 for sponsor in sponsors_list:
                     if isinstance(sponsor, dict):
                          sid = sponsor.get('sponsor_type_id')
                          session_sponsors.append({ 'bill_id': bill_id, 'legislator_id': sponsor.get('people_id'), 'sponsor_type_id': sid, 'sponsor_type': SPONSOR_TYPES.get(sid, 'Unknown'), 'sponsor_order': sponsor.get('sponsor_order', 0), 'committee_sponsor': sponsor.get('committee_sponsor', 0), 'committee_id': sponsor.get('committee_id', 0), 'session_id': session_id, 'year': year })
                     else: logger.warning(f"Invalid sponsor: {sponsor}")
            elif sponsors_list: logger.warning(f"Bad sponsor format: {type(sponsors_list)}")

            bill_record['_vote_stubs_list'] = bill.get('votes', [])
            session_bills.append(bill_record)
        except Exception as e_bill: logger.error(f"Error processing {bill_file_path}: {e_bill}", exc_info=True); bill_process_errors += 1; continue

    if bill_process_errors > 0: logger.warning(f"Encountered {bill_process_errors} errors processing bill files.")

    # --- 4. Fetch Votes & Documents --- (Corrected section header)
    session_votes = []
    vote_fetch_errors, text_fetch_errors, amendment_fetch_errors, supplement_fetch_errors = 0, 0, 0, 0

    # Use fetch_api_data and _fetch_and_save_document imported from legiscan_client
    from .legiscan_client import fetch_api_data, _fetch_and_save_document

    for bill_record in tqdm(session_bills, desc=f"Processing votes/docs for session {session_id} ({year})", unit="bill"):
        bill_id = bill_record.get('bill_id')
        if not bill_id: logger.debug("Skipping record missing bill_id"); continue
        votes_list_stubs = bill_record.get('_vote_stubs_list', [])
        if not isinstance(votes_list_stubs, list): logger.warning(f"Bad vote stubs: {type(votes_list_stubs)}"); continue
        for vote_stub in votes_list_stubs:
             if not isinstance(vote_stub, dict): logger.warning(f"Invalid vote stub: {vote_stub}"); continue
             vote_id = vote_stub.get('roll_call_id')
             if not vote_id: logger.warning(f"Stub missing roll_call_id: {vote_stub}"); continue
             vote_filename = votes_year_dir / f"vote_{vote_id}.json"; roll_call = None
             if vote_filename.exists():
                 try: roll_data = load_json(vote_filename); roll_call = roll_data['roll_call'] if roll_data and isinstance(roll_data.get('roll_call'), dict) else None
                 except Exception as e: logger.error(f"Err loading {vote_filename}: {e}")
             else:
                 try:
                     roll_data = fetch_api_data('getRollCall', {'id': vote_id})
                     if roll_data and roll_data.get('status') == 'OK' and isinstance(roll_data.get('roll_call'), dict):
                         roll_call = roll_data['roll_call']; save_json(roll_data, vote_filename)
                     else: logger.warning(f"Failed fetch vote {vote_id}: {roll_data.get('status','N/A') if roll_data else 'None'}"); vote_fetch_errors += 1
                 except APIResourceNotFoundError: logger.warning(f"Vote {vote_id} not found."); vote_fetch_errors += 1
                 except APIRateLimitError: logger.error(f"Rate limit vote {vote_id}."); raise
                 except Exception as e: logger.error(f"Err fetch vote {vote_id}: {e}"); vote_fetch_errors += 1
             if roll_call:
                 ind_votes = roll_call.get('votes', [])
                 if isinstance(ind_votes, list):
                      for v in ind_votes:
                          if isinstance(v, dict):
                               leg_id = v.get('people_id')
                               if leg_id: session_votes.append({ 'vote_id': vote_id, 'bill_id': roll_call.get('bill_id'), 'legislator_id': leg_id, 'vote_id_type': v.get('vote_id'), 'vote_text': v.get('vote_text', ''), 'vote_value': map_vote_value(v.get('vote_text')), 'date': roll_call.get('date', ''), 'description': roll_call.get('desc', ''), 'yea': roll_call.get('yea', 0), 'nay': roll_call.get('nay', 0), 'nv': roll_call.get('nv', 0), 'absent': roll_call.get('absent', 0), 'total': roll_call.get('total', 0), 'passed': int(roll_call.get('passed', 0)), 'chamber': roll_call.get('chamber', ''), 'chamber_id': roll_call.get('chamber_id'), 'session_id': session_id, 'year': year })
                               else: logger.debug(f"Vote miss leg ID: {v}")
                          else: logger.warning(f"Invalid indiv vote: {v}")
                 else: logger.warning(f"Bad votes array: {type(ind_votes)}")
        try: text_stubs, amendment_stubs, supplement_stubs = json.loads(bill_record.get('text_stubs','[]')), json.loads(bill_record.get('amendment_stubs','[]')), json.loads(bill_record.get('supplement_stubs','[]'))
        except json.JSONDecodeError as e: logger.warning(f"Bad doc stubs: {e}"); text_stubs, amendment_stubs, supplement_stubs = [], [], []
        if fetch_texts_flag and texts_year_dir and isinstance(text_stubs, list): [text_fetch_errors := text_fetch_errors + (1 - _fetch_and_save_document('text', t.get('doc_id'), bill_id, session_id, 'getText', texts_year_dir)) for t in text_stubs if isinstance(t, dict)] # Walrus requires Python 3.8+
        if fetch_amendments_flag and amendments_year_dir and isinstance(amendment_stubs, list): [amendment_fetch_errors := amendment_fetch_errors + (1 - _fetch_and_save_document('amendment', a.get('amendment_id'), bill_id, session_id, 'getAmendment', amendments_year_dir)) for a in amendment_stubs if isinstance(a, dict)]
        if fetch_supplements_flag and supplements_year_dir and isinstance(supplement_stubs, list): [supplement_fetch_errors := supplement_fetch_errors + (1 - _fetch_and_save_document('supplement', s.get('supplement_id'), bill_id, session_id, 'getSupplement', supplements_year_dir)) for s in supplement_stubs if isinstance(s, dict)]

    # --- 5. Save Consolidated Processed Lists for the Session ---
    # ... (Saving logic remains the same) ...
    logger.info(f"Finished processing session {session_id}. Results: Bills={len(session_bills)}, Sponsors={len(session_sponsors)}, Votes={len(session_votes)}.")
    if bill_process_errors > 0: logger.warning(f"Bill processing errors: {bill_process_errors}")
    if vote_fetch_errors > 0: logger.warning(f"Vote fetch errors: {vote_fetch_errors}")
    if text_fetch_errors > 0: logger.warning(f"Text fetch errors: {text_fetch_errors}")
    if amendment_fetch_errors > 0: logger.warning(f"Amendment fetch errors: {amendment_fetch_errors}")
    if supplement_fetch_errors > 0: logger.warning(f"Supplement fetch errors: {supplement_fetch_errors}")

    cleaned_session_bills = []
    for bill in session_bills:
        cleaned_bill = bill.copy(); cleaned_bill.pop('_vote_stubs_list', None)
        cleaned_session_bills.append(cleaned_bill)

    save_json(cleaned_session_bills, session_bills_json_path)
    save_json(session_sponsors, session_sponsors_json_path)
    save_json(session_votes, session_votes_json_path)

    logger.info(f"Saved updated session data to:")
    logger.info(f"  Bills: {session_bills_json_path}")
    logger.info(f"  Sponsors: {session_sponsors_json_path}")
    logger.info(f"  Votes: {session_votes_json_path}")


# --- Data Consolidation Function (Remains Here) ---
def consolidate_yearly_data(data_type: str, years: Iterable[int], columns: List[str], state_abbr: str, paths: Dict[str, Path]):
    # ... (Implementation remains the same) ...
    logger.info(f"Consolidating {data_type} data for {state_abbr}, years {min(years)}-{max(years)}...")
    raw_base_dir = paths.get(f'raw_{data_type}'); processed_base_dir = paths.get('processed')
    if not raw_base_dir or not processed_base_dir: logger.error(f"Cannot consolidate: Invalid type '{data_type}' or missing dirs."); return
    primary_keys = { 'legislators': 'legislator_id', 'committees': 'committee_id', 'bills': 'bill_id', 'sponsors': ['bill_id', 'legislator_id', 'sponsor_type_id', 'committee_id'], 'votes': ['vote_id', 'legislator_id'] }
    primary_key = primary_keys.get(data_type); logger.info(f"Deduplicating {data_type} using: {primary_key}") if primary_key else None
    for year in tqdm(years, desc=f"Consolidating {data_type} ({state_abbr})", unit="year"):
        year_dir = raw_base_dir / str(year); all_year_data = []
        if not year_dir.is_dir(): logger.debug(f"Skip {year}: no dir {year_dir}"); continue
        files_processed = 0
        for filepath in year_dir.glob(f"{data_type}_*.json"):
            filename_match = re.match(rf"^{data_type}_(\d+)\.json$", filepath.name)
            if not filename_match: logger.debug(f"Skip non-session: {filepath.name}") if not filepath.name.startswith('all_') else None; continue
            session_id_from_file = filename_match.group(1); logger.debug(f"Reading: {filepath} (Sess: {session_id_from_file})"); files_processed += 1
            try:
                session_data = load_json(filepath)
                if isinstance(session_data, list): all_year_data.extend(session_data)
                elif session_data is None: logger.warning(f"File empty/bad load: {filepath}")
                else: logger.warning(f"Expected list in {filepath}, got {type(session_data)}. Skip.")
            except Exception as e: logger.error(f"Error reading {filepath}: {e}", exc_info=True)
        if files_processed == 0: logger.debug(f"No session files in {year_dir}.")
        year_json_path = year_dir / f'all_{data_type}_{year}_{state_abbr}.json'; year_csv_path = processed_base_dir / f'{data_type}_{year}_{state_abbr}.csv'
        if all_year_data:
            orig_count = len(all_year_data); unique_data = all_year_data
            if primary_key:
                seen_ids, unique_list, dups_found = set(), [], 0
                logger.debug(f"Deduplicating {orig_count} for {year}...")
                for item in all_year_data:
                    if not isinstance(item, dict): dups_found += 1; logger.warning(f"Skip non-dict: {item}"); continue
                    item_id, key_ok = None, True
                    if isinstance(primary_key, list): # Composite key
                        try: id_tuple = tuple(item.get(pk) for pk in primary_key); key_ok = not any(pd.isna(val) for val in id_tuple); item_id = id_tuple if key_ok else None
                        except Exception as e: logger.warning(f"Key error: {e}"); key_ok = False
                    else: item_id = item.get(primary_key); key_ok = not pd.isna(item_id)
                    if not key_ok: logger.debug("Incomplete key. Keeping."); unique_list.append(item); continue
                    if item_id is not None:
                       if item_id not in seen_ids: unique_list.append(item); seen_ids.add(item_id)
                       else: dups_found += 1
                    else: logger.warning("None PK after checks. Keeping."); unique_list.append(item)
                if dups_found > 0: logger.info(f"Removed {dups_found} duplicates for {year}.")
                unique_data = unique_list
            final_count = len(unique_data); logger.info(f"Consolidated {final_count} unique for {year}.")
            save_json(unique_data, year_json_path); convert_to_csv(unique_data, year_csv_path, columns=columns)
        else: logger.warning(f"No data for {year}. Creating empty files."); save_json([], year_json_path); convert_to_csv([], year_csv_path, columns=columns)

# --- Web Scraping Functions (Idaho Specific - Remain Here) ---
# ... (parse_idaho_committee_page function remains the same) ...
# ... (scrape_committee_memberships function remains the same) ...

# --- Scraped Member Matching (Remains Here) ---
# ... (match_scraped_legislators function remains the same) ...

# --- Consolidate Scraped Memberships (Remains Here) ---
# ... (consolidate_membership_data function remains the same) ...

# --- Stub Functions for Future Data Sources (Remain Here) ---
# ... (collect_campaign_finance, etc. remain the same) ...

# --- Main Execution Block (Remains Here, uses imported functions) ---
# ... (if __name__ == "__main__" block remains the same, but calls imported functions) ...

# --- Finance Column Maps (Remains Here) ---
# ... (FINANCE_COLUMN_MAPS definition remains the same) ...