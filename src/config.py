# src/config.py
"""Central configuration settings for Valley Vote."""

import os
from pathlib import Path
from dotenv import load_dotenv
import logging
from typing import Optional, Dict, List, Any

# --- Environment Variables ---
# Load .env file if it exists in the project root (for local development)
# Create a .env file in the project root with LEGISCAN_API_KEY=YOUR_KEY
env_path = Path(__file__).resolve().parent.parent / '.env' # Assumes src is one level down from root
load_dotenv(dotenv_path=env_path)

LEGISCAN_API_KEY = os.environ.get('LEGISCAN_API_KEY')
# Ensure the API key is loaded (critical for core functionality)
if not LEGISCAN_API_KEY:
    # In a real application, raising an error might be better than just warning.
    print("CRITICAL WARNING: LEGISCAN_API_KEY environment variable not set. LegiScan API calls WILL fail.")
    # raise ValueError("FATAL: LEGISCAN_API_KEY environment variable not set.")

# --- LegiScan API Configuration ---
LEGISCAN_BASE_URL = 'https://api.legiscan.com/'
LEGISCAN_MAX_RETRIES = 5
LEGISCAN_DEFAULT_WAIT_SECONDS = 1.1 # Base wait time between API calls

# --- Data Collection Configuration ---
DEFAULT_YEARS_START = 2010 # Default start year if not specified via CLI

# --- Web Scraping & Matching Configuration ---
# Idaho Committee Scraping (Specific to ID)
ID_HOUSE_COMMITTEES_URL = "https://legislature.idaho.gov/committees/housecommittees/"
ID_SENATE_COMMITTEES_URL = "https://legislature.idaho.gov/committees/senatecommittees/"
COMMITTEE_MEMBER_MATCH_THRESHOLD = 85 # Minimum fuzzy match score

# Selectors for parsing Idaho committee pages (these might need updates if site changes)
ID_COMMITTEE_HEADING_SELECTORS = ['h3', 'h4']
ID_COMMITTEE_CONTENT_SELECTORS = ['ul', 'ol', 'p'] # Tags likely containing member lists

# Idaho Finance Scraping (Specific to ID)
ID_FINANCE_BASE_URL = 'https://sunshine.sos.idaho.gov/'
# Verify this path remains correct by inspecting the website's network traffic during a search
ID_FINANCE_DOWNLOAD_WAIT_SECONDS = 1.5 # Wait between finance download attempts

# Fuzzy Matching Thresholds (0-100)
FINANCE_MATCH_THRESHOLD = 88 # Finance record name/committee to API legislator name

# Terms indicating a committee name structure (used in finance matching)
FINANCE_COMMITTEE_INDICATORS = [
    'committee', 'campaign', 'friends of', 'citizens for', 'pac',
    'for senate', 'for house', 'for governor', 'for congress', 'election',
    'victory fund', 'leadership pac', 'party', 'caucus'
    # Add more specific terms observed in data
]

# --- File System ---
# Base data directory default, can be overridden by CLI argument
# Path('.') refers to the directory where the script is run from,
# which might be the project root. Using Path(__file__) is often safer for relative paths within a package.
# However, for a 'data' dir usually expected at the root, Path('data') is common.
DEFAULT_BASE_DATA_DIR = Path('data')

# --- Logging ---
# Log file names (these will be placed in the logs subdirectory within the base data dir)
MAIN_LOG_FILE = 'valley_vote.log'
DATA_COLLECTION_LOG_FILE = 'data_collection.log'
FINANCE_SCRAPE_LOG_FILE = 'scrape_finance_idaho.log'
FINANCE_MATCH_LOG_FILE = 'match_finance_to_leg.log'
MONITOR_LOG_FILE = 'monitor_idaho_structure.log'

# --- Data Schema Related Constants ---
# LegiScan Status Codes (for status_desc in bills.csv)
STATUS_CODES = {
    0: 'N/A', 1: 'Introduced', 2: 'Engrossed', 3: 'Enrolled', 4: 'Passed',
    5: 'Vetoed', 6: 'Failed', 7: 'Override', 8: 'Chaptered', 9: 'Refer',
    10: 'Report Pass', 11: 'Report DNP', 12: 'Draft', 13: 'Committee Process',
    14: 'Calendars', 15: 'Failed Vote', 16: 'Veto Override Pass', 17: 'Veto Override Fail'
}

# LegiScan Sponsor Types (for sponsor_type in sponsors.csv)
SPONSOR_TYPES = {
    0: 'Sponsor (Generic / Unspecified)',
    1: 'Primary Sponsor',
    2: 'Co-Sponsor',
    3: 'Joint Sponsor' # May not be used often, check API docs
}

# Mapping for vote_text to standardized vote_value in votes.csv
VOTE_TEXT_MAP = {
    'yea': 1, 'aye': 1, 'yes': 1, 'pass': 1, 'y': 1,
    'nay': 0, 'no': 0, 'fail': 0, 'n': 0,
    'not voting': -1, 'abstain': -1, 'present': -1, 'nv': -1, 'av': -1,
    'absent': -2, 'excused': -2, 'abs': -2, 'exc': -2,
}

# --- File System ---
# Base directory for all data (can be overridden via command line)
# Using Path object for better path handling
RAW_DATA_DIR = DEFAULT_BASE_DATA_DIR / 'raw'
PROCESSED_DATA_DIR = DEFAULT_BASE_DATA_DIR / 'processed'
ARTIFACTS_DIR = DEFAULT_BASE_DATA_DIR / 'artifacts'
LOG_DIR = DEFAULT_BASE_DATA_DIR / 'logs'

# Default log file name for the main data collection process
DATA_COLLECTION_LOG_FILE = LOG_DIR / 'data_collection.log'

# Ensure log directory exists
LOG_DIR.mkdir(parents=True, exist_ok=True)

# --- Finance Data Configuration ---
# These maps are used by scrape_finance_idaho.py and potentially parsing scripts
FINANCE_COLUMN_MAPS = {
    'contributions': {
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
        'committee_name': ['committee name', 'recipient committee', 'filer name'],
        'report_name': ['report name', 'report title', 'report'],
        'transaction_id': ['transaction id', 'tran id', 'transactionid']
    },
    'expenditures': {
        'expenditure_date': ['date', 'expenditure date', 'payment date'],
        'payee_name': ['payee', 'paid to', 'name', 'payee name', 'vendor name'],
        'expenditure_amount': ['amount', 'expenditure amount', '$', 'payment amount'],
        'expenditure_purpose': ['purpose', 'description', 'expenditure purpose', 'memo'],
        'payee_address': ['address', 'payee address', 'vendor address', 'addr 1'],
        'payee_city': ['city', 'payee city', 'vendor city'],
        'payee_state': ['state', 'st', 'payee state', 'vendor state'],
        'payee_zip': ['zip', 'zip code', 'payee zip', 'vendor zip'],
        'expenditure_type': ['type', 'expenditure type', 'payment type', 'expenditure code'],
        'committee_name': ['committee name', 'paying committee', 'filer name'],
        'report_name': ['report name', 'report title', 'report'],
        'transaction_id': ['transaction id', 'tran id', 'transactionid']
    }
}

# Placeholder mapping for manually acquired data (update when format known)
MANUAL_FINANCE_COLUMN_MAP = {}
# TODO: Populate MANUAL_FINANCE_COLUMN_MAP when data format is known

# --- Data Schema Related Constants ---
# LegiScan Status Codes (for status_desc in bills.csv)
STATUS_CODES = {
    0: 'N/A', 1: 'Introduced', 2: 'Engrossed', 3: 'Enrolled', 4: 'Passed',
    5: 'Vetoed', 6: 'Failed', 7: 'Override', 8: 'Chaptered', 9: 'Refer',
    10: 'Report Pass', 11: 'Report DNP', 12: 'Draft', 13: 'Committee Process',
    14: 'Calendars', 15: 'Failed Vote', 16: 'Veto Override Pass', 17: 'Veto Override Fail'
}

# LegiScan Sponsor Types (for sponsor_type in sponsors.csv)
SPONSOR_TYPES = {
    0: 'Sponsor (Generic / Unspecified)',
    1: 'Primary Sponsor',
    2: 'Co-Sponsor',
    3: 'Joint Sponsor' # May not be used often, check API docs
}

# Mapping for vote_text to standardized vote_value in votes.csv
VOTE_TEXT_MAP = {
    'yea': 1, 'aye': 1, 'yes': 1, 'pass': 1, 'y': 1,
    'nay': 0, 'no': 0, 'fail': 0, 'n': 0,
    'not voting': -1, 'abstain': -1, 'present': -1, 'nv': -1, 'av': -1,
    'absent': -2, 'excused': -2, 'abs': -2, 'exc': -2,
}

# --- File System ---
# Base directory for all data (can be overridden via command line)
# Using Path object for better path handling
RAW_DATA_DIR = DEFAULT_BASE_DATA_DIR / 'raw'
PROCESSED_DATA_DIR = DEFAULT_BASE_DATA_DIR / 'processed'
ARTIFACTS_DIR = DEFAULT_BASE_DATA_DIR / 'artifacts'
LOG_DIR = DEFAULT_BASE_DATA_DIR / 'logs'

# Default log file name for the main data collection process
DATA_COLLECTION_LOG_FILE = LOG_DIR / 'data_collection.log'

# Ensure log directory exists
LOG_DIR.mkdir(parents=True, exist_ok=True) 
