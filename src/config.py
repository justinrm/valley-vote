# src/config.py
"""Central configuration settings for Valley Vote."""

import os
from pathlib import Path
from dotenv import load_dotenv

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
ID_HOUSE_COMMITTEES_URL = 'https://legislature.idaho.gov/house/committees/'
ID_SENATE_COMMITTEES_URL = 'https://legislature.idaho.gov/senate/committees/'
ID_COMMITTEE_HEADING_SELECTORS = ['h3', 'h4'] # Tags likely containing committee names
ID_COMMITTEE_CONTENT_SELECTORS = ['ul', 'ol', 'p'] # Tags likely containing member lists
ID_MIN_EXPECTED_HEADINGS = 5 # For monitoring page structure

# Idaho Finance Scraping (Specific to ID)
ID_FINANCE_BASE_URL = 'https://sunshine.sos.idaho.gov/'
# Verify this path remains correct by inspecting the website's network traffic during a search
ID_FINANCE_DOWNLOAD_WAIT_SECONDS = 1.5 # Wait between finance download attempts

# Fuzzy Matching Thresholds (0-100)
COMMITTEE_MEMBER_MATCH_THRESHOLD = 85 # Scraped committee member name to API legislator name
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
