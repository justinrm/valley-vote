### Data Collection Script (data_collection.py) README
### Overview

This script serves as the primary data acquisition tool for the Valley Vote project, focusing on fetching core legislative information from the LegiScan API. It also includes state-specific web scraping capabilities (currently implemented only for Idaho committee memberships) and functionality to match names from scraped data to official legislator records using fuzzy matching.

## The script fetches information about (via LegiScan API):

*   Legislative Sessions (`getSessionList`)
*   Legislator Profiles (`getSessionPeople`)
*   Committee Definitions (`getCommittee`)
*   Bills (Master List via `getMasterListRaw`, Details via `getBill`)
*   Bill Sponsors (extracted from `getBill`)
*   Roll Call Votes (Stubs from `getBill`, Details via `getRollCall`)
*   Full Bill Texts, Amendments, and Supplements (Optional via `--fetch-*` flags, using `getText`, `getAmendment`, `getSupplement`)

It saves raw data responses (primarily JSON) for auditability and processes/consolidates this information into structured yearly files (JSON/CSV) suitable for downstream analysis and model training.

**Note on External Data Sources:** While this script handles core LegiScan data and Idaho committee scraping, other complex data sources (Campaign Finance, District Demographics, Election History) are intended to be collected and processed by **separate, dedicated scripts**. This script includes non-functional *stub functions* as placeholders and integration points for these external sources. For example, Idaho campaign finance data collection is currently paused for automated scraping (`scrape_finance_idaho.py`) and relies on manual acquisition and a separate parsing script (`parse_finance_idaho_manual.py`).

Features

Comprehensive LegiScan API Integration: Fetches data from key LegiScan endpoints:

getSessionList: Retrieves legislative sessions for a state and year range.

getSessionPeople: Gets legislator details for a specific session (used for primary legislator info).

getCommittee: Fetches committee definitions for a session.

getMasterList: Gets the list of bills for a session.

getBill: Retrieves detailed information for a specific bill, including sponsors, vote stubs, text/amendment/supplement stubs, subjects, and history.

*   Efficient Bill Fetching: Uses `getMasterListRaw` and compares `change_hash` values to fetch details (`getBill`) only for new or updated bills, reducing API calls.

getRollCall: Fetches detailed vote results (including individual legislator votes) for a specific vote ID.

getText: Fetches the full text content of a specific bill document version.

getAmendment: Fetches the full text content of a specific bill amendment.

getSupplement: Fetches the full text content of a specific bill supplement (e.g., fiscal note).

Robust Fetching & Error Handling:

Uses tenacity for automatic retries with exponential backoff for API calls and web scraping requests.

Handles transient network errors, timeouts, and specific HTTP status codes (e.g., 429 Rate Limit, 404 Not Found).

Parses LegiScan API status codes ('OK' vs. 'ERROR') and specific error messages.

Includes custom exceptions (APIRateLimitError, APIResourceNotFoundError).

State-Specific Web Scraping (Idaho):

Scrapes current-year committee membership information from the Idaho Legislature website (legislature.idaho.gov).

Parses HTML using BeautifulSoup. Note: This component is inherently fragile and depends heavily on the Idaho Legislature website's structure remaining consistent.

Fuzzy Name Matching:

Uses fuzzywuzzy (with python-Levenshtein for speed) to match legislator names scraped from websites (currently Idaho committees) to the official names (name field) retrieved from the LegiScan API (getSessionPeople data).

Links scraped memberships to official legislator_id based on a configurable score threshold.

Structured Data Output:

Saves raw JSON responses from the API (per legislator, bill, vote, committee, session) and scraping (per committee, consolidated raw) in the data/raw/ directory hierarchy for auditability and reprocessing.

Consolidates API data per session and aggregates it into yearly JSON and CSV files (e.g., bills_2023_ID.csv, votes_2023_ID.csv) in the data/processed/ directory.

Saves scraped and matched committee membership data to CSV files in data/processed/.

Organizes raw data by type and year/session using pathlib.

Configuration & Control:

Requires LegiScan API key via the LEGISCAN_API_KEY environment variable.

Accepts command-line arguments to specify state, year range, skip specific collection/processing steps (API, scraping, matching, consolidation), trigger stub functions, and override the base data directory.

Logging: Provides informative logging to both the console and a data_collection.log file (overwritten each run).

Extensibility Stubs: Includes placeholder functions (collect_campaign_finance, collect_district_demographics, collect_election_history) that log warnings. These explicitly indicate where integration points for separate collection scripts exist.

Document Fetching Flags: Includes `--fetch-texts`, `--fetch-amendments`, `--fetch-supplements` flags to enable fetching the full content of these documents via the API during the `run_api` step.

Dependencies

Install the required Python libraries using pip:

pip install requests pandas tenacity tqdm beautifulsoup4 fuzzywuzzy python-Levenshtein


(Note: Ensure python-Levenshtein is installed for faster fuzzy matching; fuzzywuzzy will use it automatically if available.)

Configuration

API Key: Set the LEGISCAN_API_KEY environment variable before running the script.

Linux/macOS: export LEGISCAN_API_KEY='your_actual_api_key'

Windows (cmd): set LEGISCAN_API_KEY=your_actual_api_key

Windows (PowerShell): $env:LEGISCAN_API_KEY='your_actual_api_key'

The script will exit with an error if this variable is not set.

Command-Line Arguments:

--state STATE: State abbreviation (e.g., ID, CA). Case-insensitive input, stored internally as uppercase. (Default: ID)

--start-year YEAR: Start year for data collection (inclusive). (Default: 2010)

--end-year YEAR: End year for data collection (inclusive). (Default: Current Year)

--skip-api: Skip all LegiScan API data collection steps.

--skip-scraping: Skip web scraping for current year committee memberships (currently only affects Idaho).

--skip-matching: Skip fuzzy matching of scraped members to API legislators.

--skip-consolidation: Skip consolidation of yearly API data (bills, votes, sponsors, committees) and the consolidation of matched scraped memberships across years.

--collect-finance: (STUB) Trigger placeholder for campaign finance collection. Logs a warning; requires a separate script like scrape_finance_idaho.py to actually collect data.

--collect-demographics: (STUB) Trigger placeholder for demographics collection. Logs a warning; requires a separate script to implement collection/processing.

--collect-elections: (STUB) Trigger placeholder for election history collection. Logs a warning; requires a separate script to implement collection/parsing.

--data-dir PATH: Override the base data directory (default: ./data). All raw/ and processed/ subdirectories will be relative to this path.

--fetch-texts: Fetch full bill text documents via API (requires extra API calls).

--fetch-amendments: Fetch full amendment documents via API (requires extra API calls).

--fetch-supplements: Fetch full supplement documents via API (requires extra API calls).

Directory Structure

The script creates and uses the following directory structure within the specified base data directory (default is ./data/):

<base_data_dir>/
├── raw/
│   ├── legislators/      # Raw JSON per legislator (e.g., 12345.json), all_legislators_{state}.json
│   ├── committees/       # yearly subdirs (e.g., 2023/) containing raw JSON per committee (e.g., committee_678.json), session summary (e.g., committees_{session_id}.json), yearly summary (e.g., all_committees_{year}_{state}.json)
│   ├── bills/            # yearly subdirs (e.g., 2023/) containing raw JSON per bill (e.g., bill_98765.json), session summary (e.g., bills_{session_id}.json), yearly summary (e.g., all_bills_{year}_{state}.json)
│   ├── votes/            # yearly subdirs (e.g., 2023/) containing raw JSON per roll call (e.g., vote_{roll_call_id}.json), session summary (e.g., votes_{session_id}.json), yearly summary (e.g., all_votes_{year}_{state}.json)
│   ├── sponsors/         # yearly subdirs (e.g., 2023/) containing session summary (e.g., sponsors_{session_id}.json), yearly summary (e.g., all_sponsors_{year}_{state}.json)
│   ├── committee_memberships/ # yearly subdirs (e.g., 2024/) containing raw scraped JSON per committee, consolidated raw scraped JSON (e.g., scraped_memberships_raw_{state}_{year}.json), consolidated *matched* JSON (e.g., scraped_memberships_matched_{state}_{year}.json), and potentially consolidated matched across years (e.g., all_memberships_scraped_consolidated_{state}.json)
│   ├── campaign_finance/ # (Stub) Placeholder created; data intended to be populated by separate script(s)
│   ├── demographics/     # (Stub) Placeholder created; data intended to be populated by separate script(s)
│   ├── elections/        # (Stub) Placeholder created; data intended to be populated by separate script(s)
│   ├── texts/            # Raw JSON documents if fetched via --fetch-texts (e.g., text_{doc_id}.json)
│   ├── amendments/       # Raw JSON documents if fetched via --fetch-amendments (e.g., amendment_{doc_id}.json)
│   └── supplements/      # Raw JSON documents if fetched via --fetch-supplements (e.g., supplement_{doc_id}.json)
│
└── processed/
    ├── legislators_{state}.csv                     # Consolidated unique legislators across all specified sessions
    ├── committees_{year}_{state}.csv               # Consolidated committee definitions for a given year
    ├── bills_{year}_{state}.csv                    # Consolidated bill details for a given year
    ├── votes_{year}_{state}.csv                    # Consolidated roll call vote records for a given year
    ├── sponsors_{year}_{state}.csv                 # Consolidated bill sponsorship records for a given year
    ├── committee_memberships_scraped_matched_{state}_{year}.csv # Matched scraped data (typically current year only)
    └── committee_memberships_scraped_consolidated_{state}.csv  # Consolidated matched scraped data across available years
    # Potentially other processed files created by separate scripts later
IGNORE_WHEN_COPYING_START
content_copy
download
Use code with caution.
IGNORE_WHEN_COPYING_END
Workflow

Initialization: Parses command-line arguments, sets up logging, resolves data directories, checks for API key, and creates necessary directory structures.

API Collection (if not --skip-api):

Fetches the list of relevant legislative sessions (getSessionList).

Collects unique legislators across all identified sessions (getSessionPeople) and saves the consolidated legislators_{state}.csv.

Iterates through each relevant session:

Collects session details (e.g., using `getDataset` for session info if needed, though typically derived from `getSessionList`).

Collects committee definitions (getCommittee).

Collects master bill list (getMasterList).

For each bill in the master list: Compares `change_hash` to previously stored value (if any). If different or new, fetches detailed bill data (`getBill`), extracts bill info, sponsor records, vote stubs, text/amendment/supplement stubs.

For each vote stub identified: Fetches detailed roll call data (getRollCall) and extracts individual legislator vote records.

If `--fetch-*` flags are used: For each text, amendment, or supplement stub identified in `getBill`, fetches the full document content (`getText`, `getAmendment`, `getSupplement`) and saves it to the corresponding `raw/texts/`, `raw/amendments/`, or `raw/supplements/` directory.

Saves raw JSONs for individual items (legislators, bills, votes, committees) and session-level JSON summaries in the raw/ subdirectories.

Web Scraping (if not --skip-scraping and state is supported, e.g., 'ID'):

Fetches HTML from the configured state legislature's committee pages (Idaho only currently).

Parses the HTML using BeautifulSoup to extract committee names and member lists (names, roles). This step is fragile.

Saves raw scraped data (including member names) to JSON files in raw/committee_memberships/{year}/. Returns the path to a consolidated raw scraped JSON file for the current year.

Matching (if not --skip-matching and scraping occurred or relevant raw file exists):

Loads the raw (flat) scraped membership list JSON for the current year.

Loads the consolidated legislator JSON (all_legislators_{state}.json) created during the API phase.

Uses fuzzywuzzy to match scraped legislator_name_scraped against the official name field from the legislator data.

Adds legislator_id, matched_api_name, and match_score to the membership records.

Saves the updated membership list with matching info to a new "_matched.json" file in raw/committee_memberships/{year}/.

Saves the final matched data to processed/committee_memberships_scraped_matched_{state}_{year}.csv.

Consolidation (if not --skip-consolidation):

API Data: Aggregates session-level API data (bills, votes, sponsors, committees) from the raw JSON files into yearly CSV files (processed/*_{year}_{state}.csv) and yearly JSON summaries (raw/.../all_*_{year}_{state}.json).

Scraped Data: Aggregates the matched scraped committee membership CSVs (processed/committee_memberships_scraped_matched_{state}_{year}.csv) from available years into a single consolidated CSV (processed/committee_memberships_scraped_consolidated_{state}.csv) and a corresponding JSON in the raw directory.

Stub Function Calls (if corresponding --collect-* flags are enabled): Logs warning messages indicating these collection steps need implementation via separate scripts. Creates the placeholder raw directories if they don't exist.

Completion: Logs total execution time and shuts down logging.

Output Files

The script primarily generates:

Raw JSON files: Individual records (legislator, bill, vote, committee) and session/yearly summaries stored under data/raw/. These are crucial for retaining the original data structure and for reprocessing if needed.

Processed CSV files: Yearly consolidated data suitable for analysis and input into preprocessing scripts, stored under data/processed/. Key outputs include:

data/processed/legislators_{STATE}.csv

data/processed/bills_{YEAR}_{STATE}.csv

data/processed/votes_{YEAR}_{STATE}.csv

data/processed/sponsors_{YEAR}_{STATE}.csv

data/processed/committees_{YEAR}_{STATE}.csv

data/processed/committee_memberships_scraped_matched_{STATE}_{YEAR}.csv (Result of scraping + matching, typically current year)

data/processed/committee_memberships_scraped_consolidated_{STATE}.csv (Combines matched data across available years)

Stub Functions for Future Data Sources

This script includes non-functional placeholders for collecting additional data critical for robust vote prediction. These stubs simply log a warning when triggered by their respective command-line flags:

collect_campaign_finance() (--collect-finance): Intended to collect donation data. Requires a separate, dedicated script like scrape_finance_idaho.py (provided as an example) to handle the complexities of state-specific campaign finance portals (e.g., Idaho Sunshine Portal).

*Note: Automated scraping via `scrape_finance_idaho.py` is currently PAUSED due to website complexity. The project relies on manual data acquisition and parsing via `parse_finance_idaho_manual.py` (planned).*

collect_district_demographics() (--collect-demographics): Intended to process Census data (ACS) and TIGER/Line shapefiles. Requires a separate script utilizing geospatial libraries (like geopandas).

collect_election_history() (--collect-elections): Intended to parse state election result files (often PDFs or specific formats). Requires a separate script with custom parsing logic for the target state's files.

The decision to handle these via separate scripts is due to their state-specific nature, reliance on different technologies (web scraping, GIS, PDF parsing), and overall complexity, keeping data_collection.py focused on the core LegiScan API interaction.

Limitations

Web Scraping Fragility: The committee membership scraping is hardcoded for the Idaho Legislature website structure (as observed in early 2024) and is highly likely to break if the site layout changes. Adding support for other states requires writing new, state-specific scraping and parsing logic.

No Feature Engineering: This script focuses solely on data acquisition and basic structuring/consolidation. It does not perform feature engineering (e.g., calculating legislator influence scores, party loyalty, text analysis features) or complex data merging required for modeling. These tasks should occur in a dedicated preprocessing script (e.g., data_preprocessing.py).

Stub Implementation: The functions for collecting campaign finance, demographics, and election history are placeholders only and do not collect any actual data. Their corresponding command-line flags only serve to trigger warning messages.

API Rate Limits/Costs: LegiScan has usage limits. Extensive data collection across many years or states may require a paid plan or careful management of request frequency.

Usage Examples
# --- Common Use Cases ---

# Collect data for Idaho for years 2022-2024, including scraping & matching for current year
python data_collection.py --state ID --start-year 2022 --end-year 2024

# Collect data for Idaho for 2023, AND fetch full text documents
python data_collection.py --state ID --start-year 2023 --end-year 2023 --fetch-texts

# Collect data for California for 2023 only (scraping/matching not implemented for CA, so effectively skipped)
python data_collection.py --state CA --start-year 2023 --end-year 2023

# --- Skipping Specific Steps ---

# Collect only API data for Idaho 2023, skip scraping, matching, and consolidation
python data_collection.py --state ID --start-year 2023 --end-year 2023 --skip-scraping --skip-matching --skip-consolidation

# Run ONLY scraping and matching for Idaho (assumes API data exists from previous run)
# Useful for updating committee memberships if the website changed
python data_collection.py --state ID --start-year 2024 --end-year 2024 --skip-api --skip-consolidation

# Run ONLY consolidation (assumes yearly API files and matched scraped files exist)
python data_collection.py --state ID --start-year 2020 --end-year 2024 --skip-api --skip-scraping --skip-matching

# --- Using Other Options ---

# Run for Idaho 2023, override data directory, and trigger (non-functional) stub collectors
python data_collection.py --state ID --start-year 2023 --end-year 2023 \
    --data-dir /mnt/volume/valley_vote_data \
    --collect-finance --collect-demographics --collect-elections

# Get help on all arguments
python data_collection.py --help
IGNORE_WHEN_COPYING_START
content_copy
download
Use code with caution.
Bash
IGNORE_WHEN_COPYING_END
