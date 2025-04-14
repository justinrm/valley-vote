# Valley Vote: Legislative Data Analysis & Prediction Platform

**GitHub Repository:** [https://github.com/justinrm/valley-vote](https://github.com/justinrm/valley-vote)

Valley Vote aims to collect, process, and analyze legislative data, initially focusing on the Idaho State Legislature. The platform seeks to enhance understanding of legislative behavior and potentially improve transparency by integrating diverse data sources like voting records, bill sponsorships, committee assignments, campaign finance, demographics, and election history. The ultimate goal is to provide insights through data visualization, predictive modeling (vote prediction), and potentially a user-friendly interface including a chatbot for querying legislative information.

## Overview

Understanding why legislators vote the way they do is complex and crucial for informed civic engagement. Valley Vote tackles this challenge by:

1.  **Aggregating Diverse Data:** Collecting data from official sources (LegiScan API, state legislature websites, campaign finance portals, Census) into a unified structure.
2.  **Processing & Cleaning:** Standardizing formats, handling missing data, and matching entities across different datasets (e.g., linking scraped names to official IDs, finance records to legislators).
3.  **Feature Engineering:** Creating meaningful features representing legislator characteristics (seniority, influence), bill properties (complexity, subject), and external context (district demographics, election results, campaign finance).
4.  **Predictive Modeling:** Building models (initially XGBoost) to predict how legislators might vote on future bills, using the engineered features.
5.  **Interpretation & Analysis:** Using tools like SHAP to understand the key drivers behind model predictions and legislator behavior.
6.  **(Planned) Platform Development:** Creating a web-based platform to explore the data, view visualizations, see prediction insights, and potentially interact with a chatbot to ask questions about legislation and legislators.

This project starts with Idaho as a case study, with the potential to expand to other states.

## Key Features

**Implemented:**

*   **Automated Data Collection (LegiScan API):**
    *   Retrieves comprehensive legislative data (sessions, legislators, bills, votes, sponsors, committees).
    *   Uses LegiScan's Bulk Dataset API (`getDatasetList`, `getDataset`) for efficient fetching of full session bill data, comparing dataset hashes to avoid redundant downloads.
    *   Fetches full **Bill Text, Amendment, and Supplement** documents (optional via command-line flags).
*   **Automated Data Collection (Web Scraping - Idaho):**
    *   Scrapes **Idaho Legislature website** for current committee memberships (`idaho_scraper.py` within `data_collection.py`).
    *   Includes basic structure monitoring (`monitor_idaho_structure.py`) to help detect site changes.
*   **Amendment Collection & Tracking (`amendment_collection.py`):**
    *   Collects and processes bill amendments using the LegiScan API.
    *   Compares bill text to amendment versions to analyze changes (basic diff planned).
    *   Provides consolidated datasets for amendment analysis.
*   **News Article Collection (`news_collection.py`):**
    *   Collects news articles related to legislation via News API.
    *   Generates intelligent search queries based on bill information.
    *   Extracts full article text when available using newspaper3k.
    *   Uses NLP techniques (NLTK) for basic text processing.
*   **Campaign Finance Data Collection (`finance_collection.py`):**
    *   Retrieves campaign finance data through a configurable external API.
    *   Collects candidate information and contribution details based on API capabilities.
    *   Includes functionality to match finance records to legislators using fuzzy matching (`match_finance_to_leg.py`).

**Under Development / Paused:**

*   **Campaign Finance Data Acquisition (Idaho SOS Sunshine Portal):**
    *   Uses **Playwright** for robust browser automation to handle the dynamic JavaScript-heavy website (`scrape_finance_idaho.py`).
    *   Includes validation scripts (`test_finance_scraper.py`) for testing scraping functionality.
    *   **Status: Paused.** Automated scraping via Playwright proved challenging and is currently paused. The project is pivoting to using manually acquired data obtained via public records requests. The Playwright scripts are retained for reference. A manual parser (`parse_finance_idaho_manual.py`) and validator (`validate_csv_parsing.py`) are under development.
*   **Unit & Integration Testing (`pytest`):**
    *   Test suites are under development in the `tests/` directory.
    *   Tests for core utilities, LegiScan client, finance data collection, and news data collection (`test_news_collection.py`) are partially implemented or in progress.
    *   Goal: Increase coverage for data collection, processing, and eventually modeling modules.

**Planned:**

*   **Additional Data Sources:**
    *   Collect **US Census ACS data** for district demographics.
    *   Parse **Idaho election results** for historical performance.
*   **Vote Prediction Modeling:**
    *   Prepare data and engineer features relevant to voting behavior.
    *   Train and tune an XGBoost model for predicting Yea/Nay votes.
    *   Provide model interpretability using Feature Importance and SHAP values.
*   **Web Platform & Chatbot:**
    *   Develop a user interface for data exploration, visualization, and conversational queries.

## Technology Stack

*   **Backend / Data Processing:** Python 3.x
*   **Core Libraries (Currently Used):**
    *   `requests`: HTTP requests for API calls.
    *   `playwright`: Browser automation for web scraping (Currently Paused usage).
    *   `pandas`: Data manipulation and analysis.
    *   `beautifulsoup4`: HTML parsing.
    *   `tenacity`: Retrying logic for API calls/web requests.
    *   `thefuzz` (with `python-Levenshtein`): Fuzzy string matching.
    *   `tqdm`: Progress bars.
    *   `python-dotenv`: Environment variable management.
    *   `nltk`: Natural language processing for news article analysis.
    *   `newspaper3k`: Article scraping and curation (used by `news_collection.py`).
*   **Modeling & Analysis (Planned):**
    *   `scikit-learn`: Data preprocessing, model evaluation.
    *   `xgboost`: Gradient Boosting model for prediction.
    *   `shap`: Model interpretation.
*   **Geospatial (Planned):**
    *   `geopandas`: Handling district shapefiles and spatial joins.
*   **Potential Future / Platform Components (Planned):**
    *   `requests-cache`: Caching HTTP requests.
    *   Database: PostgreSQL / SQLite
    *   Backend API: Flask / FastAPI
    *   Frontend: React / Vue / Streamlit
    *   LLM/Chatbot: OpenAI API / Anthropic API / Hugging Face Transformers
    *   Experiment Tracking: MLflow / Weights & Biases
    *   Deployment: Docker, Cloud Services (AWS/GCP/etc.)

## Project Structure

```
├── data/
│   ├── artifacts/
│   │   └── debug/      # Playwright HTML snapshots for debugging scrapers
│   ├── logs/           # Log files
│   ├── processed/      # Consolidated/processed data (e.g., yearly CSVs)
│   └── raw/            # Raw data fetched from sources
│       ├── amendments/ # Full amendment documents (JSON)
│       ├── bills/      # Raw bill details (JSON per bill), master lists
│       ├── campaign_finance/ # Raw finance data (CSV/JSON - currently manual)
│       ├── committee_memberships/ # Scraped & matched memberships
│       ├── committees/ # Committee definitions from API
│       ├── legislators/ # Legislator details from API
│       ├── news/       # News articles related to legislation
│       ├── sponsors/   # Sponsor relationships from API
│       ├── supplements/ # Full supplement documents (JSON)
│       ├── texts/      # Full bill text documents (JSON)
│       └── votes/      # Raw vote details (JSON per roll call)
├── docs/
│   ├── data_schema.md          # (Planned) Describes data structure
│   ├── feature_engineering.md  # (Planned) Describes feature creation
│   ├── README_VALIDATION.md    # Notes on validation scripts
│   └── todo.md                 # Detailed task tracking
├── notebooks/                  # Jupyter notebooks for exploration/analysis (Optional)
├── src/                        # Source code
│   ├── __init__.py
│   ├── amendment_collection.py # Collection of bill amendments and analysis
│   ├── config.py               # Configuration constants
│   ├── data_collection.py      # LegiScan API & ID Committee scraping logic
│   ├── data_preprocessing.py   # Processing raw data into features (In Progress)
│   ├── finance_collection.py   # Campaign finance data collection (via configurable API)
│   ├── legiscan_client.py      # LegiScan API client functions
│   ├── legiscan_dataset_handler.py # Handles LegiScan bulk datasets
│   ├── main.py                 # Main script orchestrator
│   ├── match_finance_to_leg.py # Matches finance data (API/Manual) to legislators
│   ├── monitor_idaho_structure.py # Monitors Idaho website structure
│   ├── news_collection.py      # News article collection related to legislation (via NewsAPI)
│   ├── idaho_scraper.py        # Idaho-specific web scraping functions (used by data_collection)
│   ├── scrape_finance_idaho.py # Scrapes Idaho finance portal (Uses Playwright - PAUSED)
│   ├── test_finance_scraper.py # Playwright-based validation for finance scraper (PAUSED)
│   ├── utils.py                # Common utilities (logging, name cleaning, etc.)
│   ├── validate_csv_parsing.py # Validates finance CSV structure (for manual data)
│   ├── validate_link_finding.py # (Potentially deprecated) Validates specific scraper logic
│   ├── parse_finance_idaho_manual.py # Parses manually acquired finance data (In Progress)
│   ├── create_tree.py          # Utility to generate directory listings (Used for Docs)
│   └── ... (Planned: xgboost_model.py, api/, frontend/, chatbot/ modules)
├── tests/                      # Unit and integration tests (planned)
├── venv/                       # Python virtual environment (add to .gitignore)
├── .env.example                # Example environment file
├── .gitignore
├── CHANGELOG.md                # Project change history
├── LICENSE
├── README.md                   # This file
└── requirements.txt            # Python dependencies
```

## Data Sources

*   **LegiScan API:** Provides core legislative data (bills, votes, legislators, etc.). Requires an API key (set in `.env` file). ([api.legiscan.com](https://api.legiscan.com/))
*   **Idaho Legislature Website:** Source for current committee memberships. Structure is subject to change, requiring scraper maintenance. ([legislature.idaho.gov](https://legislature.idaho.gov/))
*   **News API:** Source for news articles related to legislation. Requires an API key (set in `.env` file). ([newsapi.org](https://newsapi.org/))
*   **Finance Data API:** (Configurable) Source for campaign finance data. Requires an API key (set in `.env` file). Implementation uses a generic finance API interface that can be configured for different data providers.
*   **Idaho SOS Sunshine Portal:** Source for campaign finance data. Due to the dynamic interface, automated scraping with Playwright proved difficult and is **currently paused**. Data is currently planned to be acquired manually via public records request and parsed using `parse_finance_idaho_manual.py`. ([sunshine.sos.idaho.gov](https://sunshine.sos.idaho.gov/))
*   **(Planned) US Census Bureau:** American Community Survey (ACS) 5-Year Estimates for district demographics. Requires identifying correct tables and vintages. TIGER/Line shapefiles for district boundaries.
*   **(Planned) Idaho Secretary of State / voteidaho.gov:** Historical election results. Format varies (PDF, CSV, Excel?).

*Disclaimer:* Web scraping is inherently brittle. Changes to source websites **will likely break the scrapers** and require code updates. While `monitor_idaho_structure.py` can help detect changes, manual verification and updates are often necessary.

## Setup & Installation

1.  **Clone the Repository:**
    ```bash
    git clone https://github.com/justinrm/valley-vote.git # Replace with actual repo URL if different
    cd valley-vote
    ```

2.  **Create and Activate a Virtual Environment (STRONGLY Recommended):**
    *   Using a virtual environment isolates project dependencies and prevents conflicts with system-wide packages. This is standard best practice and required for reliable dependency management (due to PEP 668).
    ```bash
    # Ensure you have python3 and pip installed
    python3 -m venv venv
    source venv/bin/activate  # On Linux/macOS
    # venv\\Scripts\\activate  # On Windows Command Prompt
    # venv\\Scripts\\Activate.ps1 # On Windows PowerShell
    ```

3.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Install Playwright Browsers:** Although finance scraping is paused, Playwright might be used for other tasks or reactivated later. Required by `scrape_finance_idaho.py`.
    ```bash
    playwright install --with-deps chromium # Install Chromium and OS dependencies
    ```
    *   This downloads necessary browser binaries (Chromium specified here) and attempts to install OS-level dependencies. You might need `sudo` on Linux for the dependencies. Verify the command completes successfully.

5.  **Install NLTK Data:** Required for news article processing (`news_collection.py`).
    ```bash
    python -c "import nltk; nltk.download('punkt'); nltk.download('stopwords')"
    ```

6.  **Set Up Environment Variables:**
    *   Copy `.env.example` to `.env`:
        ```bash
        cp .env.example .env
        ```
    *   Edit the `.env` file and add your API keys:
        ```dotenv
        LEGISCAN_API_KEY="YOUR_LEGISCAN_API_KEY"
        NEWS_API_KEY="YOUR_NEWS_API_KEY"
        FINANCE_API_KEY="YOUR_FINANCE_API_KEY"
        # Add other API keys or configurations as needed
        ```
    *   The application loads this file automatically using `python-dotenv`. **Do not commit your `.env` file to Git.** Ensure `venv/` and `.env` are listed in your `.gitignore` file.

## Usage

**Activation:** Remember to **activate the virtual environment** (`source venv/bin/activate` or equivalent) in your terminal session before running any scripts.

**Main Orchestrator (`main.py`):**
The `main.py` script provides a convenient way to run common data collection workflows.

```bash
# Example: Collect LegiScan data and scrape committees for ID for 2023
python -m src.main --state ID --start-year 2023 --end-year 2023 --skip-finance --skip-matching

# Example: Collect all data (LegiScan, Committees, API Finance, News, Amendments) for ID for 2023
# Note: Check main.py --help for specific flags to enable/disable modules if available,
# otherwise run modules individually. This example assumes main orchestrates all enabled modules.
python -m src.main --state ID --start-year 2023 --end-year 2023

# Get help on available arguments
python -m src.main --help
```
*   `--state`: Specify the state abbreviation (default: ID).
*   `--start-year`, `--end-year`: Define the range of legislative years to process.
*   `--data-dir`: (Optional) Override the default `./data` directory.
*   `--skip-api`: Skip all LegiScan API data collection steps.
*   `--skip-committees`: Skip scraping and matching ID committee memberships.
*   `--skip-finance`: Skip campaign finance processing (currently uses `finance_collection.py` with API).
*   `--skip-matching`: Skip matching finance data to legislators (part of `finance_collection.py`).
*   `--skip-news`: Skip collecting news articles.
*   `--skip-amendments`: Skip collecting amendments.
*   `--monitor-only`: Run only the website structure monitor and exit.
*   `--fetch-texts`, `--fetch-amendments`, `--fetch-supplements`: Flags to enable fetching full documents via LegiScan API during the API run.

**Running Individual Modules:**
Modules can be run individually for targeted tasks, testing, or debugging. Use the `--help` flag for specific options (e.g., `python -m src.data_collection --help`).

```bash
# LegiScan Collection (specific run type):
python -m src.data_collection --state ID --start-year 2023 --end-year 2023 --fetch-bills --fetch-legislators

# Idaho Committee Membership Scraping & Matching:
python -m src.data_collection --state ID --start-year 2023 --end-year 2023 --committees-only

# Finance API Collection:
python -m src.finance_collection --start-year 2020 --end-year 2024

# Match Finance (API or Manual) to Legislators:
python -m src.match_finance_to_leg --start-year 2020 --end-year 2024

# News Collection:
python -m src.news_collection --state ID --start-year 2023 --end-year 2023 --query-limit 10

# Amendment Collection:
python -m src.amendment_collection --state ID --start-year 2023 --end-year 2023

# Monitor Idaho Website Structure:
python -m src.monitor_idaho_structure
```

*   **Parsing Manually Acquired Idaho Finance Data (`parse_finance_idaho_manual.py`):**
    This script is used to process CSV files downloaded manually from the Idaho SOS Sunshine portal. It categorizes files, performs basic cleaning, and consolidates them into separate processed files.

    ```bash
    # Example: Process all CSVs in the default raw manual directory
    python -m src.parse_finance_idaho_manual

    # Example: Specify raw and processed directories
    python -m src.parse_finance_idaho_manual --raw-dir path/to/your/raw/csvs --processed-dir path/to/save/processed/files

    # Get help on available arguments
    python -m src.parse_finance_idaho_manual --help
    ```
    This script is designed to work with the specific formats downloaded from the Idaho portal and may require adjustments if the source format changes.

*   **(PAUSED) Scraping Idaho Finance Portal (`scrape_finance_idaho.py`):**
    *This script is currently paused due to website complexity.*

## (Planned) Preprocessing & Modeling:

Usage instructions will be added once `data_preprocessing.py` and `xgboost_model.py` are implemented.

## Current Status & Roadmap

*   **Completed:**
    *   Core LegiScan API integration (including efficient updates using Bulk Dataset API).
    *   Fetching of full bill texts, amendments, supplements via LegiScan API (optional flags).
    *   Idaho committee membership scraping & matching logic.
    *   Campaign finance data collection module via configurable API (`