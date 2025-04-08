# Valley Vote: Legislative Data Analysis & Prediction Platform

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
    *   Uses LegiScan's `getMasterListRaw` and `change_hash` comparison for efficient fetching of only new or updated bill details.
    *   Fetches full **Bill Text, Amendment, and Supplement** documents (optional via command-line flags).
*   **Automated Data Collection (Web Scraping - Idaho):**
    *   Scrapes **Idaho Legislature website** for current committee memberships.
    *   Includes basic structure monitoring (`monitor_idaho_structure.py`) to help detect site changes.
*   **Robust Data Processing:**
    *   Handles API rate limits and errors gracefully (`tenacity`).
    *   Performs fuzzy matching (`thefuzz`) to link scraped committee member names to official legislator IDs.
    *   Consolidates data from different sources and time periods into structured formats (JSON, CSV).
    *   Centralized utility functions (`src/utils.py`) for common tasks like logging and name cleaning.

**Under Development / Paused:**

*   **Campaign Finance Data Acquisition (Idaho SOS Sunshine Portal):**
    *   Uses **Playwright** for robust browser automation to handle the dynamic JavaScript-heavy website.
    *   Includes validation scripts (`test_finance_scraper.py`) for testing scraping functionality.
    *   **Status: Paused.** Automated scraping via Playwright proved challenging and is currently paused. The project is pivoting to using manually acquired data obtained via public records requests. The Playwright scripts are retained for reference.

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
    *   `playwright`: Browser automation for web scraping.
    *   `pandas`: Data manipulation and analysis.
    *   `beautifulsoup4`: HTML parsing.
    *   `tenacity`: Retrying logic for API calls/web requests.
    *   `thefuzz` (with `python-Levenshtein`): Fuzzy string matching.
    *   `tqdm`: Progress bars.
    *   `python-dotenv`: Environment variable management.
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
│   ├── config.py               # Configuration constants
│   ├── data_collection.py      # LegiScan API & Committee scraping logic
│   ├── main.py                 # Main script orchestrator
│   ├── match_finance_to_leg.py # Matches finance data (if available)
│   ├── monitor_idaho_structure.py # Monitors website structure
│   ├── scrape_finance_idaho.py # Scrapes Idaho finance portal (Uses Playwright - CURRENTLY PAUSED)
│   ├── test_finance_scraper.py # Playwright-based validation for finance scraper
│   ├── validate_csv_parsing.py # Validates finance CSV structure
│   ├── validate_link_finding.py # (Potentially deprecated)
│   ├── parse_finance_idaho_manual.py # (Planned/Skeleton) Parses manually acquired finance data
│   ├── utils.py                # Common utilities (logging, name cleaning, etc.)
│   ├── data_preprocessing.py   # (Planned)
│   ├── xgboost_model.py        # (Planned)
│   └── ... (Planned: api/, frontend/, chatbot/ modules)
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
*   **Idaho SOS Sunshine Portal:** Source for campaign finance data. Due to the dynamic interface, automated scraping with Playwright proved difficult. Data is currently planned to be acquired manually via public records request. ([sunshine.sos.idaho.gov](https://sunshine.sos.idaho.gov/))
*   **(Planned) US Census Bureau:** American Community Survey (ACS) 5-Year Estimates for district demographics. Requires identifying correct tables and vintages. TIGER/Line shapefiles for district boundaries.
*   **(Planned) Idaho Secretary of State / voteidaho.gov:** Historical election results. Format varies (PDF, CSV, Excel?).

*Disclaimer:* Web scraping is inherently brittle. Changes to source websites **will likely break the scrapers** and require code updates. While `monitor_idaho_structure.py` can help detect changes, manual verification and updates are often necessary.

## Setup & Installation

1.  **Clone the Repository:**
    ```bash
    git clone https://github.com/your-username/valley-vote.git # Replace with actual repo URL
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

4.  **Install Playwright Browsers:** Although finance scraping is paused, Playwright might be used for other tasks or reactivated later.
    ```bash
    playwright install --with-deps chromium # Install Chromium and OS dependencies
    ```
    *   This downloads necessary browser binaries (Chromium specified here) and attempts to install OS-level dependencies. You might need `sudo` on Linux for the dependencies. Verify the command completes successfully.

5.  **Set Up Environment Variables:**
    *   Copy `.env.example` to `.env`:
        ```bash
        cp .env.example .env
        ```
    *   Edit the `.env` file and add your LegiScan API key:
        ```dotenv
        LEGISCAN_API_KEY="YOUR_ACTUAL_LEGISCAN_API_KEY"
        ```
    *   The application loads this file automatically using `python-dotenv`. **Do not commit your `.env` file to Git.** Ensure `venv/` and `.env` are listed in your `.gitignore` file.

## Usage

**Activation:** Remember to **activate the virtual environment** (`source venv/bin/activate` or equivalent) in your terminal session before running any scripts.

**Main Orchestrator (`main.py`):**
The `main.py` script provides a convenient way to run common data collection workflows.

```bash
# Example: Collect LegiScan data and scrape committees for ID for 2023
python -m src.main --state ID --start-year 2023 --end-year 2023 --skip-finance --skip-matching

# Get help on available arguments
python -m src.main --help
```
*   `--state`: Specify the state abbreviation (default: ID).
*   `--start-year`, `--end-year`: Define the range of legislative years to process.
*   `--data-dir`: (Optional) Override the default `./data` directory.
*   `--skip-api`: Skip all LegiScan API data collection steps.
*   `--skip-committees`: Skip scraping and matching ID committee memberships.
*   `--skip-finance`: Skip campaign finance processing (currently inactive anyway).
*   `--skip-matching`: Skip matching finance data to legislators (currently inactive anyway).
*   `--monitor-only`: Run only the website structure monitor and exit.
*   `--fetch-texts`, `--fetch-amendments`, `--fetch-supplements`: Flags to enable fetching full documents via LegiScan API during the API run.

**Running Individual Modules:**
Modules can be run individually for targeted tasks, testing, or debugging. Use the `--help` flag for specific options (e.g., `python -m src.data_collection --help`).

```bash
# LegiScan Collection (specific run type):
python -m src.data_collection --state ID --start-year 2023 --end-year 2023 --run bills

# LegiScan Collection with Full Text Documents:
python -m src.data_collection --state ID --start-year 2023 --end-year 2023 --run bills --fetch-texts

# Scrape & Match Idaho Committee Memberships:
python -m src.data_collection --state ID --run scrape_members
python -m src.data_collection --state ID --run match_members

# Finance Scraper Validation (Primarily for development/debugging):
# Note: These interact with the live Sunshine Portal and test the PAUSED automated scraper.
# python -m src.test_finance_scraper --inspect-form      # For debugging the form interaction
# python -m src.test_finance_scraper --inspect-results    # For debugging results page interaction
```

## (Planned) Preprocessing & Modeling:

Usage instructions will be added once `data_preprocessing.py` and `xgboost_model.py` are implemented.

## Current Status & Roadmap

*   **Completed:**
    *   Core LegiScan API integration (including efficient updates using `change_hash`).
    *   Fetching of full bill texts, amendments, supplements via LegiScan API.
    *   Idaho committee membership scraping & matching logic.
    *   Basic project structure, environment setup.
    *   Initial Playwright setup and validation scripts for finance portal interaction.
*   **Paused:**
    *   Automated Idaho campaign finance scraping via Playwright (pending manual data acquisition).
*   **Next Steps / Planned:**
    *   Implement caching for API calls (`requests-cache`).
    *   Acquire Idaho campaign finance data (manual source).
    *   Flesh out and implement parser for manual finance data (`parse_finance_idaho_manual.py` skeleton created).
    *   Refine finance-to-legislator matching based on manual data format.
    *   Develop parsers for other data sources (demographics, elections).
    *   Implement data preprocessing and feature engineering (`data_preprocessing.py`).
    *   Develop and train predictive models (`xgboost_model.py`).
    *   Build out testing suite (`tests/`).
    *   (Longer Term) Develop platform API, frontend, and chatbot components.

For a detailed breakdown of pending tasks, see `docs/todo.md`. For recent changes, see `CHANGELOG.md`.

## Contributing

Contributions are welcome! Please feel free to open an issue to report bugs, suggest features, or discuss potential improvements. If you'd like to contribute code, please open an issue first to discuss the proposed change and then submit a pull request. Adherence to the project's coding style and contribution guidelines (when established) is appreciated.

Licensed under MIT License.


