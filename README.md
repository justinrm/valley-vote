# Valley Vote: Legislative Data Analysis & Prediction Platform

Valley Vote aims to collect, process, and analyze legislative data, initially focusing on the Idaho State Legislature. The platform seeks to understand legislative behavior by integrating diverse data sources like voting records, bill sponsorships, committee assignments, campaign finance, demographics, and election history. The ultimate goal is to provide insights through data visualization, predictive modeling (vote prediction), and potentially a user-friendly interface including a chatbot for querying legislative information.

## Overview

Understanding why legislators vote the way they do is complex. Valley Vote tackles this by:

1.  **Aggregating Diverse Data:** Collecting data from official sources (LegiScan API, state legislature websites, campaign finance portals, Census) into a unified structure.
2.  **Processing & Cleaning:** Standardizing formats, handling missing data, and matching entities across different datasets (e.g., linking scraped names to official IDs, finance records to legislators).
3.  **Feature Engineering:** Creating meaningful features representing legislator characteristics (seniority, influence), bill properties (complexity, subject), and external context (district demographics, election results, campaign finance).
4.  **Predictive Modeling:** Building models (initially XGBoost) to predict how legislators might vote on future bills, using the engineered features.
5.  **Interpretation & Analysis:** Using tools like SHAP to understand the key drivers behind model predictions and legislator behavior.
6.  **(Planned) Platform Development:** Creating a web-based platform to explore the data, view visualizations, see prediction insights, and potentially interact with a chatbot to ask questions about legislation and legislators.

This project starts with Idaho as a case study, with the potential to expand to other states.

## Key Features

*   **Automated Data Collection:**
    *   Retrieves comprehensive legislative data (sessions, legislators, bills, votes, sponsors, committees) via the **LegiScan API**.
    *   Scrapes **Idaho Legislature website** for current committee memberships.
    *   **Scrapes Idaho SOS Sunshine Portal** for campaign finance contribution/expenditure data:
        *   Uses **Playwright** for robust browser automation to handle dynamic JavaScript-heavy website.
        *   Includes validation scripts (`test_finance_scraper.py`, etc.) to test and refine scraping functionality against the live site.
    *   (Planned) Collects **US Census ACS data** for district demographics.
    *   (Planned) Parses **Idaho election results** for historical performance.
*   **Robust Data Processing:**
    *   Handles API rate limits and errors gracefully (`tenacity`).
    *   Performs fuzzy matching (`thefuzz`) to link scraped/finance data to official legislator IDs.
    *   Consolidates data from different sources and time periods into structured formats (JSON, CSV).
    *   Centralized utility functions (`src/utils.py`) for common tasks like name cleaning.
*   **Vote Prediction Modeling:**
    *   (Planned) Prepares data and engineers features relevant to voting behavior.
    *   (Planned) Trains and tunes an XGBoost model for predicting Yea/Nay votes.
    *   (Planned) Provides model interpretability using Feature Importance and SHAP values.
*   **Monitoring:** Includes scripts to monitor the structure of scraped websites to detect changes that might break the scrapers.
*   **(Planned) Web Platform & Chatbot:** Aims to provide a user interface for data exploration, visualization, and conversational queries about legislative data.

## Technology Stack

*   **Backend / Data Processing:** Python 3.x
*   **Core Libraries:**
    *   `requests`: HTTP requests for API calls.
    *   `playwright`: Browser automation for robust scraping of dynamic websites.
    *   `pandas`: Data manipulation and analysis.
    *   `beautifulsoup4`: HTML parsing (used minimally where JS rendering isn't needed).
    *   `tenacity`: Retrying logic for API calls/web requests.
    *   `thefuzz` (with `python-Levenshtein`): Fuzzy string matching.
    *   `tqdm`: Progress bars.
    *   `python-dotenv`: Environment variable management.
*   **Modeling (Planned/Current Focus):**
    *   `scikit-learn`: Data preprocessing, model evaluation.
    *   `xgboost`: Gradient Boosting model for prediction.
    *   `shap`: Model interpretation.
*   **Geospatial (Planned):**
    *   `geopandas`: Handling district shapefiles and spatial joins.
*   **Database (Planned):** PostgreSQL / SQLite
*   **Backend API (Planned):** Flask / FastAPI
*   **Frontend (Planned):** React / Vue / Streamlit
*   **LLM/Chatbot (Planned):** OpenAI API / Anthropic API / Hugging Face Transformers
*   **Experiment Tracking (Optional):** MLflow / Weights & Biases
*   **Deployment (Planned):** Docker, AWS/GCP/Heroku/Vercel

## Project Structure

```
├── data/
│ ├── artifacts/
│ │ └── debug/ # Playwright HTML snapshots for debugging scrapers
│ ├── logs/
│ ├── processed/
│ └── raw/
│   ├── bills/
│   ├── campaign_finance/
│   ├── committee_memberships/
│   ├── committees/
│   ├── legislators/
│   ├── sponsors/
│   └── votes/
├── docs/
│ ├── data_schema.md
│ ├── feature_engineering.md
│ ├── README_VALIDATION.md
│ └── todo.md # Detailed task tracking
├── notebooks/
├── src/
│ ├── __init__.py
│ ├── config.py
│ ├── data_collection.py
│ ├── main.py
│ ├── match_finance_to_leg.py
│ ├── monitor_idaho_structure.py
│ ├── scrape_finance_idaho.py # Scrapes Idaho finance portal (Uses Playwright)
│ ├── test_finance_scraper.py # Playwright-based validation for finance scraper
│ ├── validate_csv_parsing.py
│ ├── validate_link_finding.py
│ ├── utils.py # Common utilities (logging, name cleaning, etc.)
│ ├── data_preprocessing.py # (Planned)
│ ├── xgboost_model.py # (Planned)
│ └── ... (Planned: api/, frontend/, chatbot/ modules)
├── tests/ # Unit and integration tests (planned)
├── venv/ # Python virtual environment (add to .gitignore)
├── .env.example
├── .gitignore
├── CHANGELOG.md # Project change history
├── LICENSE
├── README.md # This file
└── requirements.txt
```

## Data Sources

*   **LegiScan API:** Provides core legislative data (bills, votes, legislators, etc.). Requires an API key. ([api.legiscan.com](https://api.legiscan.com/))
*   **Idaho Legislature Website:** Source for current committee memberships. Structure is subject to change, requiring scraper maintenance. ([legislature.idaho.gov](https://legislature.idaho.gov/))
*   **Idaho SOS Sunshine Portal:** Source for campaign finance data. Relies on Playwright browser automation due to dynamic JavaScript interface. Structure is subject to change. ([sunshine.sos.idaho.gov](https://sunshine.sos.idaho.gov/))
*   **(Planned) US Census Bureau:** American Community Survey (ACS) 5-Year Estimates for district demographics. Requires identifying correct tables and vintages. TIGER/Line shapefiles for district boundaries.
*   **(Planned) Idaho Secretary of State / voteidaho.gov:** Historical election results. Format varies (PDF, CSV, Excel).

*Disclaimer:* Web scraping, especially of dynamic sites like the Sunshine Portal, is inherently brittle. Changes to these websites **will likely break the scrapers** and require code updates. The use of Playwright helps manage JavaScript, but structural changes still require maintenance.

## Setup & Installation

1.  **Clone the Repository:**
    ```bash
    git clone https://github.com/your-username/valley-vote.git # Replace with actual repo URL
    cd valley-vote
    ```

2.  **Create and Activate a Virtual Environment (HIGHLY Recommended):**
    ```bash
    python3 -m venv venv  # Use python3 or python depending on your system
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```
    *Using a virtual environment prevents conflicts with system packages and is required for reliable dependency management (due to PEP 668).* 

3.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Install Playwright Browsers:** Playwright needs browser binaries.
    ```bash
    playwright install --with-deps 
    ```
    *This downloads necessary browser binaries (Chromium by default) and installs OS-level dependencies. May require `sudo` on Linux for the dependencies.*

5.  **Set Up Environment Variables:**
    *   Copy `.env.example` to `.env`:
        ```bash
        cp .env.example .env
        ```
    *   Edit the `.env` file and add your LegiScan API key:
        ```
        LEGISCAN_API_KEY=YOUR_ACTUAL_LEGISCAN_API_KEY
        ```
    *   The application loads this file automatically using `python-dotenv`. **Do not commit your `.env` file to Git.** Add `venv/` and `.env` to your `.gitignore` file if not already present.

## Usage

The primary way to run data collection and processing tasks is via `main.py`. However, individual scripts can also be run for specific tasks or testing. **Remember to activate the virtual environment (`source venv/bin/activate`) before running scripts.**

**Using `main.py` (Recommended Orchestrator):**

The `main.py` script orchestrates the common data collection workflow.

```bash
python -m src.main --state ID --start-year 2022 --end-year 2023 --data-dir ./my_data
--state: Specify the state abbreviation (default: ID).
--start-year, --end-year: Define the range of legislative years to process.
--data-dir: (Optional) Override the default ./data directory.
--skip-api: Skip all LegiScan API data collection steps.
--skip-finance: Skip campaign finance scraping (scrape_finance_idaho.py).
--skip-matching: Skip matching finance data to legislators (match_finance_to_leg.py).
--monitor-only: Run only the website structure monitor (monitor_idaho_structure.py) and exit.
```

**Running Individual Modules (for testing/specific tasks):**
You can run modules directly using `python -m src.<module_name>`. Refer to the `--help` argument for each script for specific options.

**Finance Scraping & Validation (Using Playwright):**

```bash
# Basic scraping (NOTE: Still under development/validation)
# python -m src.scrape_finance_idaho --start-year 2023 --end-year 2023

# Validate search form elements using Playwright
python -m src.test_finance_scraper --inspect-form

# Validate search submission and results page structure using Playwright
python -m src.test_finance_scraper --inspect-results

# Run predefined search test cases (if implemented in test_finance_scraper)
# python -m src.test_finance_scraper --test-search

# Validate CSV parsing (after obtaining a CSV)
# python -m src.validate_csv_parsing path/to/file.csv --data-type contributions --suggest-mapping

# Validate link finding (if applicable separate from full search)
# python -m src.validate_link_finding --name "John Smith" --year 2022 --data-type contributions
```

**Other Modules (LegiScan, Matching, Monitoring):**

Usage remains similar, run with `python -m src.<module_name> --help` for options.

```bash
# LegiScan Collection & Committee Scraping/Matching:
python -m src.data_collection --state ID --start-year 2023 --end-year 2023 --run bills
python -m src.data_collection --state ID --run scrape_members
python -m src.data_collection --state ID --run match_members
See `data_collection.py --help` for all `--run` options
```

## (Planned) Preprocessing & Modeling:

Usage instructions will be added once `data_preprocessing.py` and `xgboost_model.py` are implemented.

## Current Status & Roadmap

The project is currently focused on validating and refining the **Idaho campaign finance scraper** using Playwright.

*   **Completed:** Core LegiScan API integration, Idaho committee membership scraping/matching, basic project structure, virtual environment setup, initial Playwright integration for finance scraper testing.
*   **In Progress:** Refining Playwright scripts (`test_finance_scraper.py`) to reliably interact with the Idaho SOS Sunshine Portal search form and results page. Identifying the Export button selector is the immediate next step.
*   **Planned:** Completing finance scraper validation, collecting other data sources (demographics, elections), data preprocessing/feature engineering, model development, platform/chatbot development.

For a detailed breakdown of pending tasks, see `docs/todo.md` and `CHANGELOG.md`.

## Contributing

Contributions are welcome! Please feel free to open an issue to report bugs, suggest features, or discuss potential improvements. If you'd like to contribute code, please open an issue first to discuss the change and then submit a pull request.

Licensed under MIT License.


