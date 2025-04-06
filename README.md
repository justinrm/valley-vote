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
    *   (In Progress/Needs Validation) Scrapes **Idaho SOS Sunshine Portal** for campaign finance contribution/expenditure data.
    *   (Planned) Collects **US Census ACS data** for district demographics.
    *   (Planned) Parses **Idaho election results** for historical performance.
*   **Robust Data Processing:**
    *   Handles API rate limits and errors gracefully (`tenacity`).
    *   Performs fuzzy matching (`thefuzz`) to link scraped/finance data to official legislator IDs.
    *   Consolidates data from different sources and time periods into structured formats (JSON, CSV).
*   **Vote Prediction Modeling:**
    *   (Planned) Prepares data and engineers features relevant to voting behavior.
    *   (Planned) Trains and tunes an XGBoost model for predicting Yea/Nay votes.
    *   (Planned) Provides model interpretability using Feature Importance and SHAP values.
*   **Monitoring:** Includes scripts to monitor the structure of scraped websites to detect changes that might break the scrapers.
*   **(Planned) Web Platform & Chatbot:** Aims to provide a user interface for data exploration, visualization, and conversational queries about legislative data.

## Technology Stack

*   **Backend / Data Processing:** Python 3.x
*   **Core Libraries:**
    *   `requests`: HTTP requests for API calls and scraping.
    *   `pandas`: Data manipulation and analysis.
    *   `beautifulsoup4`: HTML parsing for web scraping.
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
├── data/ # Data directory (generated, not in git usually)
│ ├── artifacts/ # Debugging outputs, monitoring HTML
│ ├── logs/ # Log files
│ ├── processed/ # Processed & consolidated CSV/JSON data
│ └── raw/ # Raw data fetched from sources (JSON, HTML, CSV)
│ ├── bills/
│ ├── campaign_finance/
│ ├── committee_memberships/
│ ├── committees/
│ ├── legislators/
│ ├── sponsors/
│ └── votes/
├── docs/ # Documentation files (planned)
│ ├── data_schema.md
│ └── feature_engineering.md
├── notebooks/ # Jupyter notebooks for exploration, EDA (optional)
├── src/ # Source code
│ ├── init.py
│ ├── config.py # Configuration settings, API keys (via .env)
│ ├── data_collection.py # LegiScan API collection, ID committee scraping/matching
│ ├── main.py # Main execution script orchestrator
│ ├── match_finance_to_leg.py# Matches scraped finance data to legislators
│ ├── monitor_idaho_structure.py # Checks for website structure changes
│ ├── scrape_finance_idaho.py# Scrapes Idaho campaign finance portal (needs validation)
│ ├── utils.py # Common utility functions (logging, file I/O, fetch)
│ ├── data_preprocessing.py # (Planned) Data cleaning, merging, feature engineering
│ ├── xgboost_model.py # (Planned) Model training, tuning, evaluation
│ └── ... (Planned: api/, frontend/, chatbot/ modules)
├── tests/ # Unit and integration tests (planned)
├── .env.example # Example environment file structure
├── .gitignore
├── LICENSE
├── README.md # This file
├── requirements.txt # Project dependencies
└── TODO.md # Detailed task tracking
```


## Data Sources

*   **LegiScan API:** Provides core legislative data (bills, votes, legislators, etc.). Requires an API key. ([api.legiscan.com](https://api.legiscan.com/))
*   **Idaho Legislature Website:** Source for current committee memberships. Structure is subject to change, requiring scraper maintenance. ([legislature.idaho.gov](https://legislature.idaho.gov/))
*   **Idaho SOS Sunshine Portal:** Source for campaign finance data. Scraping relies on potentially brittle form interactions and download link structures. ([sunshine.sos.idaho.gov](https://sunshine.sos.idaho.gov/))
*   **(Planned) US Census Bureau:** American Community Survey (ACS) 5-Year Estimates for district demographics. Requires identifying correct tables and vintages. TIGER/Line shapefiles for district boundaries.
*   **(Planned) Idaho Secretary of State / voteidaho.gov:** Historical election results. Format varies (PDF, CSV, Excel).

*Disclaimer:* Web scraping is dependent on the target website's structure. Changes to these websites (especially the Idaho Legislature site and Sunshine Portal) **will likely break the scrapers** and require code updates. The `monitor_idaho_structure.py` script can help detect some changes.

## Setup & Installation

1.  **Clone the Repository:**
    ```bash
    git clone https://github.com/your-username/valley-vote.git # Replace with actual repo URL
    cd valley-vote
    ```

2.  **Create and Activate a Virtual Environment (Recommended):**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```

3.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
    *Note:* If `python-Levenshtein` fails on install (common issue needing C++ build tools), you might try `pip install fuzzywuzzy` first, and then `pip install python-Levenshtein` or `pip install thefuzz[speedup]`. Consult platform-specific instructions if needed.

4.  **Set Up Environment Variables:**
    *   Copy `.env.example` to `.env`:
        ```bash
        cp .env.example .env
        ```
    *   Edit the `.env` file and add your LegiScan API key:
        ```
        LEGISCAN_API_KEY=YOUR_ACTUAL_LEGISCAN_API_KEY
        ```
    *   The application loads this file automatically using `python-dotenv`. **Do not commit your `.env` file to Git.**

## Usage

The primary way to run data collection and processing tasks is via `main.py`. However, individual scripts can also be run for specific tasks or testing.

**Using `main.py` (Recommended Orchestrator):**

The `main.py` script orchestrates the common data collection workflow.

```bash
python src/main.py --state ID --start-year 2022 --end-year 2023 --data-dir ./my_data
--state: Specify the state abbreviation (default: ID).
--start-year, --end-year: Define the range of legislative years to process.
--data-dir: (Optional) Override the default ./data directory.
--skip-api: Skip all LegiScan API data collection steps.
--skip-finance: Skip campaign finance scraping (scrape_finance_idaho.py).
--skip-matching: Skip matching finance data to legislators (match_finance_to_leg.py).
--monitor-only: Run only the website structure monitor (monitor_idaho_structure.py) and exit.
```

Running Individual Modules (for testing/specific tasks):
You can run modules directly using python -m src.<module_name>. Refer to the --help argument for each script for specific options.

## LegiScan Collection & Committee Scraping/Matching: ##

```
python -m src.data_collection --state ID --start-year 2023 --end-year 2023 --run bills
python -m src.data_collection --state ID --run scrape_members
python -m src.data_collection --state ID --run match_members
See `data_collection.py --help` for all `--run` options
```

## Finance Scraping: ##

```
python -m src.scrape_finance_idaho --start-year 2023 --end-year 2023
```

## Finance Matching: ##

```
# Ensure finance_*.csv and legislators_*.csv exist in processed/
python -m src.match_finance_to_leg data/processed/finance_idaho_consolidated_....csv data/processed/legislators_ID.csv data/processed/finance_idaho_matched_....csv
```

## Website Monitoring ##

```
python -m src.monitor_idaho_structure
```

## (Planned) Preprocessing & Modeling: ##

Usage instructions will be added once data_preprocessing.py and xgboost_model.py are implemented.

### Current Status & Roadmap ###

The project is currently in the data collection and initial processing phase.

## Completed: ## Core LegiScan API integration, Idaho committee membership scraping and basic matching. Basic project structure and utilities.

## In Progress / Needs Validation: ## Idaho campaign finance scraping and matching (requires significant validation and potential refinement due to website fragility).

## Planned: ##
```
Demographics and Election History data collection.
Data preprocessing and robust feature engineering.
Predictive model (XGBoost) development, training, and interpretation.
Development of backend API and database.
Frontend UI for data exploration and visualization.
Chatbot integration.
Comprehensive testing and documentation.
Deployment strategy and implementation.
For a detailed breakdown of pending tasks, see the TODO.md file.
```

## Contributing ##

Contributions are welcome! Please feel free to open an issue to report bugs, suggest features, or discuss potential improvements. If you'd like to contribute code, please open an issue first to discuss the change and then submit a pull request.

Licensed under MIT License.


