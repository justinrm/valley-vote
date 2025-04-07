# Project TODO List: Valley Vote (Legislative Vote Prediction & Analysis Platform)

This document tracks the tasks required to build and improve the Valley Vote platform, which aims to predict legislative votes and provide data analysis capabilities, including chatbot interaction. It synthesizes information from project code, previous TODOs, and incorporates the goal of building a user-facing platform.

**Key:**
*   `[x]` = Completed
*   `[~]` = In Progress / Partially Implemented / Needs Validation
*   `[ ]` = Not Started

## Phase 1: Core Data Acquisition & Initial Processing

*Focuses on gathering data from various sources and performing initial structuring and cleaning.*

-   **LegiScan API Data (`data_collection.py`):**
    -   [x] Implement core API fetching framework (Sessions, Legislators, Committees, Bills, Votes, Sponsors).
    -   [x] Implement robust retry logic (`tenacity`), error handling, and logging.
    -   [x] Implement data structuring and saving (Raw JSONs per item/session).
    -   [x] Implement basic consolidation of yearly API data into session-level JSONs.
    -   [x] Implement consolidation of yearly session JSONs into yearly CSV/JSON (`consolidate_yearly_data`).
    -   [ ] (Optional/Optimization) Implement `getMasterListRaw` and `change_hash` comparison for more efficient bill updates (currently fetches all via `getBill`).
    -   [ ] (Feature Expansion) Fetch full Bill Text, Amendments, Supplements via API (`getText`, `getAmendment`, `getSupplement`) and implement parsing/storage if needed for NLP features or chatbot context.

-   **Idaho Committee Membership Scraping (`data_collection.py`):**
    -   [x] Implement web scraper for current year committee memberships (ID House/Senate).
    -   [x] Implement fuzzy name matching (`thefuzz`) to link scraped member names to LegiScan `legislator_id`.
    -   [x] Save raw scraped HTML (`artifacts/monitor`).
    -   [x] Save raw parsed committee/member list (JSON).
    -   [x] Save matched membership results (JSON/CSV).
    -   [x] Implement consolidation of yearly matched membership data.
    -   [~] **Monitor & Maintain:** Regularly check Idaho Legislature website structure and update scraper (`monitor_idaho_structure.py`). Needs automated checks or scheduled runs.

-   **Idaho Campaign Finance Data (`scrape_finance_idaho.py` & related):**
    -   [x] **Initial Setup:**
        -   [x] Added Playwright dependency (`requirements.txt`).
        -   [x] Set up virtual environment (`venv`).
        -   [x] Created basic finance scraper structure (`scrape_finance_idaho.py`).
        -   [x] Created validation/test script (`test_finance_scraper.py`).
    -   [~] **Refactor & Validate Scraper (`test_finance_scraper.py`, `scrape_finance_idaho.py`):** Use Playwright for Sunshine Portal interaction.
        -   [x] Refactor `--inspect-form` to use Playwright, identify initial elements.
        -   [x] Refactor `--inspect-results` to use Playwright.
            -   [x] Implement robust selector for name/committee input (`#panel-campaigns-content input[role="combobox"]...`).
            -   [x] Implement focus logic for hidden input (`.focus()` with JS fallback).
            -   [x] Implement typing/filling name input.
            -   [ ] (Minor) Handle dropdown option selection gracefully (currently skips if not found).
            -   [x] Implement locating and filling date inputs.
            -   [x] Implement locating and clicking search button.
            -   [x] Implement waiting for/detecting results area.
            -   [x] Implement finding results items count.
            -   [~] **Implement Finding Export Link/Button:** Timeout currently occurs. **(Next Step)**
        -   [ ] Implement `--test-search` function using validated Playwright logic.
        -   [ ] Refactor `scrape_finance_idaho.py` main scraping loop to use the validated Playwright interaction logic from `test_finance_scraper.py`.
        -   [ ] Implement robust error handling and retry logic within Playwright interactions.
    -   [~] **Refine CSV Parsing (`validate_csv_parsing.py`, `scrape_finance_idaho.py`):** Standardize column mapping (`FINANCE_COLUMN_MAPS` in `data_collection.py`), improve data cleaning (amounts, dates), type conversion based on actual Idaho CSV format. Handle potential Excel files if CSV fails.
    -   [~] **Develop Robust Matching (`match_finance_to_leg.py`):**
        -   [x] Initial fuzzy matching logic implemented (`thefuzz`).
        -   [x] Centralized `clean_name` utility in `src/utils.py`.
        -   [ ] **Refine Strategy:** Improve matching beyond simple name fuzziness (consider committee indicators, election year/office, rules, manual review).
        -   [ ] **Validate Matches:** Perform spot-checks/build validation set.
    -   [ ] Extract and standardize donor details (name, address, employer, occupation) for categorization/analysis.
    -   [ ] (Optional) Extract and standardize expenditure data more thoroughly.

-   **District Demographics Data (`process_demographics_idaho.py` - Planned Script):**
    -   [ ] **Identify Data Sources:** Pinpoint specific Census ACS tables and TIGER/Line shapefiles for relevant ID legislative districts/years.
    -   [ ] **Implement Data Download:** Automate fetching Census data and TIGER files.
    -   [ ] **Implement Geospatial Processing (`geopandas`):** Load shapefiles, load Census data, perform spatial join, aggregate demographics per district.
    -   [ ] **Establish Linking:** Ensure consistent key (`district_id`, `geoid`).
    -   [ ] **Save Processed Data:** Output CSV mapping district identifier to features.

-   **Election History Data (`parse_elections_idaho.py` - Planned Script):**
    -   [ ] **Identify Data Sources:** Locate official historical election results for ID legislature (PDF, Excel, CSV?).
    -   [ ] **Implement Data Download/Access:** Obtain result files.
    -   [ ] **Implement Parsing Logic:** Develop robust parsers. Extract candidate, party, district, year, votes.
    -   [ ] **Calculate Metrics:** Margin of victory, vote share, competitiveness.
    -   [ ] **Develop Robust Matching:** Match election candidates to LegiScan `legislator_id`.
    -   [ ] **Save Processed Data:** Output CSV linked to `legislator_id`.

-   **General Acquisition Improvements:**
    -   [ ] Implement caching for API calls and web requests (`requests-cache`).
    -   [ ] Add configuration options for state-specific parameters to facilitate expansion.

## Phase 2: Data Preprocessing & Feature Engineering

*Consolidating, cleaning, merging data, and creating predictive features. Requires `data_preprocessing.py` (Planned Script).*

-   [ ] **Create `data_preprocessing.py` script.**
-   [ ] **Implement Master Data Loading:** Load all relevant processed CSVs.
-   [ ] **Implement Robust Data Linking & Merging:** Finalize/apply matching, verify keys, join logically.
-   [ ] **Implement Data Cleaning & Preparation:** Handle missing values, standardize formats, filter votes, address inconsistencies.
-   [ ] **Implement Feature Engineering:** Legislator features (seniority, party, influence, etc.), Bill features (subject, complexity, etc.), Committee features, External Context (Demographics, Finance, Elections), Interaction features.
-   [ ] **Generate Final Feature Matrix:** Create dataset (`voting_data.csv`) with target and features.

## Phase 3: Predictive Modeling (XGBoost Focus)

*Building, training, tuning, and interpreting the vote prediction model. Requires `xgboost_model.py` (Planned Script).*

-   [ ] **Create `xgboost_model.py` script** (or notebook).
-   [ ] Load feature matrix.
-   [ ] **Implement Data Splitting:** Time-based, `GroupKFold`, etc.
-   [ ] **Handle Features for XGBoost:** Encoding, scaling (if needed).
-   [ ] **Implement XGBoost Model Training:** `.fit()`.
-   [ ] **Perform Hyperparameter Tuning:** `GridSearchCV`/`RandomizedSearchCV`/`Optuna` with CV.
-   [ ] **Save Trained Model:** Persist artifact.
-   [ ] **Implement Feature Importance Analysis:** XGBoost `feature_importances_`, **SHAP** values.
-   [ ] (Optional) Train baseline models.
-   [ ] (Optional) Experiment with other models.

## Phase 4: Model Evaluation & Basic Deployment Prep

*Assessing model performance and preparing for potential use.*

-   [ ] **Implement Model Evaluation:** Evaluate on test set (Accuracy, Precision, Recall, F1, ROC-AUC, Confusion Matrix). Analyze subgroups. Plot curves.
-   [ ] **Refine Cross-Validation:** Verify CV strategy.
-   [ ] **Interpret Results:** Analyze metrics and SHAP.
-   [ ] (Recommended) Set up experiment tracking (MLflow, W&B).
-   [ ] **Design Model Retraining Strategy:** Plan for updates.
-   [ ] (Optional/Initial) Develop basic prediction function.

## Phase 5: Backend API & Data Serving

*Building the infrastructure to serve data and model predictions for the platform.*

-   [ ] **Design Database Schema:** Choose DB, design tables.
-   [ ] **Implement Data Loading Pipeline:** Scripts to load processed data into DB.
-   [ ] **Develop Backend API:** Choose framework (Flask/FastAPI). Implement endpoints for data, predictions, aggregations. Add docs.

## Phase 6: Frontend Development & Visualization

*Building the user interface for data exploration and interaction.*

-   [ ] **Choose Frontend Framework:** React/Vue/Streamlit.
-   [ ] **Design User Interface:** Layout, navigation, components.
-   [ ] **Implement UI Components.**
-   [ ] **Integrate with Backend API.**
-   [ ] **Implement Data Visualizations:** D3/Plotly/Chart.js. Voting patterns, bill outcomes, finance, demographics, SHAP plots.
-   [ ] Ensure responsive design.

## Phase 7: Chatbot Integration

*Adding conversational AI capabilities to query and understand the data.*

-   [ ] **Select Language Model (LLM):** GPT-4/Claude/Llama etc.
-   [ ] **Design Chatbot Interaction Flow.**
-   [ ] **Develop Prompt Engineering Strategy.**
-   [ ] **Implement Backend Chat Logic:** LLM SDK, parse query, fetch data, structure context, handle response.
-   [ ] (Optional/Advanced) Implement RAG (Vector DB/embeddings).
-   [ ] **Integrate Chatbot into Frontend.**

## Phase 8: Deployment & Maintenance

*Making the platform available and ensuring its ongoing operation.*

-   [ ] **Choose Deployment Strategy:** Hosting, containerization (Docker).
-   [ ] **Implement CI/CD Pipeline:** GitHub Actions, etc.
-   [ ] **Deploy Application Stack.**
-   [ ] **Implement Platform Monitoring:** Logging, error tracking, performance.
-   [ ] **Monitor Data Sources:** Maintain scrapers/parsers.
-   [ ] **Schedule Data Updates:** Automate pipelines.
-   [ ] **Gather User Feedback.**
-   [ ] **Iterate and Improve.**

## Phase 9: Documentation & Testing

*Ensuring project quality, usability, and sustainability (Ongoing).*

-   [x] Create initial `data_collection_readme.md`.
-   [x] Create initial project `TODO.md`.
-   [x] Update `README.md` with initial status/structure.
-   [x] Update `TODO.md` based on current progress (this update).
-   [x] Create validation scripts documentation (`README_VALIDATION.md`).
-   [x] Create `CHANGELOG.md`.
-   [ ] **Write/Update Script/Module Documentation:** Add detailed READMEs/docstrings.
-   [ ] **Document Data Schema:** `docs/data_schema.md`.
-   [ ] **Document Feature Engineering:** `docs/feature_engineering.md`.
-   [ ] **Document API Endpoints:** Generate/maintain API docs.
-   [ ] **Document Deployment & Setup.**
-   [ ] **Add Code Comments & Docstrings.**
-   [ ] **Implement Unit/Integration Tests (`pytest`):** API parsing, Scraper parsing, Fuzzy matching, Data merging, Feature calcs, API endpoints, Chatbot logic, Frontend components.
-   [x] **Maintain `requirements.txt`:** Dependencies updated.
-   [ ] **Refactor Code:** Periodically review/improve.
-   [ ] **Address TODOs/FIXMEs in Code.**
