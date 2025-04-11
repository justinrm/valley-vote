# Project TODO List: Valley Vote (Legislative Vote Prediction & Analysis Platform)

This document tracks the tasks required to build and improve the Valley Vote platform, which aims to predict legislative votes and provide data analysis capabilities, including chatbot interaction. It synthesizes information from project code, previous TODOs, and incorporates the goal of building a user-facing platform.

**Key:**
*   `[x]` = Completed
*   `[~]` = In Progress / Partially Implemented / Needs Validation
*   `[ ]` = Not Started
*   `[P]` = Paused / Blocked
*   `[R]` = Refactored / Replaced

## Phase 1: Core Data Acquisition & Initial Processing

*Focuses on gathering data from various sources and performing initial structuring and cleaning.*

-   **LegiScan API Data (`data_collection.py`):**
    -   [x] Implement core API fetching framework (Sessions, Legislators, Committees, Votes, Sponsors).
    -   [x] Implement robust retry logic (`tenacity`), error handling, and logging.
    -   [R] ~~Implement `getMasterListRaw` and `change_hash` comparison for efficient bill updates.~~ (Replaced by Bulk Dataset API)
    -   [x] Implement Bill collection via Bulk Dataset API (`getDatasetList`, `getDataset`, hash comparison).
    -   [x] Implement data structuring and saving (Raw JSONs per item/session).
    -   [x] Implement basic consolidation of yearly API data into session-level JSONs.
    -   [x] Implement consolidation of yearly session JSONs into yearly CSV/JSON (`consolidate_yearly_data`).
    -   [x] Fetch full Bill Text, Amendments, Supplements via API (`getText`, `getAmendment`, `getSupplement`) (Requires `--fetch-*` flags).

-   **Idaho Committee Membership Scraping (`data_collection.py`):**
    -   [x] Implement web scraper for current year committee memberships (ID House/Senate).
    -   [x] Implement fuzzy name matching (`thefuzz`) to link scraped member names to LegiScan `legislator_id`.
    -   [x] Save raw scraped HTML (`artifacts/monitor`).
    -   [x] Save raw parsed committee/member list (JSON).
    -   [x] Save matched membership results (JSON/CSV).
    -   [x] Implement consolidation of yearly matched membership data.
    -   [~] **Monitor & Maintain:** Regularly check Idaho Legislature website structure and update scraper (`monitor_idaho_structure.py`). Needs automated checks or scheduled runs.

-   **Idaho Campaign Finance Data:**
    -   [P] **Overall Status: Paused Automated Scraping** - Pivoting to manually acquired data via records request due to Sunshine Portal scraping challenges. Automated Playwright-based scraping logic (`scrape_finance_idaho.py`, `test_finance_scraper.py`) is retained for reference but is not currently active or maintained.
    -   [x] **Initial Setup (Playwright - for potential future use or reference):**
        -   [x] Added Playwright dependency (`requirements.txt`).
        -   [x] Set up virtual environment (`venv`).
        -   [x] Created basic finance scraper structure (`scrape_finance_idaho.py`).
        -   [x] Created validation/test script (`test_finance_scraper.py`).
    -   [P] **Refactor & Validate Scraper (Playwright - relevant if scraping is resumed):**
        -   [x] Refactor `--inspect-form` to use Playwright, identify initial elements.
        -   [x] Refactor `--inspect-results` to use Playwright.
            -   [x] Implement robust selector for name/committee input (`#panel-campaigns-content input[role="combobox"]...`).
            -   [x] Implement focus logic for hidden input (`.focus()` with JS fallback).
            -   [x] Implement typing/filling name input.
            -   [ ] (Minor) Handle dropdown option selection gracefully.
            -   [x] Implement locating and filling date inputs.
            -   [x] Implement locating and clicking search button.
            -   [x] Implement waiting for/detecting results area.
            -   [x] Implement finding results items count.
            -   [P] **Implement Finding Export Link/Button:** Timeout occurred during previous attempts.
        -   [P] Implement `--test-search` function using validated Playwright logic.
        -   [P] Refactor `scrape_finance_idaho.py` main scraping loop.
        -   [P] Implement robust error handling and retry logic within Playwright interactions.
    -   [ ] **Parse Manually Acquired Data:**
        -   [~] Develop parser script (`parse_finance_idaho_manual.py` or similar) to read received CSV/other format.
        -   [ ] Implement robust data cleaning (amounts, dates, names, addresses, etc.) based on actual data format.
        -   [ ] Standardize column names based on `config.py` maps or define new ones.
        -   [ ] Handle potential variations in file structure/format.
        -   [ ] Save cleaned/standardized data to `processed/` directory (e.g., CSV).
    -   [~] **Develop Robust Matching (`match_finance_to_leg.py`):**
        -   [x] Initial fuzzy matching logic implemented (`thefuzz`).
        -   [x] Centralized `clean_name` utility in `src/utils.py`.
        -   [~] **Refine Strategy:** Improve matching based on manual data format (consider committee indicators, election year/office, filer IDs, manual review steps). Needs validation with actual manual data.
        -   [ ] **Validate Matches:** Perform spot-checks/build validation set using the manual data.
    -   [ ] Extract and standardize donor details (name, address, employer, occupation) for categorization/analysis from manual data.
    -   [ ] (Optional) Extract and standardize expenditure data more thoroughly from manual data.

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

-   **General Acquisition Improvements & Refactoring:**
    -   [x] Refactor Bill collection to use LegiScan Bulk Dataset API (`getDatasetList`/`getDataset`).
    -   [ ] Implement caching for API calls and web requests (`requests-cache`).
    -   [ ] Add configuration options for state-specific parameters to facilitate expansion.
    -   [ ] Add more robust error logging and notification for failed data acquisition.
    -   [ ] Implement a data quality check system to verify completeness of acquired data.

## Phase 2: Data Preprocessing & Feature Engineering

*Consolidating, cleaning, merging data, and creating predictive features. Requires `data_preprocessing.py` (Implemented but needs enhancement).*

-   [x] **Create `data_preprocessing.py` script.**
-   [x] **Implement Master Data Loading:** Load all relevant processed CSVs.
-   [~] **Implement Robust Data Linking & Merging:** Finalize/apply matching, verify keys, join logically.
-   [~] **Implement Data Cleaning & Preparation:** Handle missing values, standardize formats, filter votes, address inconsistencies.
-   [~] **Implement Feature Engineering:** 
    -   [~] Legislator features (seniority, party, influence, etc.)
    -   [~] Bill features (subject, complexity, etc.)
    -   [~] Committee features
    -   [ ] External Context (Demographics, Finance, Elections)
    -   [ ] Interaction features
-   [~] **Generate Final Feature Matrix:** Create dataset (`voting_feature_matrix.csv`) with target and features.
-   [ ] **Validate Generated Features:** Ensure consistency, normality, and significance.
-   [ ] **Implement Feature Selection:** Identify and select most important features.
-   [ ] **Optimize Feature Computation:** Improve performance of feature engineering code.

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
-   [ ] (Optional) Experiment with other models (Random Forest, Neural Networks).
-   [ ] **Document Model Training Process:** Include parameterization, performance metrics, and key insights.

## Phase 4: Model Evaluation & Basic Deployment Prep

*Assessing model performance and preparing for potential use.*

-   [ ] **Implement Model Evaluation:** Evaluate on test set (Accuracy, Precision, Recall, F1, ROC-AUC, Confusion Matrix). Analyze subgroups. Plot curves.
-   [ ] **Refine Cross-Validation:** Verify CV strategy.
-   [ ] **Interpret Results:** Analyze metrics and SHAP.
-   [ ] (Recommended) Set up experiment tracking (MLflow, W&B).
-   [ ] **Design Model Retraining Strategy:** Plan for updates.
-   [ ] (Optional/Initial) Develop basic prediction function.
-   [ ] **Create Evaluation Report:** Document performance findings and limitations.
-   [ ] **Implement Model Versioning:** Track model versions and their performance.

## Phase 5: Backend API & Data Serving

*Building the infrastructure to serve data and model predictions for the platform.*

-   [ ] **Design Database Schema:** Choose DB, design tables.
-   [ ] **Implement Data Loading Pipeline:** Scripts to load processed data into DB.
-   [ ] **Develop Backend API:** Choose framework (Flask/FastAPI). Implement endpoints for data, predictions, aggregations. Add docs.
-   [ ] **Implement Authentication & Authorization:** Protect sensitive endpoints.
-   [ ] **Develop API Testing Suite:** Ensure API endpoints function as expected.
-   [ ] **Create API Documentation:** Use Swagger/OpenAPI for interactive docs.
-   [ ] **Implement Caching:** Optimize response times for common queries.

## Phase 6: Frontend Development & Visualization

*Building the user interface for data exploration and interaction.*

-   [ ] **Choose Frontend Framework:** React/Vue/Streamlit.
-   [ ] **Design User Interface:** Layout, navigation, components.
-   [ ] **Implement UI Components.**
-   [ ] **Integrate with Backend API.**
-   [ ] **Implement Data Visualizations:** D3/Plotly/Chart.js. Voting patterns, bill outcomes, finance, demographics, SHAP plots.
-   [ ] Ensure responsive design.
-   [ ] **Develop User Onboarding:** Create tutorials and help documentation.
-   [ ] **Implement User Settings:** Allow customization of views and analysis parameters.

## Phase 7: Chatbot Integration

*Adding conversational AI capabilities to query and understand the data.*

-   [ ] **Select Language Model (LLM):** GPT-4/Claude/Llama etc.
-   [ ] **Design Chatbot Interaction Flow.**
-   [ ] **Develop Prompt Engineering Strategy.**
-   [ ] **Implement Backend Chat Logic:** LLM SDK, parse query, fetch data, structure context, handle response.
-   [ ] (Optional/Advanced) Implement RAG (Vector DB/embeddings).
-   [ ] **Integrate Chatbot into Frontend.**
-   [ ] **Train Chatbot on Domain-Specific Data:** Improve accuracy for legislative terminology.
-   [ ] **Implement Feedback Loop:** Collect and utilize user feedback to improve chatbot responses.
-   [ ] **Create Fallback Mechanisms:** Handle cases where chatbot cannot provide an answer.

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
-   [ ] **Implement Backup and Recovery Procedures:** Ensure data safety.
-   [ ] **Plan for Scalability:** Prepare for increased usage and data volume.

## Phase 9: Documentation & Testing

*Ensuring project quality, usability, and sustainability (Ongoing).*

-   [x] Create initial `readme_data_collection.md`.
-   [x] Create initial project `TODO.md`.
-   [x] Update `README.md` with initial status/structure.
-   [x] Update `TODO.md` based on current progress (this update).
-   [x] Create validation scripts documentation (`README_VALIDATION.md`).
-   [x] Create `CHANGELOG.md`.
-   [~] **Write/Update Script/Module Documentation:**
    -   [~] `data_collection.py` - Documentation updated but needs enhancements
    -   [~] `scrape_finance_idaho.py` - Documented but paused
    -   [~] `data_preprocessing.py` - Basic documentation completed, needs updates with feature engineering details
    -   [ ] Other modules - Require documentation updates
-   [~] **Document Data Schema:** `docs/data_schema.md` - Initial version created, needs updates.
-   [~] **Document Feature Engineering:** `docs/feature_engineering.md` - Initial version created, needs validation against implemented features.
-   [ ] **Document API Endpoints:** Generate/maintain API docs.
-   [ ] **Document Deployment & Setup.**
-   [~] **Add Code Comments & Docstrings.**
-   [ ] **Implement Unit/Integration Tests (`pytest`):** API parsing, Scraper parsing, Fuzzy matching, Data merging, Feature calcs, API endpoints, Chatbot logic, Frontend components.
-   [x] **Maintain `requirements.txt`:** Dependencies updated.
-   [~] **Refactor Code:** Periodically review/improve (LegiScan Bulk API implemented for Bills).
    -   [~] Update `main.py` to use new `collect_bills_votes_sponsors` signature and manage `dataset_hashes`.
    -   [ ] Consider using Bulk Dataset API for Legislator/Person data (currently uses `getSessionPeople`).
    -   [ ] Consider using Bulk Dataset API for Vote/RollCall data (currently uses `getRollCall` individually).
-   [~] **Address TODOs/FIXMEs in Code.**
-   [ ] **Create User Guide:** Comprehensive documentation for end users.
-   [ ] **Implement Code Quality Checks:** Add linting, formatting, and type checking tools.
-   [ ] **Create Contributing Guidelines:** Instructions for contributors.

## Recent Updates (Last Updated: [Current Date])

-   Updated todo.md to reflect current project status
-   Updated LegiScan API section to mark Bulk Dataset API implementation as completed
-   Enhanced data preprocessing section with more specific tasks
-   Added new tasks for data quality checks and feature validation
-   Updated documentation section with current status
-   Marked Playwright-based scraping as paused
-   Added new tasks for model documentation and versioning
