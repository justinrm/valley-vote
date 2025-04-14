# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Efficient fetching of LegiScan bill data using `getMasterListRaw` and `change_hash` comparison, reducing redundant API calls (`src/data_collection.py`).
- Ability to fetch full bill texts, amendments, and supplements via LegiScan API using `--fetch-*` flags (`src/data_collection.py`).
- Initial `CHANGELOG.md` file to track project changes.
- Script (`src/parse_finance_idaho_manual.py`) to parse, combine, and clean manually downloaded Idaho campaign finance CSV files.

### Changed
- Refined `README.md` with improved structure, clarity, accuracy, and reflection of current project status (LegiScan optimization, paused finance scraping).
- Updated `docs/todo.md` to mark LegiScan optimization as complete, indicate campaign finance scraping as paused, and add tasks for processing manually acquired finance data.
- Paused automated scraping of Idaho campaign finance data via Playwright (`src/scrape_finance_idaho.py`, `src/test_finance_scraper.py`) due to challenges with the target website. Project will proceed using manually acquired data for this source.
- Refactored `tests/test_finance_scraper.py` to remove outdated test functions and imports, improved test logic for `download_and_extract_finance_data` to test actual function behavior rather than using mocks, and removed related CLI arguments.
- Refactored LegiScan bill data collection in `src/data_collection.py` to use the Bulk Dataset API (`getDatasetList`, `getDataset`) instead of `getMasterListRaw`/`getBill`. This significantly reduces API call volume for fetching bill data.

### Fixed
- N/A

### Documentation
- Updated `README.md`, `docs/todo.md`, `docs/readme_data_collection.md`, `docs/data_schema.md`, and `CHANGELOG.md` to reflect:
    - Completion of LegiScan efficient fetching and full document fetching features.
    - Paused status of automated campaign finance scraping and shift to manual data.
    - Updated project structure, data schemas, usage instructions, and roadmap.
- Comprehensively revised `docs/todo.md` to better reflect current project status with more granular progress tracking and additional tasks for data quality and model documentation.
- Enhanced `docs/feature_engineering.md` with status indicators for each feature (Implemented/In Progress/Planned), added new features that match actual implementation, and included sections on limitations and future enhancements.
- Fixed duplicate information in `src/config.py` by removing redundant definitions.
- Corrected imports and column mapping references in validation scripts to ensure consistency with changes in the configuration.

### Removed
- N/A 