# Data Collection Modules

This document provides details on the data collection modules in the Valley Vote project, which are responsible for gathering various types of legislative data.

## Core Data Collection (`data_collection.py`)

This module handles the core data collection from the LegiScan API and scraping of committee memberships.

### Components

1. **API Integration**
   - LegiScan API client functions (`legiscan_client.py`)
   - Session management and retry logic (`tenacity`)
   - Bulk Dataset API handling for efficient data collection (`legiscan_dataset_handler.py`)

2. **Data Collection Functions**
   - Session data (`collect_sessions`)
   - Legislator information (`collect_legislators`)
   - Committee details (`collect_committees`)
   - Bill information via Bulk Dataset API (`collect_bills_votes_sponsors`)
   - Vote details (`collect_vote_data`)
   - Bill sponsorship data (`collect_sponsor_data`)
   - Bill texts, amendments, and supplements (`collect_bill_documents`)

3. **Committee Membership Scraping**
   - Idaho Legislature website scraping (`scrape_committee_members`)
   - Committee member name normalization and cleaning
   - Fuzzy matching to link scraped members to LegiScan legislator_id (`match_committee_members`)

4. **Data Consolidation**
   - Yearly data consolidation functions
   - Data structure standardization
   - CSV/JSON output generation

### Usage Examples

```python
# Collect session data for Idaho (ID) for 2023
python -m src.data_collection --state ID --start-year 2023 --end-year 2023 --run sessions

# Collect legislator data
python -m src.data_collection --state ID --start-year 2023 --end-year 2023 --run legislators

# Collect committee data
python -m src.data_collection --state ID --start-year 2023 --end-year 2023 --run committees

# Collect bill data using Bulk Dataset API (includes votes and sponsors)
python -m src.data_collection --state ID --start-year 2023 --end-year 2023 --run bills

# Scrape committee memberships from the Idaho Legislature website
python -m src.data_collection --state ID --run scrape_members

# Match scraped committee members to LegiScan legislator IDs
python -m src.data_collection --state ID --run match_members

# Collect bill texts, amendments, and supplements
python -m src.data_collection --state ID --start-year 2023 --end-year 2023 --run bills --fetch-texts --fetch-amendments --fetch-supplements
```

## Finance Data Collection (`finance_collection.py`)

This module manages campaign finance data collection through an API.

### Components

1. **API Integration**
   - Finance API client with authentication
   - Request management and error handling
   - Rate limiting and retries

2. **Data Processing**
   - Candidate list retrieval
   - Contribution data collection
   - Data cleaning and standardization
   - Yearly data consolidation

3. **Fuzzy Matching**
   - Candidate name normalization
   - Matching candidates to legislators using fuzzy string matching
   - Confidence score calculation and threshold filtering

4. **Main Collection Pipeline**
   - Structured workflow for finance data collection
   - Configuration management
   - Progress tracking and logging

### Usage Examples

```python
# Collect finance data for Idaho (ID) for 2023
python -m src.finance_collection --state ID --start-year 2023 --end-year 2023

# Collect finance data with custom confidence threshold for matching
python -m src.finance_collection --state ID --start-year 2023 --end-year 2023 --match-threshold 80

# Collect finance data and skip the matching step
python -m src.finance_collection --state ID --start-year 2023 --end-year 2023 --skip-matching
```

## News Article Collection (`news_collection.py`)

This module collects news articles related to legislation using the News API.

### Components

1. **API Integration**
   - News API client with authentication
   - Request management and error handling
   - Rate limiting and retries

2. **Query Generation**
   - Intelligent search query formation based on bill information
   - Keyword extraction from bill titles and descriptions
   - Query optimization for relevant results

3. **Content Extraction**
   - Article URL processing
   - Full text extraction from article pages
   - Text cleaning and standardization
   - NLP techniques for improved text processing (NLTK)

4. **Data Organization**
   - Article metadata storage
   - Content-to-bill association
   - Consolidated datasets for analysis
   - Yearly aggregation

### Usage Examples

```python
# Collect news articles for Idaho (ID) legislation for 2023
python -m src.news_collection --state ID --start-year 2023 --end-year 2023

# Collect news articles with a specific query limit per bill
python -m src.news_collection --state ID --start-year 2023 --end-year 2023 --query-limit 5

# Collect news articles and skip full text extraction
python -m src.news_collection --state ID --start-year 2023 --end-year 2023 --skip-full-text
```

## Amendment Collection (`amendment_collection.py`)

This module specializes in collecting and analyzing bill amendments.

### Components

1. **Amendment Collection**
   - LegiScan API integration for amendment retrieval
   - Document parsing and content extraction
   - Storage of amendment documents and metadata

2. **Comparison Analysis**
   - Differential analysis between bill versions
   - Text extraction and normalization
   - Change detection and quantification
   - Semantic analysis of amendments

3. **Consolidated Analysis**
   - Amendment patterns across bills and sessions
   - Statistical analysis of amendment frequency and impact
   - Temporal tracking of amendments

### Usage Examples

```python
# Collect and analyze amendments for Idaho (ID) legislation for 2023
python -m src.amendment_collection --state ID --start-year 2023 --end-year 2023

# Collect amendments with a specific bill type filter
python -m src.amendment_collection --state ID --start-year 2023 --end-year 2023 --bill-type "SB"

# Perform detailed comparison analysis on amendments
python -m src.amendment_collection --state ID --start-year 2023 --end-year 2023 --detailed-comparison
```

## Manual Finance Data (`parse_finance_idaho_manual.py`)

This planned module will process manually acquired campaign finance data files.

### Components

1. **File Parsing**
   - CSV/Excel file reading capabilities
   - Handling of various file formats and encodings
   - Robust parsing with error handling

2. **Data Transformation**
   - Standardization of column names and formats
   - Data cleaning and normalization
   - Conversion to project-standard formats

3. **Integration with Existing Data**
   - Merging with API-collected finance data
   - Consistency checks and validation
   - Consolidated output generation

### Usage Examples

```python
# Process a manually acquired finance data file
python -m src.parse_finance_idaho_manual --input-file path/to/finance_data.csv

# Process with custom column mapping
python -m src.parse_finance_idaho_manual --input-file path/to/finance_data.csv --mapping-file path/to/mapping.json

# Process and merge with existing API data
python -m src.parse_finance_idaho_manual --input-file path/to/finance_data.csv --merge-api-data
```

## Main Orchestrator (`main.py`)

This module coordinates the various data collection modules, providing a unified interface.

### Components

1. **Command Line Interface**
   - Argument parsing and validation
   - Configuration management
   - Module selection and execution

2. **Workflow Orchestration**
   - Sequential execution of data collection steps
   - Dependency management between modules
   - Status tracking and reporting

3. **Error Handling and Logging**
   - Centralized error handling
   - Comprehensive logging
   - Progress reporting

### Usage Examples

```python
# Run all data collection modules for Idaho (ID) for 2023
python -m src.main --state ID --start-year 2023 --end-year 2023

# Run specific modules only
python -m src.main --state ID --start-year 2023 --end-year 2023 --skip-finance --skip-committees

# Run with full document collection
python -m src.main --state ID --start-year 2023 --end-year 2023 --fetch-texts --fetch-amendments --fetch-supplements
```

## Common Features Across Modules

All data collection modules share these common features:

1. **Robust Error Handling**
   - Exception catching and logging
   - Retry mechanisms for transient errors
   - Graceful degradation

2. **Comprehensive Logging**
   - Detailed logging of operations
   - Error tracking and reporting
   - Progress monitoring

3. **Consistent File Structure**
   - Standardized data storage paths
   - Organized by data type and year
   - Raw and processed data separation

4. **Efficient Processing**
   - Avoidance of redundant operations
   - Incremental updates
   - Hash-based change detection

5. **Configuration Management**
   - Environment variable integration
   - Command-line parameter handling
   - Default configuration with overrides 