# Valley Vote - Data Collection Module

## Overview

This document provides comprehensive documentation for the data collection component of the Valley Vote platform. The `data_collection.py` script serves as the foundation of our Idaho Legislative Vote Prediction system, designed to gather, process, and organize legislative data from the LegiScan API in preparation for machine learning model development.

## Purpose

The primary purpose of this module is to systematically collect five years (2020-2024) of Idaho legislative data required to train machine learning models that can predict how legislators will vote on bills. This module:

1. Retrieves raw data from the LegiScan API
2. Organizes it into a structured file hierarchy
3. Processes the data into formats optimized for analysis
4. Generates preliminary feature matrices for model training

## Data Sources

All data is sourced from the LegiScan API, a reputable provider of legislative information for U.S. states. The script collects:

- **Legislators**: Detailed profiles including party affiliation, district, role, and demographics
- **Bills**: Complete bill information including title, description, subjects, and status
- **Votes**: Individual voting records with standardized encodings
- **Committees**: Committee structures and memberships
- **Sponsorships**: Bill sponsorship relationships

## Requirements

- Python 3.8+
- LegiScan API key (configured in the script)
- Required packages:
  - requests
  - pandas
  - tenacity
  - tqdm
  - logging

Install dependencies using:
```
pip install requests pandas tenacity tqdm
```

## Directory Structure

The script maintains a specific directory structure for organized data management:

```
data/
├── raw/                      # Raw JSON data
│   ├── legislators/          # Individual legislator profiles
│   ├── bills/                # Bills organized by year
│   │   ├── 2020/
│   │   └── ...
│   ├── votes/                # Vote records organized by year
│   │   ├── 2020/
│   │   └── ...
│   ├── committees/           # Committee information by year
│   │   ├── 2020/
│   │   └── ...
│   ├── committee_memberships/ # Committee membership details
│   │   ├── 2020/
│   │   └── ...
│   └── sponsors/             # Bill sponsorship information
│       ├── 2020/
│       └── ...
├── processed/                # CSV files ready for analysis
    ├── legislators.csv
    ├── bills_2020.csv
    ├── bills_2021.csv
    ├── ...
    ├── votes_2020.csv
    ├── ...
    ├── committee_memberships.csv
    ├── sponsors_2020.csv
    ├── ...
    └── vote_features.csv     # Preliminary feature matrix
```

## Usage

### Basic Execution

Run the script from the command line:

```bash
python data_collection.py
```

The script will:
1. Connect to the LegiScan API
2. Retrieve data for Idaho legislative sessions from 2020-2024
3. Process and store the data in both raw and processed formats
4. Generate a preliminary feature matrix for model training

### Configuration Options

Edit the following constants at the top of the script to adjust its behavior:

- `API_KEY`: Your LegiScan API key
- `STATE`: State abbreviation (default: 'ID' for Idaho)
- `YEARS`: Year range to collect data for (default: 2020-2024)
- `RAW_DIR`: Directory for raw JSON data (default: 'data/raw/')
- `PROCESSED_DIR`: Directory for processed CSV files (default: 'data/processed/')
- `MAX_RETRIES`: Maximum number of API request retries (default: 5)
- `DEFAULT_WAIT`: Base wait time between API calls in seconds (default: 1)

## Key Features

### Comprehensive Data Collection

The script collects a rich set of data points essential for effective vote prediction:

- **Detailed Legislator Profiles**: Beyond basic information like name and party, the script captures role, district, and demographic data (gender, ethnicity, religion) when available.

- **Complete Bill Context**: Collects not just bill titles but also descriptions, subjects/topics, status, and URLs, providing context for vote analysis.

- **Committee Networks**: Gathers committee structures and memberships to enable network analysis of legislative relationships.

- **Bill Sponsorships**: Captures primary and co-sponsorship data as indicators of legislative priorities and alliances.

- **Standardized Vote Values**: Encodes votes numerically (1=yea, 0=nay, -1=abstain, -2=absent) for consistent analysis.

### Technical Robustness

The script incorporates several technical features to ensure reliable data collection:

- **Intelligent Retry Logic**: Uses the `tenacity` library to implement exponential backoff for API failures.

- **Rate Limit Management**: Varies request timing and detects rate limit responses to avoid API restrictions.

- **Comprehensive Logging**: Maintains detailed logs of all operations for debugging and audit purposes.

- **Progress Visualization**: Uses `tqdm` progress bars to provide visual feedback during long-running operations.

- **Error Resilience**: Continues processing when individual records fail, ensuring maximum data collection.

### Data Processing Features

The script goes beyond data collection to prepare for analysis:

- **Standardized CSV Generation**: Converts raw JSON to analysis-ready CSV files with consistent column structures.

- **Feature Engineering**: Calculates derived features such as party voting patterns and alignment metrics.

- **Data Summarization**: Generates statistical summaries of the collected data, including vote distributions and party representation.

## Advanced Usage

### Incremental Updates

To update the dataset with new data for a specific year only:

1. Modify the `YEARS` constant to include only the target year
2. Run the script
3. Merge the new processed files with existing ones

### Feature Matrix Generation

The script automatically generates `vote_features.csv`, which includes:

- Basic legislator features (party, district, role)
- Vote values (1 for yea, 0 for nay)
- Party voting patterns (average vote by party on each bill)
- Individual alignment metrics (agreement with party average)

This serves as a starting point for more sophisticated feature engineering.

## Technical Details

### Vote Value Encoding

The script standardizes vote values according to the following scheme:

- `1`: Affirmative votes (yea, aye, yes)
- `0`: Negative votes (nay, no)
- `-1`: Present but not voting (abstain, present)
- `-2`: Not present (absent, not voting)
- `-9`: Unknown or other vote values

### API Rate Limiting Strategy

To avoid triggering LegiScan's rate limits, the script:

1. Implements a base delay between requests (1 second by default)
2. Adds random jitter (0.1-0.5 seconds) to vary request timing
3. Detects 429 (Too Many Requests) responses
4. Implements exponential backoff when rate limits are encountered

### Error Handling Approach

The script employs a multi-layered error handling strategy:

1. Function-level try/except blocks capture and log specific errors
2. The `tenacity` retry mechanism handles transient network and API issues
3. Process-wide exception handling ensures graceful failure
4. Comprehensive logging provides troubleshooting context

## Design Decisions

### Raw and Processed Data Separation

The dual storage approach (raw JSON + processed CSV) serves several purposes:

1. **Data Integrity**: Original API responses are preserved for reference
2. **Troubleshooting**: Raw data can be examined when processing issues occur
3. **Flexibility**: New features can be derived from raw data without re-fetching
4. **Efficiency**: Processed CSVs are optimized for analysis workflows

### Feature Matrix Generation

The automatic generation of a preliminary feature matrix:

1. Provides immediate value for exploratory analysis
2. Serves as a template for more sophisticated feature engineering
3. Validates the completeness and usability of collected data
4. Accelerates the model development workflow

### Standardized Vote Encoding

The numerical encoding of votes facilitates:

1. Consistent treatment across varying vote terminology (yea/aye/yes)
2. Clear differentiation between negative votes and non-votes
3. Simplified model development (classification target is 1 or 0)
4. Potential for treating abstentions and absences as special cases

## Performance Considerations

### API Efficiency

The script optimizes API usage by:

1. Collecting core data (legislators, sessions) once rather than repeatedly
2. Using appropriate bulk endpoints where available
3. Implementing intelligent waiting between requests
4. Storing raw data to minimize redundant API calls

### Processing Efficiency

For large datasets, consider:

1. Running the script on a machine with adequate memory (8GB+ recommended)
2. Allocating sufficient disk space (approximately 1GB for 5 years of Idaho data)
3. Expecting runtime of 1-3 hours depending on the volume of legislative activity

## Troubleshooting

### Common Issues

1. **API Key Errors**: Ensure your LegiScan API key is valid and has not reached usage limits
2. **HTTP Timeouts**: Network issues may cause timeouts; the retry logic will handle most cases
3. **Disk Space**: Ensure sufficient disk space for both raw and processed data
4. **Memory Usage**: Processing large datasets may require significant memory

### Logging

The script generates a detailed log file (`data_collection.log`) containing:
- Information messages tracking progress
- Warning messages for non-fatal issues
- Error messages for failures
- Timestamps for all events

Review this log file to diagnose issues.

## Future Enhancements

The data collection module could be extended in several ways:

1. **Parallel Processing**: Implement multi-threading for concurrent API requests
2. **Incremental Updates**: Add smart detection of already-collected data
3. **Text Analysis**: Extract features from bill text using NLP techniques
4. **Historical Expansion**: Extend data collection to earlier years for trend analysis
5. **Cross-State Comparison**: Add support for collecting data from neighboring states

## Conclusion

The Valley Vote data collection module provides a robust foundation for the legislative vote prediction platform. By gathering comprehensive, well-structured data, it enables the development of accurate and interpretable machine learning models to predict legislative behavior in Idaho.
