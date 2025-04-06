# Valley Vote API Documentation

This document describes the key functions and classes in the Valley Vote codebase.

## Data Collection Module

### LegiScan API Functions

#### fetch_api_data
```python
def fetch_api_data(operation: str, params: Dict, wait_time: Optional[float] = None) -> Optional[Dict]
```
Fetches data from the LegiScan API with robust error handling.

**Arguments:**
- `operation`: API operation name (e.g., 'getSessionList', 'getBill')
- `params`: Dictionary of API parameters (excluding key and op)
- `wait_time`: Optional override for wait time between calls

**Returns:**
- Dictionary containing API response data, or None on failure

**Raises:**
- `APIRateLimitError`: If rate limited (HTTP 429)
- `APIResourceNotFoundError`: If resource not found
- `requests.exceptions.RequestException`: For other network errors

#### get_session_list
```python
def get_session_list(state: str, years: List[int]) -> List[Dict]
```
Gets list of legislative sessions for a state and years.

**Arguments:**
- `state`: State abbreviation (e.g., 'ID')
- `years`: List of years to get sessions for

**Returns:**
- List of session dictionaries, sorted by year (descending)

### Web Scraping Functions

#### parse_committee_name
```python
def parse_committee_name(committee_name: str) -> Optional[str]
```
Extracts potential candidate name from committee name.

**Arguments:**
- `committee_name`: Raw committee name string

**Returns:**
- Extracted candidate name or None if no valid name found

#### check_page_structure
```python
def check_page_structure(name: str, url: str) -> bool
```
Checks if a webpage's structure matches expected patterns.

**Arguments:**
- `name`: Name of the page being checked
- `url`: URL to check

**Returns:**
- True if structure is valid, False otherwise

## Data Processing Module

### Feature Engineering Functions

#### calculate_seniority
```python
def calculate_seniority(legislator_sessions: pd.DataFrame) -> float
```
Calculates legislator's seniority based on sessions served.

**Arguments:**
- `legislator_sessions`: DataFrame of legislator's session history

**Returns:**
- Seniority score (years served)

#### calculate_party_loyalty
```python
def calculate_party_loyalty(legislator_votes: pd.DataFrame, party_majority_votes: pd.DataFrame) -> float
```
Calculates percentage of votes aligned with party majority.

**Arguments:**
- `legislator_votes`: DataFrame of legislator's votes
- `party_majority_votes`: DataFrame of party majority positions

**Returns:**
- Party loyalty score (0-100)

#### calculate_influence
```python
def calculate_influence(
    legislator_data: Dict,
    bills_sponsored: pd.DataFrame,
    committee_roles: pd.DataFrame
) -> float
```
Calculates composite influence score for a legislator.

**Arguments:**
- `legislator_data`: Basic legislator info
- `bills_sponsored`: Bills sponsored by legislator
- `committee_roles`: Committee memberships

**Returns:**
- Influence score (0-100)

### Data Matching Functions

#### match_finance_to_legislators
```python
def match_finance_to_legislators(
    finance_file: Path,
    legislators_file: Path,
    output_file: Path,
    threshold: int = 88
) -> None
```
Matches finance records to legislators using fuzzy matching.

**Arguments:**
- `finance_file`: Path to finance data CSV
- `legislators_file`: Path to legislators CSV
- `output_file`: Path for output matched CSV
- `threshold`: Matching score threshold (0-100)

## Utility Functions

### File Operations

#### save_json
```python
def save_json(data: Any, path: Union[str, Path]) -> bool
```
Saves data as JSON file, creating directories if needed.

**Arguments:**
- `data`: Data to save
- `path`: Path to save to

**Returns:**
- True if successful, False otherwise

#### convert_to_csv
```python
def convert_to_csv(
    data: List[Dict],
    csv_path: Union[str, Path],
    columns: Optional[List[str]] = None
) -> int
```
Converts list of dictionaries to CSV with specified columns.

**Arguments:**
- `data`: List of dictionaries to convert
- `csv_path`: Path to save CSV
- `columns`: Optional list of columns to include/order

**Returns:**
- Number of rows written

### Logging

#### setup_logging
```python
def setup_logging(log_file: str, mode: str = 'w') -> logging.Logger
```
Configures logging with consistent format.

**Arguments:**
- `log_file`: Name of log file
- `mode`: File mode ('w' for overwrite, 'a' for append)

**Returns:**
- Configured logger instance

### Path Management

#### setup_project_paths
```python
def setup_project_paths(base_dir: Optional[Union[str, Path]] = None) -> Dict[str, Path]
```
Sets up project directory structure.

**Arguments:**
- `base_dir`: Optional override for base data directory

**Returns:**
- Dictionary mapping directory names to Path objects

## Error Classes

### APIRateLimitError
Custom exception for API rate limiting (HTTP 429).

### APIResourceNotFoundError
Custom exception for resources not found.

### NoDataFound
Custom exception for when no finance data is found.

## Configuration

### Environment Variables
- `LEGISCAN_API_KEY`: Required API key for LegiScan

### Constants
- `DEFAULT_MATCH_THRESHOLD`: Default threshold for fuzzy matching (88)
- `DEFAULT_WAIT`: Default wait time between API calls (1.5s)
- `MIN_EXPECTED_HEADINGS`: Minimum expected committee headings (5)

## Usage Examples

### Collecting Legislative Data
```python
from src.data_collection import collect_legislative_data
from src.utils import setup_project_paths

paths = setup_project_paths()
collect_legislative_data('ID', 2023, 2024, paths)
```

### Matching Finance Data
```python
from src.match_finance_to_leg import match_finance_to_legislators
from pathlib import Path

match_finance_to_legislators(
    finance_file=Path('data/processed/finance_idaho_contributions_2023.csv'),
    legislators_file=Path('data/processed/legislators_ID.csv'),
    output_file=Path('data/processed/finance_idaho_matched_2023.csv'),
    threshold=90
)
```

### Monitoring Website Structure
```python
from src.monitor_idaho_structure import check_page_structure

status = check_page_structure(
    'House Committees',
    'https://legislature.idaho.gov/house/committees/'
)
print('Structure OK' if status else 'Structure Changed')
``` 