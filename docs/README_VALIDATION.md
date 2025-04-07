# Idaho SOS Sunshine Portal Scraper Validation

This directory contains scripts to help validate and refine the Idaho SOS Sunshine Portal scraper. These scripts are designed to help test and improve the scraper's functionality, particularly focusing on:

1. Form field validation and search functionality
2. Link finding and extraction
3. CSV parsing and data standardization

## Validation Scripts

### 1. `test_finance_scraper.py`

This script helps validate the search functionality and form field handling.

**Usage:**
```bash
# Inspect form fields on the search page
python src/test_finance_scraper.py --inspect-form

# Inspect search results page
python src/test_finance_scraper.py --inspect-results

# Test search functionality with sample data
python src/test_finance_scraper.py --test-search

# Test a specific case
python src/test_finance_scraper.py --test-case "John Smith" 2022 contributions

# Override data directory
python src/test_finance_scraper.py --test-search --data-dir /path/to/data
```

**Features:**
- Inspects form fields to identify correct field names
- Tests search functionality with sample data
- Logs detailed information about the search process
- Saves debug information for further inspection

### 2. `validate_csv_parsing.py`

This script helps validate and refine CSV parsing functionality.

**Usage:**
```bash
# Basic CSV parsing
python src/validate_csv_parsing.py path/to/file.csv

# Specify data type
python src/validate_csv_parsing.py path/to/file.csv --data-type contributions

# Specify encoding
python src/validate_csv_parsing.py path/to/file.csv --encoding utf-8

# Suggest column mapping
python src/validate_csv_parsing.py path/to/file.csv --suggest-mapping

# Override data directory
python src/validate_csv_parsing.py path/to/file.csv --data-dir /path/to/data
```

**Features:**
- Detects file encoding
- Tries multiple encodings if detection fails
- Analyzes CSV structure (columns, data types, etc.)
- Tests column mapping
- Suggests column mappings based on content
- Saves suggested mappings to JSON files

### 3. `validate_link_finding.py`

This script helps validate and refine link finding functionality.

**Usage:**
```bash
# Basic link finding
python src/validate_link_finding.py --name "John Smith" --year 2022

# Specify data type
python src/validate_link_finding.py --name "John Smith" --year 2022 --data-type expenditures

# Override data directory
python src/validate_link_finding.py --name "John Smith" --year 2022 --data-dir /path/to/data
```

**Features:**
- Inspects page structure
- Finds all possible download links
- Tests link finding strategies
- Saves debug information for further inspection

## Validation Process

To thoroughly validate the scraper, follow these steps:

1. **Form Field Validation:**
   ```bash
   python src/test_finance_scraper.py --inspect-form
   ```
   This will help identify the correct form field names and structure.

2. **Link Finding Validation:**
   ```bash
   python src/validate_link_finding.py --name "John Smith" --year 2022
   ```
   This will help identify the correct way to find download links.

3. **CSV Parsing Validation:**
   ```bash
   python src/validate_csv_parsing.py path/to/downloaded.csv --suggest-mapping
   ```
   This will help identify the correct column mappings and data types.

4. **End-to-End Testing:**
   ```bash
   python src/test_finance_scraper.py --test-search
   ```
   This will test the entire scraping process with sample data.

## Debug Information

All validation scripts save debug information to the `data/artifacts/debug` directory. This information can be used to:

- Inspect HTML structure
- Identify form fields
- Find download links
- Analyze CSV structure
- Suggest column mappings

## Logging

All validation scripts log detailed information to log files in the `data/log` directory:

- `test_finance_scraper.log`
- `validate_csv_parsing.log`
- `validate_link_finding.log`

These logs contain detailed information about the validation process, including:

- Form field inspection
- Link finding attempts
- CSV parsing attempts
- Column mapping suggestions
- Error messages and warnings

## Troubleshooting

If you encounter issues with the validation scripts:

1. Check the log files for detailed error messages
2. Inspect the debug information saved to the `data/artifacts/debug` directory
3. Try different encodings for CSV parsing
4. Try different form field names for search
5. Try different link finding strategies

## Contributing

When contributing to the scraper:

1. Run the validation scripts before making changes
2. Document any changes to form field names or link finding strategies
3. Update column mappings if necessary
4. Test with real data from the Idaho SOS Sunshine Portal
5. Update the validation scripts if necessary 