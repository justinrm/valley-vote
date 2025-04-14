# Validation Scripts for Valley Vote Data Collection

This document details the validation scripts for the Valley Vote data collection modules, particularly for the Idaho SOS Sunshine Portal finance data scraper.

## Validation Scripts

The following scripts are available for validation:

### `test_finance_scraper.py`

Validates the search functionality and form field handling of the finance scraper.

```bash
# Basic usage - validate all aspects of the finance scraper
python -m tests.test_finance_scraper

# Validate only the search functionality
python -m tests.test_finance_scraper --validate-search

# Validate only form field handling
python -m tests.test_finance_scraper --validate-forms

# Validate with a specific config file
python -m tests.test_finance_scraper --config path/to/config.json
```

### `validate_csv_parsing.py`

Validates the CSV parsing functionality of the data collection modules.

```bash
# Basic usage - validate CSV parsing with default settings
python -m tests.validate_csv_parsing --file path/to/csv_file.csv

# Specify data types for columns
python -m tests.validate_csv_parsing --file path/to/csv_file.csv --types "amount:float,date:date,id:int"

# Suggest column mappings based on content
python -m tests.validate_csv_parsing --file path/to/csv_file.csv --suggest-mappings
```

### `validate_link_finding.py`

Validates the link finding functionality used in web scraping modules.

```bash
# Basic usage - validate link finding on a specific URL
python -m tests.validate_link_finding --url https://example.com/page

# Specify expected link patterns
python -m tests.validate_link_finding --url https://example.com/page --patterns "pdf$,/reports/,\.xlsx$"

# Validate with specific data types
python -m tests.validate_link_finding --url https://example.com/page --types "pdf,excel,zip"
```

### `validate_news_collection.py`

Validates the news article collection functionality.

```bash
# Basic usage - validate news collection for specific bills
python -m tests.validate_news_collection --bills "H0001,S0002,H0055"

# Validate with sample queries
python -m tests.validate_news_collection --sample-queries "Idaho education bill,property tax Idaho,gun legislation"

# Validate content extraction
python -m tests.validate_news_collection --validate-extraction --urls "https://example.com/article1,https://example.com/article2"
```

### `validate_amendment_tracking.py`

Validates the amendment tracking functionality.

```bash
# Basic usage - validate amendment tracking for specific bills
python -m tests.validate_amendment_tracking --bills "H0001,S0002,H0055"

# Validate text comparison functionality
python -m tests.validate_amendment_tracking --validate-comparison

# Validate with sample amendment documents
python -m tests.validate_amendment_tracking --sample-docs "path/to/amendment1.pdf,path/to/amendment2.pdf"
```

## Validation Process

For a thorough validation of the data collection modules, follow these steps:

### 1. Form Field Validation

If a module interacts with web forms, validate the form field handling:

```bash
python -m tests.test_finance_scraper --validate-forms
```

This verifies:
- All required fields are identified
- Field types are correctly detected
- Validation rules are enforced
- Form submission works correctly

### 2. Link Finding Validation

For modules that find and follow links:

```bash
python -m tests.validate_link_finding --url https://example.com/relevant_page
```

This verifies:
- Links are correctly identified
- Filters work properly
- Relative/absolute URL handling works

### 3. CSV Parsing Validation

For modules that process CSV files:

```bash
python -m tests.validate_csv_parsing --file path/to/sample.csv
```

This verifies:
- Headers are correctly identified
- Data types are properly inferred
- Row handling is correct
- Empty/special values are handled

### 4. News Collection Validation

For the news collection module:

```bash
python -m tests.validate_news_collection --sample-queries "relevant search term"
```

This verifies:
- API connections work
- Query generation is effective
- Results are properly processed
- Article extraction works

### 5. Amendment Tracking Validation

For the amendment tracking module:

```bash
python -m tests.validate_amendment_tracking --validate-comparison
```

This verifies:
- Document retrieval works
- Text extraction is accurate
- Text comparison correctly identifies changes
- Results are properly formatted

### 6. End-to-End Testing

Finally, perform an end-to-end test to ensure all components work together:

```bash
python -m tests.validate_e2e --modules "finance,news,amendments" --sample-bills "H0001,S0002"
```

This verifies the full data collection pipeline works correctly from start to finish.

## Debug Information

All validation scripts save detailed debug information to:

```
data/debug/validation/{module_name}/{timestamp}/
```

This includes:
- Screenshots (for web-based modules)
- Raw responses
- Intermediate processing results
- Validation reports

## Logging

Validation scripts generate detailed logs to:

```
logs/validation_{module}_{timestamp}.log
```

Set the log level using the `--log-level` parameter:

```bash
python -m tests.test_finance_scraper --log-level DEBUG
```

## Troubleshooting

If validation fails, check the following:

1. **API Access Issues**
   - Verify API keys are valid
   - Check rate limits
   - Confirm network connectivity

2. **Website Structure Changes**
   - Check if selectors need updating
   - Verify URL patterns are still valid
   - Update expected form fields if needed

3. **Data Format Changes**
   - Update expected column names
   - Adjust data type mappings
   - Modify parsing logic if needed

4. **Environment Issues**
   - Ensure dependencies are installed
   - Check Python version compatibility
   - Verify browser drivers are up-to-date (for web scraping)

## Contributing

To add a new validation script:

1. Create a new Python file in the `tests/` directory
2. Follow the validation script template
3. Implement the necessary validation logic
4. Add documentation to this README

For modifying existing scripts:

1. Maintain backward compatibility when possible
2. Update this README with any parameter changes
3. Add tests for new functionality 