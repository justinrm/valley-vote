#!/usr/bin/env python3
"""
Validate and refine CSV parsing for Idaho SOS Sunshine Portal data.

This script helps test different CSV parsing strategies and column mappings
to ensure accurate data extraction from downloaded finance files.
"""

import argparse
import csv
import io
import logging
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

import pandas as pd
import chardet

from src.config import (
    FINANCE_COLUMN_MAPS
)
from src.utils import setup_logging, setup_project_paths
from src.scrape_finance_idaho import standardize_columns

# --- Configure Logging ---
logger = logging.getLogger('validate_csv_parsing')

def detect_encoding(file_path: Path) -> str:
    """
    Detect the encoding of a file.
    
    Args:
        file_path: Path to the file
        
    Returns:
        Detected encoding
    """
    logger.info(f"Detecting encoding for {file_path}")
    
    # Read a sample of the file
    with open(file_path, 'rb') as f:
        raw_data = f.read(10000)  # Read first 10KB
    
    # Detect encoding
    result = chardet.detect(raw_data)
    encoding = result['encoding']
    confidence = result['confidence']
    
    logger.info(f"Detected encoding: {encoding} (confidence: {confidence:.2f})")
    
    return encoding

def try_parse_csv(file_path: Path, encoding: Optional[str] = None) -> Tuple[Optional[pd.DataFrame], str]:
    """
    Try to parse a CSV file with different encodings and settings.
    
    Args:
        file_path: Path to the CSV file
        encoding: Optional encoding to try first
        
    Returns:
        Tuple of (DataFrame, encoding used)
    """
    logger.info(f"Attempting to parse {file_path}")
    
    # Try different encodings
    encodings_to_try = []
    if encoding:
        encodings_to_try.append(encoding)
    
    # Add common encodings
    encodings_to_try.extend(['utf-8', 'latin-1', 'cp1252', 'iso-8859-1'])
    
    # Try each encoding
    for enc in encodings_to_try:
        try:
            logger.info(f"Trying encoding: {enc}")
            df = pd.read_csv(file_path, encoding=enc, low_memory=False, on_bad_lines='warn')
            logger.info(f"Successfully parsed with encoding: {enc}")
            return df, enc
        except Exception as e:
            logger.warning(f"Failed to parse with encoding {enc}: {e}")
    
    # If all encodings fail, try to detect the encoding
    detected_encoding = detect_encoding(file_path)
    if detected_encoding and detected_encoding not in encodings_to_try:
        try:
            logger.info(f"Trying detected encoding: {detected_encoding}")
            df = pd.read_csv(file_path, encoding=detected_encoding, low_memory=False, on_bad_lines='warn')
            logger.info(f"Successfully parsed with detected encoding: {detected_encoding}")
            return df, detected_encoding
        except Exception as e:
            logger.error(f"Failed to parse with detected encoding {detected_encoding}: {e}")
    
    logger.error("Failed to parse CSV with any encoding")
    return None, ""

def analyze_csv_structure(df: pd.DataFrame, file_path: Path) -> None:
    """
    Analyze the structure of a parsed CSV DataFrame.
    
    Args:
        df: Parsed DataFrame
        file_path: Path to the original file
    """
    logger.info(f"Analyzing CSV structure for {file_path.name}")
    
    # Basic info
    logger.info(f"Shape: {df.shape}")
    logger.info(f"Columns: {df.columns.tolist()}")
    
    # Data types
    logger.info("Data types:")
    for col, dtype in df.dtypes.items():
        logger.info(f"  {col}: {dtype}")
    
    # Sample data
    logger.info("Sample data (first 2 rows):")
    logger.info(df.head(2).to_string())
    
    # Check for missing values
    missing = df.isnull().sum()
    if missing.any():
        logger.info("Missing values:")
        for col, count in missing[missing > 0].items():
            logger.info(f"  {col}: {count} ({count/len(df)*100:.2f}%)")
    
    # Check for potential date columns
    date_pattern = re.compile(r'date|year|month|day|period|filing', re.I)
    potential_date_cols = [col for col in df.columns if date_pattern.search(col)]
    if potential_date_cols:
        logger.info(f"Potential date columns: {potential_date_cols}")
        for col in potential_date_cols:
            sample_values = df[col].dropna().head(5).tolist()
            logger.info(f"  {col} sample values: {sample_values}")
    
    # Check for potential amount columns
    amount_pattern = re.compile(r'amount|contribution|expenditure|payment|donation|receipt|disbursement', re.I)
    potential_amount_cols = [col for col in df.columns if amount_pattern.search(col)]
    if potential_amount_cols:
        logger.info(f"Potential amount columns: {potential_amount_cols}")
        for col in potential_amount_cols:
            sample_values = df[col].dropna().head(5).tolist()
            logger.info(f"  {col} sample values: {sample_values}")
    
    # Check for potential ID columns
    id_pattern = re.compile(r'id|number|code|reference', re.I)
    potential_id_cols = [col for col in df.columns if id_pattern.search(col)]
    if potential_id_cols:
        logger.info(f"Potential ID columns: {potential_id_cols}")
        for col in potential_id_cols:
            sample_values = df[col].dropna().head(5).tolist()
            logger.info(f"  {col} sample values: {sample_values}")

def test_column_mapping(df: pd.DataFrame, column_map: Dict[str, List[str]], data_type: str) -> pd.DataFrame:
    """
    Test column mapping on a DataFrame.
    
    Args:
        df: Parsed DataFrame
        column_map: Column mapping dictionary
        data_type: Type of data ('contributions' or 'expenditures')
        
    Returns:
        Standardized DataFrame
    """
    logger.info(f"Testing column mapping for {data_type}")
    
    # Log original columns
    logger.info(f"Original columns: {df.columns.tolist()}")
    
    # Apply column mapping
    df_standardized = standardize_columns(df, column_map)
    
    # Log mapped columns
    logger.info(f"Mapped columns: {df_standardized.columns.tolist()}")
    
    # Check for unmapped columns
    unmapped = set(df.columns) - set(column_map.keys())
    if unmapped:
        logger.warning(f"Unmapped columns: {unmapped}")
    
    # Check for missing standard columns
    missing_standard = set(column_map.keys()) - set(df_standardized.columns)
    if missing_standard:
        logger.warning(f"Missing standard columns: {missing_standard}")
    
    return df_standardized

def suggest_column_mapping(df: pd.DataFrame, data_type: str) -> Dict[str, List[str]]:
    """
    Suggest a column mapping based on the DataFrame columns.
    
    Args:
        df: Parsed DataFrame
        data_type: Type of data ('contributions' or 'expenditures')
        
    Returns:
        Suggested column mapping
    """
    logger.info(f"Suggesting column mapping for {data_type}")
    
    # Get existing column map
    existing_map = FINANCE_COLUMN_MAPS[data_type]
    
    # Create a new mapping
    suggested_map = {}
    
    # For each standard column, find potential matches
    for standard_col, variations in existing_map.items():
        # Check if any variation exists in the DataFrame
        matches = [var for var in variations if var in df.columns]
        if matches:
            suggested_map[standard_col] = matches
        else:
            # Try to find a similar column
            similar_cols = []
            for col in df.columns:
                # Check if the column name contains the standard column name
                if standard_col.lower() in col.lower():
                    similar_cols.append(col)
                # Check if any variation contains the column name
                elif any(var.lower() in col.lower() for var in variations):
                    similar_cols.append(col)
            
            if similar_cols:
                suggested_map[standard_col] = similar_cols
            else:
                # No match found, use the first variation as a placeholder
                suggested_map[standard_col] = [variations[0]]
    
    # Log the suggested mapping
    logger.info("Suggested column mapping:")
    for standard_col, matches in suggested_map.items():
        logger.info(f"  {standard_col}: {matches}")
    
    return suggested_map

def main() -> int:
    """Main function for command-line execution."""
    parser = argparse.ArgumentParser(
        description="Validate and refine CSV parsing for Idaho SOS Sunshine Portal data.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('file', type=Path, help='Path to the CSV file to analyze')
    parser.add_argument('--data-type', type=str, choices=['contributions', 'expenditures'], default='contributions',
                        help='Type of data in the CSV file')
    parser.add_argument('--encoding', type=str, help='Encoding to use for parsing (optional)')
    parser.add_argument('--suggest-mapping', action='store_true',
                        help='Suggest a column mapping based on the CSV structure')
    parser.add_argument('--data-dir', type=str, default=None,
                        help='Override base data directory (default: ./data from config/utils)')
    
    args = parser.parse_args()
    
    # Setup paths
    try:
        paths = setup_project_paths(args.data_dir)
    except SystemExit:
        sys.exit(1)
    
    # Setup logging
    logger = setup_logging('validate_csv_parsing.log', paths['log'])
    
    # Check if file exists
    if not args.file.exists():
        logger.error(f"File not found: {args.file}")
        return 1
    
    # Parse the CSV
    df, encoding = try_parse_csv(args.file, args.encoding)
    if df is None:
        logger.error("Failed to parse CSV")
        return 1
    
    # Analyze the CSV structure
    analyze_csv_structure(df, args.file)
    
    # Test column mapping
    column_map = FINANCE_COLUMN_MAPS[args.data_type]
    df_standardized = test_column_mapping(df, column_map, args.data_type)
    
    # Suggest column mapping if requested
    if args.suggest_mapping:
        suggested_map = suggest_column_mapping(df, args.data_type)
        
        # Save the suggested mapping to a file
        output_file = paths['artifacts'] / f"suggested_{args.data_type}_column_map_{args.file.stem}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(suggested_map, f, indent=2)
        
        logger.info(f"Saved suggested column mapping to {output_file}")
    
    return 0

if __name__ == "__main__":
    import json
    
    sys.exit(main()) 