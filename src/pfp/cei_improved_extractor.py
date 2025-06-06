"""
Improved CEI data extraction script with better table detection and company name filtering.
"""

import logging
import os
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import camelot
import pandas as pd

from .utils import extract_year_from_filename, find_pdfs_in_folder


def extract_cei_data_improved(pdf_path: str, year: int) -> pd.DataFrame:
    """
    Improved extraction that better identifies actual company names and CEI scores.
    
    Args:
        pdf_path: Path to the CEI PDF file
        year: Year of the report
        
    Returns:
        DataFrame with columns: Company, CEI_Score, Year
    """
    try:
        # Try multiple extraction strategies
        strategies = [
            _extract_strategy_appendix,
            _extract_strategy_wide_pages, 
            _extract_strategy_all_pages
        ]
        
        for strategy in strategies:
            result = strategy(pdf_path, year)
            if not result.empty and len(result) > 10:  # Need substantial data
                logging.info(f"Successfully extracted {len(result)} companies for {year}")
                return result
        
        logging.warning(f"No substantial CEI data found in {pdf_path}")
        return pd.DataFrame()
            
    except Exception as e:
        logging.error(f"Error processing {pdf_path}: {e}")
        return pd.DataFrame()


def _extract_strategy_appendix(pdf_path: str, year: int) -> pd.DataFrame:
    """Try extracting from appendix pages (common location for company lists)."""
    try:
        # Look for appendix pages - usually later in document
        tables = camelot.read_pdf(pdf_path, pages="40-100", flavor="stream")
        return _process_tables_for_companies(tables, year)
    except:
        return pd.DataFrame()


def _extract_strategy_wide_pages(pdf_path: str, year: int) -> pd.DataFrame:
    """Try extracting from a wider range of pages."""
    try:
        tables = camelot.read_pdf(pdf_path, pages="20-80", flavor="stream")
        return _process_tables_for_companies(tables, year)
    except:
        return pd.DataFrame()


def _extract_strategy_all_pages(pdf_path: str, year: int) -> pd.DataFrame:
    """Last resort - try all pages."""
    try:
        tables = camelot.read_pdf(pdf_path, pages="all", flavor="stream")
        return _process_tables_for_companies(tables, year)
    except:
        return pd.DataFrame()


def _process_tables_for_companies(tables, year: int) -> pd.DataFrame:
    """Process extracted tables to find company data."""
    if not tables:
        return pd.DataFrame()
    
    all_companies = []
    
    for table in tables:
        df = table.df
        if df.empty or df.shape[1] < 2:
            continue
            
        # Try to find company data in this table
        company_data = _extract_companies_from_table(df, year)
        if not company_data.empty:
            all_companies.append(company_data)
    
    if all_companies:
        result = pd.concat(all_companies, ignore_index=True)
        result = result.drop_duplicates(subset=['Company'])
        return result
    
    return pd.DataFrame()


def _extract_companies_from_table(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Extract company names and scores from a single table."""
    if df.empty:
        return pd.DataFrame()
    
    # Try different column combinations
    best_result = pd.DataFrame()
    best_score = 0
    
    for company_col_idx in range(min(3, df.shape[1])):  # Try first 3 columns as company
        for score_col_idx in range(df.shape[1]):
            if score_col_idx == company_col_idx:
                continue
                
            result = _try_column_combination(df, company_col_idx, score_col_idx, year)
            if len(result) > best_score:
                best_result = result
                best_score = len(result)
    
    return best_result


def _try_column_combination(df: pd.DataFrame, company_col: int, score_col: int, year: int) -> pd.DataFrame:
    """Try extracting data using specific column combination."""
    try:
        # Extract the two columns
        temp_df = df.iloc[:, [company_col, score_col]].copy()
        temp_df.columns = ['Company', 'CEI_Score']
        
        # Clean company names
        temp_df['Company'] = temp_df['Company'].astype(str).str.strip()
        temp_df = temp_df[temp_df['Company'] != '']
        temp_df = temp_df[~temp_df['Company'].str.lower().isin(['nan', 'none', ''])]
        
        # Filter out obvious non-companies
        temp_df = temp_df[temp_df['Company'].str.len() > 3]  # At least 4 characters
        temp_df = temp_df[~temp_df['Company'].str.match(r'^\d+\.?\d*$', na=False)]  # Not just numbers
        temp_df = temp_df[~temp_df['Company'].str.lower().str.contains(
            r'\b(score|rating|cei|points?|appendix|page|table|total|average)\b', 
            na=False, regex=True
        )]
        
        # Must look like company names (contains letters, possibly with common suffixes)
        company_pattern = r'[A-Za-z].*(?:inc\.?|corp\.?|llc|ltd\.?|llp|co\.?|company|group|holdings?|enterprises?|associates?|partners?|&|and|\w+\s+\w+)'
        temp_df = temp_df[
            temp_df['Company'].str.contains(company_pattern, case=False, na=False, regex=True) |
            (temp_df['Company'].str.len() > 10)  # Or reasonably long names
        ]
        
        # Clean and validate scores
        temp_df['CEI_Score'] = pd.to_numeric(temp_df['CEI_Score'], errors='coerce')
        temp_df = temp_df.dropna(subset=['CEI_Score'])
        
        # CEI scores should be 0-100
        temp_df = temp_df[temp_df['CEI_Score'].between(0, 100)]
        
        # Should have some variety in scores (not all the same score)
        if temp_df['CEI_Score'].nunique() < 2 and len(temp_df) > 5:
            return pd.DataFrame()  # Probably wrong column
        
        # Filter out obvious scoring rubric entries
        temp_df = temp_df[~temp_df['Company'].str.contains(
            r'\b(criterion|criteria|requirement|policy|benefit|training|harassment|discrimination)\b',
            case=False, na=False, regex=True
        )]
        
        return temp_df
        
    except Exception:
        return pd.DataFrame()


def fix_cei_files_in_directory(fix_dir: str, output_dir: str) -> None:
    """
    Re-process the PDFs for files in the fix directory.
    
    Args:
        fix_dir: Directory containing incorrectly processed CSV files
        output_dir: Directory to save corrected CSV files
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # Get list of years that need fixing
    fix_files = os.listdir(fix_dir)
    years_to_fix = []
    
    for file in fix_files:
        if file.startswith('cei_') and file.endswith('.csv'):
            year_match = re.search(r'cei_(\d{4})\.csv', file)
            if year_match:
                years_to_fix.append(int(year_match.group(1)))
    
    logging.info(f"Found {len(years_to_fix)} years to fix: {years_to_fix}")
    
    # Find corresponding PDF files
    cei_folder = "/Users/guy/Projects/noni/pfp/data/raw/CEI"
    pdf_files = find_pdfs_in_folder(cei_folder)
    
    for year in years_to_fix:
        # Find PDF for this year
        pdf_file = None
        for pdf_path in pdf_files:
            if extract_year_from_filename(pdf_path) == year:
                pdf_file = pdf_path
                break
        
        if pdf_file is None:
            logging.warning(f"No PDF found for year {year}")
            continue
        
        logging.info(f"Re-processing {os.path.basename(pdf_file)} for year {year}")
        
        # Extract with improved method
        cei_data = extract_cei_data_improved(pdf_file, year)
        
        if cei_data.empty:
            logging.warning(f"Still no data extracted for year {year}")
            continue
        
        # Save corrected file
        output_file = os.path.join(output_dir, f"cei_{year}.csv")
        cei_data.to_csv(output_file, index=False)
        
        logging.info(f"Fixed {year}: saved {len(cei_data)} companies to {output_file}")


def process_missing_years(cei_folder: str, output_dir: str) -> None:
    """
    Process years that don't have CSV files yet.
    
    Args:
        cei_folder: Directory containing CEI PDFs
        output_dir: Directory to save CSV files
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # Get existing CSV years
    existing_years = set()
    for subdir in ['/cei', '/fix']:
        try:
            files = os.listdir(output_dir + subdir)
            for file in files:
                if file.startswith('cei_') and file.endswith('.csv'):
                    year_match = re.search(r'cei_(\d{4})\.csv', file)
                    if year_match:
                        existing_years.add(int(year_match.group(1)))
        except FileNotFoundError:
            continue
    
    # Get all PDF years
    pdf_files = find_pdfs_in_folder(cei_folder)
    all_years = set()
    for pdf_path in pdf_files:
        year = extract_year_from_filename(pdf_path)
        if year:
            all_years.add(year)
    
    # Find missing years
    missing_years = all_years - existing_years
    logging.info(f"Found {len(missing_years)} missing years: {sorted(missing_years)}")
    
    # Process missing years
    for year in sorted(missing_years):
        # Find PDF for this year
        pdf_file = None
        for pdf_path in pdf_files:
            if extract_year_from_filename(pdf_path) == year:
                pdf_file = pdf_path
                break
        
        if pdf_file is None:
            continue
        
        logging.info(f"Processing {os.path.basename(pdf_file)} for year {year}")
        
        # Extract data
        cei_data = extract_cei_data_improved(pdf_file, year)
        
        if cei_data.empty:
            logging.warning(f"No data extracted for year {year}")
            continue
        
        # Save file
        output_file = os.path.join(output_dir, f"cei_{year}.csv")
        cei_data.to_csv(output_file, index=False)
        
        logging.info(f"Processed {year}: saved {len(cei_data)} companies to {output_file}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Fix incorrectly processed files
    fix_dir = "/Users/guy/Projects/noni/pfp/data/processed/fix"
    cei_dir = "/Users/guy/Projects/noni/pfp/data/processed/cei"
    
    print("Fixing incorrectly processed files...")
    fix_cei_files_in_directory(fix_dir, cei_dir)
    
    # Process missing years
    cei_folder = "/Users/guy/Projects/noni/pfp/data/raw/CEI"
    output_dir = "/Users/guy/Projects/noni/pfp/data/processed/cei"
    
    print("Processing missing years...")
    process_missing_years(cei_folder, output_dir)