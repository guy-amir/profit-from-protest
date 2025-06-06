"""
CEI data extraction script for processing PDF reports.
"""

import logging
import os
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import camelot
import pandas as pd

from .utils import extract_year_from_filename, find_pdfs_in_folder


def extract_cei_data_from_pdf(pdf_path: str, year: int) -> pd.DataFrame:
    """
    Extract CEI data from a single PDF file.
    
    Args:
        pdf_path: Path to the CEI PDF file
        year: Year of the report
        
    Returns:
        DataFrame with columns: Company, CEI_Score, Year
    """
    try:
        # Use existing function from utils.py for consistency
        from .utils import pdf_to_df
        
        # Extract tables using the existing function
        df = pdf_to_df(pdf_path)
        
        if df.empty:
            logging.warning(f"No data extracted from {pdf_path}")
            return pd.DataFrame()
            
        # Clean and process the extracted data
        cleaned_df = _process_extracted_table(df, year)
        
        if not cleaned_df.empty:
            cleaned_df['Year'] = year
            return cleaned_df
        else:
            logging.warning(f"No valid CEI data found in {pdf_path}")
            return pd.DataFrame()
            
    except Exception as e:
        logging.error(f"Error processing {pdf_path}: {e}")
        return pd.DataFrame()


def _is_cei_score_table(df: pd.DataFrame) -> bool:
    """
    Heuristic to determine if a table contains CEI score data.
    """
    if df.empty or df.shape[1] < 2:
        return False
        
    # Convert to string and look for patterns
    df_str = df.astype(str)
    
    # Look for keywords that indicate this is a CEI table
    keywords = ['company', 'corporation', 'score', 'rating', 'cei', 'equality']
    
    # Get text from first row and first column
    first_row_text = ' '.join(df_str.iloc[0].tolist()).lower()
    first_col_text = ' '.join(df_str.iloc[:3, 0].tolist()).lower()
    combined_text = first_row_text + ' ' + first_col_text
    
    has_keywords = any(keyword in combined_text for keyword in keywords)
    
    # Look for numeric scores (0-100 range typical for CEI)
    has_numeric_scores = False
    for col in df.columns:
        try:
            numeric_vals = pd.to_numeric(df[col], errors='coerce')
            if numeric_vals.between(0, 100).sum() > 5:  # At least 5 valid scores
                has_numeric_scores = True
                break
        except:
            continue
            
    return has_keywords and has_numeric_scores


def _process_extracted_table(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """
    Process the extracted table to find company names and CEI scores.
    """
    if df.empty:
        return pd.DataFrame()
        
    # Make a copy to avoid modifying original
    df = df.copy()
    
    # For CEI reports, we typically have company names in first column
    # and CEI scores in another column
    if df.shape[1] < 2:
        return pd.DataFrame()
    
    # Assume first column contains company names and look for score column
    company_col = df.columns[0]
    score_col = None
    
    # Look for the column that contains the most valid CEI scores (0-100)
    best_score_count = 0
    for col in df.columns[1:]:  # Skip company column
        try:
            # Convert to numeric and count valid scores
            numeric_vals = pd.to_numeric(df[col], errors='coerce')
            valid_scores = numeric_vals.between(0, 100).sum()
            if valid_scores > best_score_count:
                best_score_count = valid_scores
                score_col = col
        except:
            continue
    
    if score_col is None or best_score_count < 5:  # Need at least 5 valid scores
        logging.warning(f"Could not find valid CEI score column for year {year}")
        return pd.DataFrame()
    
    # Extract company and score data
    result = df[[company_col, score_col]].copy()
    result.columns = ['Company', 'CEI_Score']
    
    # Clean company names - remove empty, non-company entries
    result['Company'] = result['Company'].astype(str).str.strip()
    result = result[result['Company'] != '']
    result = result[~result['Company'].str.lower().isin(['nan', 'none', 'company', ''])]
    
    # Filter out obvious non-company entries (like headers, footnotes)
    non_company_patterns = [
        r'^\d+$',  # Just numbers
        r'^[a-z\s]*$',  # All lowercase (likely headers)
        r'page \d+',   # Page numbers
        r'appendix',   # Appendix text
        r'cei score',  # Header text
        r'rating',     # Header text
    ]
    
    for pattern in non_company_patterns:
        result = result[~result['Company'].str.lower().str.match(pattern, na=False)]
    
    # Clean and validate scores
    result['CEI_Score'] = pd.to_numeric(result['CEI_Score'], errors='coerce')
    result = result.dropna(subset=['CEI_Score'])
    result = result[result['CEI_Score'].between(0, 100)]
    
    # Remove duplicates
    result = result.drop_duplicates(subset=['Company'])
    
    logging.info(f"Processed {len(result)} companies for year {year}")
    return result


def process_all_cei_pdfs(cei_folder: str, output_folder: str) -> None:
    """
    Process all CEI PDFs and save individual CSV files.
    
    Args:
        cei_folder: Path to folder containing CEI PDFs
        output_folder: Path to save processed CSV files
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)
    
    # Find all PDF files
    pdf_files = find_pdfs_in_folder(cei_folder)
    
    if not pdf_files:
        logging.error(f"No PDF files found in {cei_folder}")
        return
    
    logging.info(f"Found {len(pdf_files)} PDF files to process")
    
    processed_count = 0
    for pdf_path in pdf_files:
        year = extract_year_from_filename(pdf_path)
        if year is None:
            logging.warning(f"Could not extract year from {pdf_path}")
            continue
            
        logging.info(f"Processing {os.path.basename(pdf_path)} for year {year}")
        
        # Extract data
        cei_data = extract_cei_data_from_pdf(pdf_path, year)
        
        if cei_data.empty:
            logging.warning(f"No data extracted from {pdf_path}")
            continue
        
        # Save to CSV
        output_file = os.path.join(output_folder, f"cei_{year}.csv")
        cei_data.to_csv(output_file, index=False)
        
        logging.info(f"Saved {len(cei_data)} companies for year {year} to {output_file}")
        processed_count += 1
    
    logging.info(f"Processing complete. Successfully processed {processed_count} files.")


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Process all CEI PDFs
    cei_folder = "/Users/guy/Projects/noni/pfp/data/raw/CEI"
    output_folder = "/Users/guy/Projects/noni/pfp/data/processed"
    
    process_all_cei_pdfs(cei_folder, output_folder)