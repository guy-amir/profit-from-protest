"""
Comprehensive CEI extraction with multiple strategies for all years.
"""

import logging
import os
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import camelot
import pandas as pd

from .utils import extract_year_from_filename, find_pdfs_in_folder


def extract_cei_comprehensive(pdf_path: str, year: int) -> pd.DataFrame:
    """
    Comprehensive extraction using multiple strategies and formats.
    """
    try:
        logging.info(f"Processing {os.path.basename(pdf_path)} for year {year}")
        
        # Try multiple extraction approaches
        strategies = [
            _strategy_lattice_all_pages,
            _strategy_stream_all_pages,
            _strategy_lattice_appendix,
            _strategy_stream_appendix,
            _strategy_wide_range_lattice,
            _strategy_wide_range_stream,
        ]
        
        best_result = pd.DataFrame()
        best_count = 0
        
        for i, strategy in enumerate(strategies):
            try:
                result = strategy(pdf_path, year)
                if len(result) > best_count:
                    best_result = result
                    best_count = len(result)
                    logging.info(f"Strategy {i+1} found {len(result)} companies")
                    
                    # If we found a substantial amount, use it
                    if len(result) > 50:
                        break
                        
            except Exception as e:
                logging.debug(f"Strategy {i+1} failed: {e}")
                continue
        
        if len(best_result) > 0:
            best_result['Year'] = year
            logging.info(f"Best result: {len(best_result)} companies for year {year}")
            return best_result
        else:
            logging.warning(f"No data extracted from {pdf_path}")
            return pd.DataFrame()
            
    except Exception as e:
        logging.error(f"Error processing {pdf_path}: {e}")
        return pd.DataFrame()


def _strategy_lattice_all_pages(pdf_path: str, year: int) -> pd.DataFrame:
    """Extract using lattice method on all pages."""
    tables = camelot.read_pdf(pdf_path, pages="all", flavor="lattice")
    return _process_tables_comprehensive(tables, year)


def _strategy_stream_all_pages(pdf_path: str, year: int) -> pd.DataFrame:
    """Extract using stream method on all pages."""
    tables = camelot.read_pdf(pdf_path, pages="all", flavor="stream")
    return _process_tables_comprehensive(tables, year)


def _strategy_lattice_appendix(pdf_path: str, year: int) -> pd.DataFrame:
    """Extract using lattice method on appendix pages."""
    tables = camelot.read_pdf(pdf_path, pages="30-100", flavor="lattice")
    return _process_tables_comprehensive(tables, year)


def _strategy_stream_appendix(pdf_path: str, year: int) -> pd.DataFrame:
    """Extract using stream method on appendix pages."""
    tables = camelot.read_pdf(pdf_path, pages="30-100", flavor="stream")
    return _process_tables_comprehensive(tables, year)


def _strategy_wide_range_lattice(pdf_path: str, year: int) -> pd.DataFrame:
    """Extract using lattice method on wide page range."""
    tables = camelot.read_pdf(pdf_path, pages="10-80", flavor="lattice")
    return _process_tables_comprehensive(tables, year)


def _strategy_wide_range_stream(pdf_path: str, year: int) -> pd.DataFrame:
    """Extract using stream method on wide page range."""
    tables = camelot.read_pdf(pdf_path, pages="10-80", flavor="stream")
    return _process_tables_comprehensive(tables, year)


def _process_tables_comprehensive(tables, year: int) -> pd.DataFrame:
    """Process extracted tables with comprehensive company detection."""
    if not tables:
        return pd.DataFrame()
    
    all_companies = []
    
    for table in tables:
        df = table.df
        if df.empty:
            continue
            
        # Try different approaches to find company data
        companies = _extract_companies_comprehensive(df, year)
        if len(companies) > 0:
            all_companies.append(companies)
    
    if all_companies:
        result = pd.concat(all_companies, ignore_index=True)
        # Remove duplicates and clean
        result = result.drop_duplicates(subset=['Company'])
        result = result[result['CEI_Score'].between(0, 100)]
        return result
    
    return pd.DataFrame()


def _extract_companies_comprehensive(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Extract companies using comprehensive approach."""
    if df.empty or df.shape[1] < 2:
        return pd.DataFrame()
    
    # Try all possible column combinations
    best_result = pd.DataFrame()
    best_score = 0
    
    for company_col in range(min(4, df.shape[1])):
        for score_col in range(df.shape[1]):
            if score_col == company_col:
                continue
                
            result = _try_extraction(df, company_col, score_col, year)
            
            # Score the result based on quality indicators
            score = _score_extraction_quality(result)
            
            if score > best_score:
                best_result = result
                best_score = score
    
    return best_result


def _try_extraction(df: pd.DataFrame, company_col: int, score_col: int, year: int) -> pd.DataFrame:
    """Try extracting using specific column combination."""
    try:
        # Extract columns
        temp_df = df.iloc[:, [company_col, score_col]].copy()
        temp_df.columns = ['Company', 'CEI_Score']
        
        # Basic cleaning
        temp_df['Company'] = temp_df['Company'].astype(str).str.strip()
        temp_df = temp_df[temp_df['Company'] != '']
        temp_df = temp_df[~temp_df['Company'].str.lower().isin(['nan', 'none', ''])]
        
        # Convert scores to numeric
        temp_df['CEI_Score'] = pd.to_numeric(temp_df['CEI_Score'], errors='coerce')
        temp_df = temp_df.dropna(subset=['CEI_Score'])
        
        # Filter for reasonable score range
        temp_df = temp_df[temp_df['CEI_Score'].between(0, 100)]
        
        if len(temp_df) < 3:  # Need at least 3 entries
            return pd.DataFrame()
        
        # Filter out obvious non-companies
        temp_df = _filter_non_companies(temp_df)
        
        return temp_df
        
    except Exception:
        return pd.DataFrame()


def _filter_non_companies(df: pd.DataFrame) -> pd.DataFrame:
    """Filter out obvious non-company entries."""
    if df.empty:
        return df
    
    # Remove short entries (likely headers/numbers)
    df = df[df['Company'].str.len() > 2]
    
    # Remove pure numbers
    df = df[~df['Company'].str.match(r'^\d+\.?\d*$', na=False)]
    
    # Remove common header/footer text
    exclusion_patterns = [
        r'^\d+$',  # Just numbers
        r'^page\s+\d+',  # Page numbers
        r'^appendix',  # Appendix headers
        r'^table\s+\d+',  # Table numbers
        r'^figure\s+\d+',  # Figure numbers
        r'^score$',  # Score header
        r'^rating$',  # Rating header
        r'^company$',  # Company header
        r'^cei\s*score',  # CEI Score header
        r'^total$',  # Total
        r'^average$',  # Average
        r'^points?$',  # Points
        r'^\s*$',  # Empty/whitespace
    ]
    
    for pattern in exclusion_patterns:
        df = df[~df['Company'].str.lower().str.match(pattern, na=False)]
    
    # Keep entries that look like company names
    # Either have common company suffixes or are reasonably long
    company_indicators = [
        r'\b(inc\.?|corp\.?|corporation|company|llc|ltd\.?|llp|co\.?|group|holdings?|enterprises?)\b',
        r'&',  # Companies with &
        r'\w+\s+\w+.*',  # Multi-word names
    ]
    
    company_mask = False
    for pattern in company_indicators:
        company_mask = company_mask | df['Company'].str.contains(pattern, case=False, na=False, regex=True)
    
    # Also keep longer names (likely companies even without obvious indicators)
    company_mask = company_mask | (df['Company'].str.len() > 15)
    
    df_filtered = df[company_mask]
    
    # If we filtered too aggressively and have very few results, be less strict
    if len(df_filtered) < len(df) * 0.1 and len(df) > 20:
        # Just remove the most obvious non-companies
        basic_exclusions = [r'^\d+$', r'^page\s+\d+', r'^appendix', r'^table\s+\d+']
        for pattern in basic_exclusions:
            df = df[~df['Company'].str.lower().str.match(pattern, na=False)]
        return df
    
    return df_filtered


def _score_extraction_quality(df: pd.DataFrame) -> int:
    """Score the quality of an extraction result."""
    if df.empty:
        return 0
    
    score = len(df)  # Base score is number of entries
    
    # Bonus for variety in scores
    if df['CEI_Score'].nunique() > 1:
        score += 10
    
    # Bonus for having perfect scores (100) - common in CEI
    if (df['CEI_Score'] == 100).any():
        score += 5
    
    # Bonus for having reasonable score distribution
    if df['CEI_Score'].min() < df['CEI_Score'].max():
        score += 5
    
    # Penalty for too many identical scores (probably wrong column)
    most_common_score_count = df['CEI_Score'].value_counts().iloc[0]
    if most_common_score_count > len(df) * 0.8 and len(df) > 10:
        score -= 20
    
    # Bonus for company-like names
    company_pattern = r'\b(inc\.?|corp\.?|llc|ltd\.?|company|group)\b'
    company_like = df['Company'].str.contains(company_pattern, case=False, na=False, regex=True).sum()
    score += company_like * 2
    
    return score


def process_all_missing_years():
    """Process all missing years comprehensively."""
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    cei_folder = "/Users/guy/Projects/noni/pfp/data/raw/CEI"
    output_folder = "/Users/guy/Projects/noni/pfp/data/processed/cei"
    
    # Get existing processed years
    existing_years = set()
    try:
        for file in os.listdir(output_folder):
            if file.startswith('cei_') and file.endswith('.csv'):
                year_match = re.search(r'cei_(\d{4})\.csv', file)
                if year_match:
                    existing_years.add(int(year_match.group(1)))
    except FileNotFoundError:
        os.makedirs(output_folder, exist_ok=True)
    
    # Get all PDF files
    pdf_files = find_pdfs_in_folder(cei_folder)
    
    # Process missing years
    processed_count = 0
    for pdf_path in pdf_files:
        year = extract_year_from_filename(pdf_path)
        if year is None or year in existing_years:
            continue
        
        # Extract data
        cei_data = extract_cei_comprehensive(pdf_path, year)
        
        if cei_data.empty:
            logging.warning(f"No data extracted for year {year}")
            continue
        
        # Save file
        output_file = os.path.join(output_folder, f"cei_{year}.csv")
        cei_data.to_csv(output_file, index=False)
        
        logging.info(f"✓ Saved {len(cei_data)} companies for year {year}")
        processed_count += 1
    
    logging.info(f"Processing complete. Successfully processed {processed_count} additional years.")


if __name__ == "__main__":
    process_all_missing_years()