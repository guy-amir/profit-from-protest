"""
Utility functions for the pfp project.
"""

import logging
import os
import re
from datetime import timedelta
from pathlib import Path
from typing import List, Optional, Tuple

import camelot
import pandas as pd

def pdf_to_df(pdf_path: str) -> pd.DataFrame:
    """
    Example Python script to extract tables from the CEI PDF (specifically from Appendix A)
    and produce a cleaned DataFrame with the following columns:
    - Company (the list of companies)
    - CEI_Rating (the company rating for that year)
    - Publication Date (the date the report was published for that year)
    - Location (company location, later used for state classification)
    - stable_id (a unique identifier for each company)
    - public_flag (set to 1 if the company appears to be publicly traded)
    
    Notes:
    - This example uses Camelot (with the “stream” flavor) to extract tables.
    - Adjust the page range (pages="45-50") as needed to target the pages containing Appendix A.
    - The heuristic for “public_flag” is basic and may need adjustment.
    """
    # Extract all tables from a specific range of pages (adjust pages as necessary)
    tables = camelot.read_pdf(pdf_path, pages="45-50", flavor="stream")
    year = extract_year_from_filename(pdf_path) 
    # Combine tables from the specified pages into one DataFrame
    if tables:
        df_list = [t.df for t in tables]
        df_raw = pd.concat(df_list, ignore_index=True)
    else:
        raise ValueError("No tables were found in the specified pages.")

    # # Print a preview of the raw extracted table
    # print("Raw extracted table head:")
    # print(df_raw.head())

    # Assume the first row contains headers; assign them as column names
    df_raw.columns = df_raw.iloc[0]
    df = df_raw[1:].reset_index(drop=True)

    # Strip whitespace from column names
    df.columns = [col.strip() for col in df.columns]
    # print("Columns in extracted table:", df.columns.tolist())
    return df

def find_pdfs_in_folder(folder_path: str) -> List[str]:
    """
    Recursively finds all PDF files in the given folder.
    
    Args:
        folder_path (str): The directory to search in.
        
    Returns:
        A list of full paths to PDF files.
    """
    pdf_files = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.lower().endswith(".pdf"):
                if extract_year_from_filename(file) is not None:
                    pdf_files.append(os.path.join(root, file))
    return pdf_files

def is_public_heuristic(company_name: str) -> int:
    public_indicators = ["Inc", "Corp", "Corporation", "Limited", "Ltd", "LLC", "PLC", "Group", "Holdings", "NV", "SA", "AG"]
    private_indicators = ["LLP", "LP", "Private", "Partners"]

    name_lower = company_name.lower()

    if any(keyword.lower() in name_lower for keyword in private_indicators):
        return 0  # Explicitly private indicators
    if any(keyword.lower() in name_lower for keyword in public_indicators):
        return 1  # Explicitly public indicators
    else:
        return -1  # Default assumption is private if uncertain
    
def extract_year_from_filename(filename: str) -> Optional[int]:
    match = re.search(r'(\d{4})', os.path.basename(filename))
    return int(match.group(1)) if match else None


def process_cei_df_list(pdf_file_list: List[str]) -> List[pd.DataFrame]:
    cei_df_list = []
    for pdf in pdf_file_list:
        try:
            df = pdf_to_df(pdf)
            year = extract_year_from_filename(pdf)
            df['Year'] = year
            cei_df_list.append(df)
        except Exception as e:
            logging.warning(f"Error processing {pdf}: {e}")
            continue
    return cei_df_list

# Define the updated function to filter a specific date range
def load_date_range_rows(
    csv_path: str, 
    start_date: str, 
    end_date: str,
    date_col: str = 'date'
) -> pd.DataFrame:
    chunksize = 10**5
    filtered_chunks = []

    #start_time = time.time()

    for chunk in pd.read_csv(csv_path, chunksize=chunksize, parse_dates=[date_col]):
        mask = (chunk[date_col] >= pd.to_datetime(start_date)) & (chunk[date_col] <= pd.to_datetime(end_date))
        filtered = chunk[mask]
        if not filtered.empty:
            filtered_chunks.append(filtered)

    df_range = pd.concat(filtered_chunks, ignore_index=True)
    #elapsed_time = time.time() - start_time

    return df_range#, elapsed_time


def load_cei_release_dates(csv_path: str) -> pd.DataFrame:
    """
    Loads the CEI release dates from a CSV file.

    Parameters:
        csv_path (str): Path to the CSV file.

    Returns:
        pd.DataFrame: DataFrame with 'Year' as int and 'Release Date' as datetime.
    """
    df = pd.read_csv(csv_path)
    df['Year'] = df['Year'].astype(int)
    df['Release Date'] = pd.to_datetime(df['Release Date'], format='%Y-%m-%d')
    return df

def get_cei_date_range(
    year: int, 
    cei_df: pd.DataFrame, 
    before_days: int = 5, 
    after_days: int = 5
) -> Tuple[pd.Timestamp, pd.Timestamp]:
    """
    Given a year, returns the 5-day before and after range of the CEI release date.

    Parameters:
        year (int): Year for which to fetch the date range.
        cei_df (pd.DataFrame): DataFrame with 'Year' (int) and 'Release Date' (datetime).

    Returns:
        tuple[pd.Timestamp, pd.Timestamp]: (start_date, end_date)
    """
    row = cei_df[cei_df['Year'] == year]
    if row.empty:
        raise ValueError(f"No CEI release date found for year {year}")
    
    release_date = row.iloc[0]['Release Date']
    start_date = release_date - timedelta(days=before_days)
    end_date = release_date + timedelta(days=after_days)
    return start_date, end_date
