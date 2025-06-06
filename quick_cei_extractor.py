#!/usr/bin/env python3
"""
Quick CEI extraction focusing on most likely successful approaches.
"""

import sys
import os
sys.path.append('src')

import logging
import re
import camelot
import pandas as pd

def quick_extract_year(year):
    """Quick extraction for a single year."""
    cei_folder = "/Users/guy/Projects/noni/pfp/data/raw/CEI"
    output_folder = "/Users/guy/Projects/noni/pfp/data/processed/cei"
    
    # Find PDF for this year
    pdf_path = None
    for file in os.listdir(cei_folder):
        if file.lower().endswith(".pdf"):
            match = re.search(r'(\d{4})', file)
            if match and int(match.group(1)) == year:
                pdf_path = os.path.join(cei_folder, file)
                break
    
    if not pdf_path:
        print(f"No PDF found for year {year}")
        return False
    
    print(f"Processing {os.path.basename(pdf_path)} for year {year}")
    
    # Try simple approach - just stream on limited pages
    try:
        # Most CEI data is in appendix, try pages 40-70
        tables = camelot.read_pdf(pdf_path, pages="40-70", flavor="stream")
        
        if not tables:
            # Fallback to wider range
            tables = camelot.read_pdf(pdf_path, pages="30-80", flavor="stream")
            
        if not tables:
            print(f"No tables found in {pdf_path}")
            return False
        
        # Process tables to find company data
        all_data = []
        for table in tables:
            df = table.df
            if df.empty or df.shape[1] < 2:
                continue
            
            # Simple heuristic: look for first column with names, second+ columns with numbers
            company_col = 0
            score_col = None
            
            # Find best numeric column
            for col in range(1, min(df.shape[1], 5)):  # Check first few columns
                try:
                    numeric_vals = pd.to_numeric(df.iloc[:, col], errors='coerce')
                    valid_scores = numeric_vals.between(0, 100).sum()
                    if valid_scores > 10:  # Need at least 10 valid scores
                        score_col = col
                        break
                except:
                    continue
            
            if score_col is None:
                continue
            
            # Extract data
            result_df = df.iloc[:, [company_col, score_col]].copy()
            result_df.columns = ['Company', 'CEI_Score']
            
            # Clean
            result_df['Company'] = result_df['Company'].astype(str).str.strip()
            result_df = result_df[result_df['Company'] != '']
            result_df = result_df[~result_df['Company'].str.lower().isin(['nan', 'none', ''])]
            
            # Convert scores
            result_df['CEI_Score'] = pd.to_numeric(result_df['CEI_Score'], errors='coerce')
            result_df = result_df.dropna(subset=['CEI_Score'])
            result_df = result_df[result_df['CEI_Score'].between(0, 100)]
            
            # Basic filtering
            result_df = result_df[result_df['Company'].str.len() > 3]
            result_df = result_df[~result_df['Company'].str.match(r'^\d+\.?\d*$', na=False)]
            
            if len(result_df) > 5:  # Only keep if we have reasonable amount
                all_data.append(result_df)
        
        if not all_data:
            print(f"No valid data found for year {year}")
            return False
        
        # Combine and clean
        final_df = pd.concat(all_data, ignore_index=True)
        final_df = final_df.drop_duplicates(subset=['Company'])
        final_df['Year'] = year
        
        # Save
        os.makedirs(output_folder, exist_ok=True)
        output_file = os.path.join(output_folder, f"cei_{year}.csv")
        final_df.to_csv(output_file, index=False)
        
        print(f"✓ Saved {len(final_df)} companies for year {year}")
        return True
        
    except Exception as e:
        print(f"Error processing year {year}: {e}")
        return False

def process_missing_years():
    """Process all missing years quickly."""
    # Missing years list
    missing_years = [2002, 2003, 2004, 2005, 2006, 2009, 2010, 2011, 2013, 2014, 2015, 2018, 2020, 2021, 2022]
    
    processed = 0
    for year in missing_years:
        if quick_extract_year(year):
            processed += 1
        print(f"Progress: {processed}/{len(missing_years)} completed")
    
    print(f"Final result: {processed}/{len(missing_years)} years processed successfully")

if __name__ == "__main__":
    if len(sys.argv) == 2:
        year = int(sys.argv[1])
        quick_extract_year(year)
    else:
        process_missing_years()