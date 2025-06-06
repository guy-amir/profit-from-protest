#!/usr/bin/env python3
"""
Process a single year at a time to avoid timeouts.
"""

import sys
import os
sys.path.append('src')

from src.pfp.comprehensive_cei_extractor import extract_cei_comprehensive
import logging

def process_single_year(year):
    """Process a single year."""
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    cei_folder = "/Users/guy/Projects/noni/pfp/data/raw/CEI"
    output_folder = "/Users/guy/Projects/noni/pfp/data/processed/cei"
    
    # Find PDF for this year
    pdf_files = []
    for root, dirs, files in os.walk(cei_folder):
        for file in files:
            if file.lower().endswith(".pdf"):
                import re
                match = re.search(r'(\d{4})', file)
                if match and int(match.group(1)) == year:
                    pdf_files.append(os.path.join(root, file))
    
    if not pdf_files:
        print(f"No PDF found for year {year}")
        return False
    
    pdf_path = pdf_files[0]  # Take first match
    print(f"Processing {os.path.basename(pdf_path)} for year {year}")
    
    # Extract data
    cei_data = extract_cei_comprehensive(pdf_path, year)
    
    if cei_data.empty:
        print(f"No data extracted for year {year}")
        return False
    
    # Save file
    os.makedirs(output_folder, exist_ok=True)
    output_file = os.path.join(output_folder, f"cei_{year}.csv")
    cei_data.to_csv(output_file, index=False)
    
    print(f"✓ Saved {len(cei_data)} companies for year {year} to {output_file}")
    return True

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python process_year.py YEAR")
        sys.exit(1)
    
    year = int(sys.argv[1])
    success = process_single_year(year)
    sys.exit(0 if success else 1)