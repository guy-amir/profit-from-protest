"""
OCR-based CEI extraction for PDFs that don't work with table extraction.
"""

import logging
import os
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import pytesseract
from pdf2image import convert_from_path
from PIL import Image

from .utils import extract_year_from_filename, find_pdfs_in_folder


def ocr_extract_cei_data(pdf_path: str, year: int) -> pd.DataFrame:
    """
    Extract CEI data using OCR on PDF pages.
    
    Args:
        pdf_path: Path to the CEI PDF file
        year: Year of the report
        
    Returns:
        DataFrame with columns: Company, CEI_Score, Year
    """
    try:
        logging.info(f"OCR processing {os.path.basename(pdf_path)} for year {year}")
        
        # Convert PDF to images (focus on likely appendix pages)
        if year >= 2015:
            # Modern reports - appendix usually around pages 40-80
            pages = convert_from_path(pdf_path, first_page=40, last_page=80, dpi=300)
        elif year >= 2010:
            # Mid-era reports - try pages 20-60
            pages = convert_from_path(pdf_path, first_page=20, last_page=60, dpi=300)
        else:
            # Older reports - try broader range
            pages = convert_from_path(pdf_path, first_page=10, last_page=50, dpi=300)
        
        if not pages:
            logging.warning(f"No pages extracted from {pdf_path}")
            return pd.DataFrame()
        
        logging.info(f"Processing {len(pages)} pages with OCR")
        
        # Extract text from all pages
        all_text = []
        for i, page in enumerate(pages):
            try:
                # Use OCR to extract text
                text = pytesseract.image_to_string(page, config='--psm 6')
                all_text.append(text)
                
                # Progress logging
                if (i + 1) % 10 == 0:
                    logging.info(f"Processed {i + 1}/{len(pages)} pages")
                    
            except Exception as e:
                logging.debug(f"Error processing page {i}: {e}")
                continue
        
        if not all_text:
            logging.warning(f"No text extracted from {pdf_path}")
            return pd.DataFrame()
        
        # Combine all text
        combined_text = '\n'.join(all_text)
        
        # Extract company data from text
        companies_data = _parse_cei_text(combined_text, year)
        
        if companies_data:
            df = pd.DataFrame(companies_data, columns=['Company', 'CEI_Score'])
            df['Year'] = year
            logging.info(f"OCR extracted {len(df)} companies for year {year}")
            return df
        else:
            logging.warning(f"No company data found in OCR text for year {year}")
            return pd.DataFrame()
            
    except Exception as e:
        logging.error(f"Error in OCR processing {pdf_path}: {e}")
        return pd.DataFrame()


def _parse_cei_text(text: str, year: int) -> List[Tuple[str, float]]:
    """
    Parse OCR text to extract company names and CEI scores.
    """
    companies = []
    lines = text.split('\n')
    
    # Look for cleaner company list patterns first
    companies.extend(_parse_clean_company_list(lines))
    
    # Different parsing strategies based on year
    if year >= 2015:
        companies.extend(_parse_modern_format(lines))
    elif year >= 2010:
        companies.extend(_parse_mid_format(lines))
    else:
        companies.extend(_parse_legacy_format(lines))
    
    # Remove duplicates and clean
    seen = set()
    cleaned_companies = []
    
    for company, score in companies:
        company = _clean_company_name(company)
        if company and company not in seen and _is_valid_company_score(company, score):
            seen.add(company)
            cleaned_companies.append((company, score))
    
    return cleaned_companies


def _parse_clean_company_list(lines: List[str]) -> List[Tuple[str, float]]:
    """Parse clean company list format - typical in appendices."""
    companies = []
    
    for line in lines:
        line = line.strip()
        if not line or len(line) < 10:
            continue
        
        # Look for lines with company name followed by location and score
        # Pattern: "Company Name City ST : score" or "Company Name : score"
        score_patterns = [
            r'^(.+?)\s+([A-Z]{2})\s*[:;]\s*(\d{1,3})(?:\s*$)',  # Company City ST : score
            r'^(.+?)\s*[:;]\s*(\d{1,3})(?:\s*$)',  # Company : score
            r'^(.+?)\s+(\d{1,3})(?:\s*$)',  # Company score
        ]
        
        for pattern in score_patterns:
            match = re.search(pattern, line)
            if match:
                if len(match.groups()) == 3:
                    company_part, state, score_str = match.groups()
                else:
                    company_part, score_str = match.groups()
                
                try:
                    score = int(score_str)
                    if 0 <= score <= 100:
                        # Clean company name
                        company_part = re.sub(r'[.:;]+$', '', company_part).strip()
                        company_part = re.sub(r'\s+', ' ', company_part)
                        
                        if len(company_part) > 3 and _looks_like_company_clean(company_part):
                            companies.append((company_part, float(score)))
                            break
                except ValueError:
                    continue
    
    return companies


def _parse_modern_format(lines: List[str]) -> List[Tuple[str, float]]:
    """Parse modern CEI format (2015+)."""
    companies = []
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        # Look for patterns like "Company Name 100" or "Company Name...100"
        # Score is usually at end of line
        score_match = re.search(r'(\d{1,3})(?:\s*$|\s*\n)', line)
        if score_match:
            score = int(score_match.group(1))
            if 0 <= score <= 100:
                # Extract company name (everything before the score)
                company_part = line[:score_match.start()].strip()
                company_part = re.sub(r'\.{2,}', '', company_part)  # Remove dots
                company_part = company_part.strip()
                
                if len(company_part) > 3 and _looks_like_company(company_part):
                    companies.append((company_part, float(score)))
    
    return companies


def _parse_mid_format(lines: List[str]) -> List[Tuple[str, float]]:
    """Parse mid-era CEI format (2010-2014)."""
    companies = []
    
    for i, line in enumerate(lines):
        line = line.strip()
        if not line:
            continue
        
        # Look for company names followed by scores on same or next line
        if _looks_like_company(line):
            # Check current line for score
            score_match = re.search(r'\b(\d{1,3})\b', line)
            if score_match:
                score = int(score_match.group(1))
                if 0 <= score <= 100:
                    company_name = re.sub(r'\b\d{1,3}\b', '', line).strip()
                    companies.append((company_name, float(score)))
            
            # Check next line for score
            elif i + 1 < len(lines):
                next_line = lines[i + 1].strip()
                score_match = re.search(r'^(\d{1,3})$', next_line)
                if score_match:
                    score = int(score_match.group(1))
                    if 0 <= score <= 100:
                        companies.append((line, float(score)))
    
    return companies


def _parse_legacy_format(lines: List[str]) -> List[Tuple[str, float]]:
    """Parse legacy CEI format (pre-2010)."""
    companies = []
    
    # Legacy formats often have different layouts
    # Try to find tabular data
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        # Split by whitespace and look for score at end
        parts = line.split()
        if len(parts) >= 2:
            try:
                last_part = parts[-1]
                score = float(last_part)
                if 0 <= score <= 100:
                    company_name = ' '.join(parts[:-1])
                    if _looks_like_company(company_name):
                        companies.append((company_name, score))
            except ValueError:
                continue
    
    return companies


def _parse_generic_format(lines: List[str]) -> List[Tuple[str, float]]:
    """Generic parsing as fallback."""
    companies = []
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        # Look for any line with company indicators and numbers
        if any(indicator in line.lower() for indicator in ['inc', 'corp', 'llc', 'ltd', 'company', '&']):
            # Extract all numbers from the line
            numbers = re.findall(r'\b(\d{1,3})\b', line)
            for num_str in numbers:
                num = int(num_str)
                if 0 <= num <= 100:
                    # Use the line as company name, removing the score
                    company_name = re.sub(r'\b' + num_str + r'\b', '', line).strip()
                    company_name = re.sub(r'\s+', ' ', company_name)  # Clean whitespace
                    if len(company_name) > 3:
                        companies.append((company_name, float(num)))
                    break  # Only take first valid score per line
    
    return companies


def _looks_like_company(text: str) -> bool:
    """Check if text looks like a company name."""
    if len(text) < 3:
        return False
    
    # Company indicators
    company_indicators = [
        'inc', 'corp', 'corporation', 'company', 'llc', 'ltd', 'llp', 
        'co.', 'group', 'holdings', 'enterprises', 'associates', 
        'partners', '&', 'and'
    ]
    
    text_lower = text.lower()
    
    # Has company indicators
    if any(indicator in text_lower for indicator in company_indicators):
        return True
    
    # Or is a reasonable length and has multiple words
    if len(text) > 10 and len(text.split()) >= 2:
        return True
    
    # Or looks like a proper name (capitalized words)
    words = text.split()
    if len(words) >= 2 and all(word[0].isupper() for word in words if word):
        return True
    
    return False


def _clean_company_name(company: str) -> str:
    """Clean up company name from OCR artifacts."""
    if not company:
        return ""
    
    # Remove common OCR artifacts
    company = re.sub(r'[;:]+$', '', company)  # Remove trailing punctuation
    company = re.sub(r'[@#$%^&*()]+', '', company)  # Remove symbols
    company = re.sub(r'\s+', ' ', company)  # Normalize whitespace
    company = re.sub(r'[.]{2,}', '', company)  # Remove multiple dots
    company = re.sub(r'[e]{3,}', '', company)  # Remove repeated 'e's from OCR
    company = re.sub(r'\s*:\s*$', '', company)  # Remove trailing colons
    
    # Remove location parts that got attached
    company = re.sub(r'\s+[A-Z]{2}\s*$', '', company)  # Remove state codes
    company = re.sub(r'\s+\d{5}(-\d{4})?\s*$', '', company)  # Remove zip codes
    
    return company.strip()


def _looks_like_company_clean(text: str) -> bool:
    """Enhanced company detection for cleaner data."""
    if len(text) < 3:
        return False
    
    text_lower = text.lower()
    
    # Filter out obvious non-companies first
    non_companies = [
        'score', 'rating', 'points', 'total', 'average', 'page', 'appendix',
        'table', 'figure', 'notes', 'criteria', 'requirement', 'policy',
        'benefit', 'training', 'harassment', 'discrimination', 'equality',
        'index', 'corporate', 'www.', 'http', 'email', '@', 'based on',
        'sexual orientation', 'gender identity', 'equivalency', 'credit',
        'exclusion', 'transition'
    ]
    
    if any(nc in text_lower for nc in non_companies):
        return False
    
    # Must have alphabetic characters
    if not re.search(r'[a-zA-Z]', text):
        return False
    
    # Company indicators (stronger filter)
    company_indicators = [
        'inc', 'corp', 'corporation', 'company', 'llc', 'ltd', 'llp', 
        'co.', 'group', 'holdings', 'enterprises', 'associates', 
        'partners', '&', 'financial', 'bank', 'insurance', 'healthcare',
        'systems', 'technologies', 'solutions', 'services'
    ]
    
    has_indicator = any(indicator in text_lower for indicator in company_indicators)
    
    # If has strong indicator, accept
    if has_indicator:
        return True
    
    # Otherwise, must be reasonably long and well-formed
    if len(text) > 15 and len(text.split()) >= 2:
        # Check if it looks like a proper name (most words capitalized)
        words = text.split()
        capitalized_words = sum(1 for word in words if word and word[0].isupper())
        if capitalized_words >= len(words) * 0.7:  # 70% of words capitalized
            return True
    
    return False


def _is_valid_company_score(company: str, score: float) -> bool:
    """Validate company name and score."""
    if not company or len(company.strip()) < 3:
        return False
    
    if not (0 <= score <= 100):
        return False
    
    company_lower = company.lower().strip()
    
    # Filter out obvious non-companies
    non_companies = [
        'score', 'rating', 'points', 'total', 'average', 'page', 'appendix',
        'table', 'figure', 'notes', 'criteria', 'requirement', 'policy',
        'benefit', 'training', 'harassment', 'discrimination', 'equality',
        'index', 'corporate', 'www.', 'http', 'email', '@', 'based on',
        'sexual orientation', 'gender identity', 'equivalency', 'credit',
        'exclusion', 'transition', 'blanket', 'individuals', 'without'
    ]
    
    if any(nc in company_lower for nc in non_companies):
        return False
    
    # Filter out pure numbers or single letters
    if re.match(r'^[\d\s\.]+$', company):
        return False
    
    # Filter out lines that are clearly OCR artifacts
    if len(re.findall(r'[^\w\s&\.\-]', company)) > len(company) * 0.3:
        return False  # Too many special characters
    
    if len(company.strip()) < 3:
        return False
    
    return True


def process_missing_years_ocr():
    """Process all missing years using OCR."""
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    cei_folder = "/Users/guy/Projects/noni/pfp/data/raw/CEI"
    output_folder = "/Users/guy/Projects/noni/pfp/data/processed/cei"
    
    # Missing years that need OCR processing
    missing_years = [2002, 2003, 2004, 2005, 2006, 2010, 2011, 2013, 2014, 2015, 2021, 2022]
    
    # Get PDF files
    pdf_files = find_pdfs_in_folder(cei_folder)
    
    processed_count = 0
    for year in missing_years:
        # Find PDF for this year
        pdf_file = None
        for pdf_path in pdf_files:
            if extract_year_from_filename(pdf_path) == year:
                pdf_file = pdf_path
                break
        
        if pdf_file is None:
            logging.warning(f"No PDF found for year {year}")
            continue
        
        # Extract with OCR
        cei_data = ocr_extract_cei_data(pdf_file, year)
        
        if cei_data.empty:
            logging.warning(f"No data extracted for year {year}")
            continue
        
        # Save file
        os.makedirs(output_folder, exist_ok=True)
        output_file = os.path.join(output_folder, f"cei_{year}.csv")
        cei_data.to_csv(output_file, index=False)
        
        logging.info(f"✓ Saved {len(cei_data)} companies for year {year}")
        processed_count += 1
    
    logging.info(f"OCR processing complete. Successfully processed {processed_count}/{len(missing_years)} years.")


def fix_incorrect_extractions():
    """Fix incorrectly extracted 2018 and 2020 data using OCR."""
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    cei_folder = "/Users/guy/Projects/noni/pfp/data/raw/CEI"
    output_folder = "/Users/guy/Projects/noni/pfp/data/processed/cei"
    
    years_to_fix = [2018, 2020]
    
    for year in years_to_fix:
        # Find PDF
        pdf_files = find_pdfs_in_folder(cei_folder)
        pdf_file = None
        for pdf_path in pdf_files:
            if extract_year_from_filename(pdf_path) == year:
                pdf_file = pdf_path
                break
        
        if pdf_file is None:
            logging.warning(f"No PDF found for year {year}")
            continue
        
        logging.info(f"Re-processing {year} with OCR")
        
        # Extract with OCR
        cei_data = ocr_extract_cei_data(pdf_file, year)
        
        if cei_data.empty:
            logging.warning(f"No OCR data extracted for year {year}")
            continue
        
        # Save corrected file
        output_file = os.path.join(output_folder, f"cei_{year}.csv")
        cei_data.to_csv(output_file, index=False)
        
        logging.info(f"✓ Fixed {year}: saved {len(cei_data)} companies")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "fix":
        fix_incorrect_extractions()
    else:
        process_missing_years_ocr()