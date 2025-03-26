# Import necessary libraries
import urllib.request
import urllib.error
from bs4 import BeautifulSoup
import pandas as pd
import re
import sys
import os
import logging
import datetime
import openpyxl
from openpyxl.utils import get_column_letter
import time
import ssl
import math  # For isinf() function

# Python 3 strings are all Unicode, so no need for special handling
# Ensure proper decoding when reading from files or network

def read_urls_from_excel(excel_path, column_index=0, sheet_index=0):
    """
    Read URLs from an Excel file using pandas.
    Args:
        excel_path: Path to the Excel file
        column_index: Index of the column containing URLs (default: 0, i.e., first column)
        sheet_index: Index of the sheet containing URLs (default: 0, i.e., first sheet)
    Returns:
        List of URLs
    """
    try:
        # Use pandas to read the Excel file
        df = pd.read_excel(excel_path, sheet_name=sheet_index)
        
        # Determine which column to read
        if isinstance(column_index, int):
            if column_index >= len(df.columns):
                return []
            column_name = df.columns[column_index]
        else:
            column_name = column_index
            if column_name not in df.columns:
                return []
        
        # Extract URLs from the specified column
        urls = []
        for value in df[column_name].dropna():
            value_str = str(value).strip()
            if value_str.startswith("http"):
                urls.append(value_str)
        
        return urls
    except Exception as e:
        print("Error reading Excel file: {}".format(str(e)))
        return []

# Add a new function to read from TSV files
def read_urls_from_tsv(tsv_path, column_index=0):
    """
    Read URLs from a TSV file.
    Args:
        tsv_path: Path to the TSV file
        column_index: Index of the column containing URLs (default: 0, i.e., first column)
    Returns:
        List of URLs
    """
    try:
        urls = []
        with open(tsv_path, 'r', encoding='utf-8') as f:
            # Skip header line if present
            next(f, None)
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) > column_index:
                    url = parts[column_index].strip()
                    if url.startswith("http"):
                        urls.append(url)
        return urls
    except Exception as e:
        print("Error reading TSV file: {}".format(e))
        return []


# Add this helper function to convert monetary values to floats
def convert_to_float(value_str):
    """Convert a monetary string (like $1,234.56) to a float (1234.56)"""
    if value_str is None or value_str == '':
        return ""
    
    try:
        # Handle values that are already numbers
        if isinstance(value_str, (int, float)):
            return float(value_str)
            
        # Remove dollar signs, commas, and spaces
        cleaned_str = str(value_str).replace('$', '').replace(',', '').replace(' ', '')
        # If there's nothing left, return empty string
        if not cleaned_str:
            return ""
        # Convert to float    
        return float(cleaned_str)
    except (ValueError, TypeError):
        # If conversion fails, return original string
        return value_str


def generate_urls_for_year_range(start_year, end_year, start_ids, end_ids):
    """
    Generate a list of all possible URLs for the given year range.
    
    Args:
        start_year: Starting year (e.g., 2015)
        end_year: Ending year (e.g., 2025)
        start_ids: Dictionary mapping years to starting IDs
        end_ids: Dictionary mapping years to ending IDs
        
    Returns:
        List of URLs
    """
    urls = []
    
    for year in range(start_year, end_year + 1):
        if year in start_ids and year in end_ids:
            # Extract the numeric part of the ID and ensure they're integers
            start_id = int(start_ids[year])
            end_id = int(end_ids[year])
            
            # Format with leading zeros to ensure 5 digits (fixed format)
            id_format = "{:05d}"  # Always 5 digit format
            
            for id_num in range(start_id, end_id + 1):
                formatted_id = id_format.format(id_num)
                url = f"https://oag.ca.gov/prop65/60-Day-Notice-{year}-{formatted_id}"
                urls.append(url)
    
    return urls

# Example usage with proper 5-digit formatting:
start_ids = {
    2015: 1,      # Will be formatted as 00001
    2016: 1,      # Will be formatted as 00001
    2017: 1,
    2018: 1,
    2019: 1,
    2020: 1,
    2021: 1,
    2022: 1,
    2023: 1,
    2024: 1,
    2025: 1
}

end_ids = {
    2015: 1349,   # Will be formatted as 01349
    2016: 1581,   # Will be formatted as 01581
    2017: 2713,   # Will be formatted as 02713
    2018: 2368,   # Will be formatted as 02368
    2019: 2423,   # Will be formatted as 02423
    2020: 3543,   # Will be formatted as 03543
    2021: 3165,   # Will be formatted as 03165
    2022: 3174,   # Will be formatted as 03174
    2023: 4142,   # Will be formatted as 04142
    2024: 5403,   # Will be formatted as 05403
    2025: 881     # Will be formatted as 00881
}

# Generate URLs for the past 10 years
all_urls = generate_urls_for_year_range(2015, 2025, start_ids, end_ids)


# Move this function outside main()
def write_data_to_sheet_with_all_headers(sheet, all_data, all_headers_by_category):
    # Define data categories here to ensure they're in scope
    data_categories = ['data', 'flat_civil_complaint_data', 'flat_settlement_data', 
                    'flat_judgment_data', 'flat_corrected_settlement_data']
    
    # Define the exact column order with withdrawal columns removed
    ordered_headers = [
        # Main data
        'link',
        'AG Number',
        'Alleged Violators',
        'Chemicals',
        'Date Filed',
        'Notice PDF',
        'Noticing Party',
        'Plaintiff Attorney',
        'Source',
        
        # Only include Withdrawal Status and ID
        'Withdrawal Status',
        'Withdrawal ID',
        'Withdrawal Date',
        'Withdrawal Letter',
        
        # Civil Complaint data
        'Civil_Complaint_Date Filed',
        'Civil_Complaint_Case Name',
        'Civil_Complaint_Court Name',
        'Civil_Complaint_Court Docket Number',
        'Civil_Complaint_Plaintiff',
        'Civil_Complaint_Plaintiff Attorney',
        'Civil_Complaint_Defendant',
        'Civil_Complaint_Type of Claim',
        'Civil_Complaint_Relief Sought',
        'Civil_Complaint_Contact Name',
        'Civil_Complaint_Contact Organization',
        'Civil_Complaint_Email Address',
        'Civil_Complaint_Address',
        'Civil_Complaint_City, State, Zip',
        'Civil_Complaint_Phone Number'
    ]

    # Add Settlement columns (1-5) instead of just (1-3)
    for settlement_num in range(1, 6):
        settlement_fields = [
            'Settlement_{}_Settlement Date'.format(settlement_num),
            'Settlement_{}_Case Name'.format(settlement_num),
            'Settlement_{}_Court Name'.format(settlement_num),
            'Settlement_{}_Court Docket Number'.format(settlement_num),
            'Settlement_{}_Plaintiff'.format(settlement_num),
            'Settlement_{}_Plaintiff Attorney'.format(settlement_num),
            'Settlement_{}_Defendant'.format(settlement_num),
            'Settlement_{}_Injunctive Relief'.format(settlement_num),
            'Settlement_{}_Non-Contingent Civil Penalty'.format(settlement_num),
            'Settlement_{}_Attorneys Fees and Costs'.format(settlement_num),
            'Settlement_{}_Payment in Lieu of Penalty'.format(settlement_num),
            'Settlement_{}_Total Payments'.format(settlement_num),
            'Settlement_{}_Will settlement be submitted to court?'.format(settlement_num),
            'Settlement_{}_Contact Name'.format(settlement_num),
            'Settlement_{}_Contact Organization'.format(settlement_num),
            'Settlement_{}_Email Address'.format(settlement_num),
            'Settlement_{}_Address'.format(settlement_num),
            'Settlement_{}_City, State, Zip'.format(settlement_num),
            'Settlement_{}_Phone Number'.format(settlement_num)
        ]
        ordered_headers.extend(settlement_fields)
    
    # Add Corrected Settlement columns (1-5) instead of just (1-3)
    for settlement_num in range(1, 6):
        corrected_settlement_fields = [
            'Corrected_Settlement_{}_Settlement Date'.format(settlement_num),
            'Corrected_Settlement_{}_Case Name'.format(settlement_num),
            'Corrected_Settlement_{}_Court Name'.format(settlement_num),
            'Corrected_Settlement_{}_Court Docket Number'.format(settlement_num),
            'Corrected_Settlement_{}_Plaintiff'.format(settlement_num),
            'Corrected_Settlement_{}_Plaintiff Attorney'.format(settlement_num),
            'Corrected_Settlement_{}_Defendant'.format(settlement_num),
            'Corrected_Settlement_{}_Injunctive Relief'.format(settlement_num),
            'Corrected_Settlement_{}_Non-Contingent Civil Penalty'.format(settlement_num),
            'Corrected_Settlement_{}_Attorneys Fees and Costs'.format(settlement_num),
            'Corrected_Settlement_{}_Payment in Lieu of Penalty'.format(settlement_num),
            'Corrected_Settlement_{}_Total Payments'.format(settlement_num),
            'Corrected_Settlement_{}_Will settlement be submitted to court?'.format(settlement_num),
            'Corrected_Settlement_{}_Contact Name'.format(settlement_num),
            'Corrected_Settlement_{}_Contact Organization'.format(settlement_num),
            'Corrected_Settlement_{}_Email Address'.format(settlement_num),
            'Corrected_Settlement_{}_Address'.format(settlement_num),
            'Corrected_Settlement_{}_City, State, Zip'.format(settlement_num),
            'Corrected_Settlement_{}_Phone Number'.format(settlement_num)
        ]
        ordered_headers.extend(corrected_settlement_fields)
    
    # Add Judgment columns (1-5) instead of just 1
    for judgment_num in range(1, 6):
        judgment_fields = [
            'Judgment_{}_Judgment Date'.format(judgment_num),
            'Judgment_{}_Settlement reported to AG'.format(judgment_num),
            'Judgment_{}_Case Name'.format(judgment_num),
            'Judgment_{}_Court Name'.format(judgment_num),
            'Judgment_{}_Court Docket Number'.format(judgment_num),
            'Judgment_{}_Plaintiff'.format(judgment_num),
            'Judgment_{}_Plaintiff Attorney'.format(judgment_num),
            'Judgment_{}_Defendant'.format(judgment_num),
            'Judgment_{}_Injunctive Relief'.format(judgment_num),
            'Judgment_{}_Non-Contingent Civil Penalty'.format(judgment_num),
            'Judgment_{}_Attorneys Fees and Costs'.format(judgment_num),
            'Judgment_{}_Payment in Lieu of Penalty'.format(judgment_num),
            'Judgment_{}_Total Payments'.format(judgment_num),
            'Judgment_{}_Is Judgment Pursuant to Settlement?'.format(judgment_num),
            'Judgment_{}_Contact Name'.format(judgment_num),
            'Judgment_{}_Contact Organization'.format(judgment_num),
            'Judgment_{}_Email Address'.format(judgment_num),
            'Judgment_{}_Address'.format(judgment_num),
            'Judgment_{}_City, State, Zip'.format(judgment_num),
            'Judgment_{}_Phone Number'.format(judgment_num)
        ]
        ordered_headers.extend(judgment_fields)
    
    # Write headers in the first row - openpyxl uses 1-based indexing
    for col_idx, header in enumerate(ordered_headers, 1):
        sheet.cell(row=1, column=col_idx, value=header)
    
    # Create a mapping of headers to their column index - openpyxl uses 1-based indexing
    header_to_column = {header: i for i, header in enumerate(ordered_headers, 1)}
    
    # Initialize all empty values to prevent None errors
    for col_idx, _ in enumerate(ordered_headers, 1):
        for row_idx in range(2, len(all_data) + 2):
            sheet.cell(row=row_idx, column=col_idx, value="")
    
    # Process each URL's data - openpyxl uses 1-based indexing for rows too
    row_idx = 2  # Start from row 2 (after headers)
    
    def is_monetary_field(header):
        monetary_keywords = [
            "civil penalty", 
            "fees", 
            "costs",
            "payment",
            "penalty",
            "payments",
            "total"
        ]
        header_lower = header.lower()
        return any(keyword in header_lower for keyword in monetary_keywords)

    for entry in all_data:
        # Process all categories of data
        for category in data_categories:
            if category in entry:
                for header, value in entry[category].items():
                    # Find the column for this header
                    if header in header_to_column:
                        # Check if this is a monetary field that should be converted to float
                        if is_monetary_field(header):
                            value = convert_to_float(value)
                        
                        # Write value to cell - only if not empty
                        if value:  # Only write non-empty values
                            sheet.cell(row=row_idx, column=header_to_column[header], value=value)
        
        # Handle 'link' value separately to avoid duplication
        if 'data' in entry and 'link' in entry.get('data', {}):
            sheet.cell(row=row_idx, column=header_to_column['link'], 
                    value=entry['data']['link'])
        
        # Move to next row
        row_idx += 1

# Move this function outside of main()
def extract_value(soup, label_text):
    """Extract value from HTML based on a label."""
    label = soup.find("div", class_="field-label", string=lambda x: x and label_text in x)
    if label and label.find_next_sibling():
        sibling = label.find_next_sibling()
        if sibling:
            return sibling.text.strip()
    return ""

# Update the main function to handle TSV files
def main():
    """
    Main function to execute the OAG CA Gov scraper.
    This function sets up logging, processes input files or URLs, and scrapes data from the provided URLs.
    It supports reading URLs from Excel (.xls, .xlsx) or TSV (.tsv, .txt) files, or using a hardcoded list of URLs.
    The scraped data is then saved to an Excel file.
    The function performs the following steps:
    1. Sets up logging to a file with a timestamped filename.
    2. Logs the start of the scraping process.
    3. Checks if an input file is provided as an argument and reads URLs from the file if it exists.
    4. If no input file is provided, it uses a generated list of URLs or a hardcoded list of URLs.
    5. Defines helper functions to extract data from the HTML content.
    6. Collects data from all URLs, with retry mechanisms for failed URLs.
    7. Processes URLs in batches to prevent memory issues.
    8. Logs progress and saves intermediate results after each batch.
    9. Logs a summary of the results, including any failed URLs.
    10. Writes the scraped data to an Excel file.
    Note:
    - The function uses BeautifulSoup for HTML parsing.
    - The function handles special cases for extracting specific fields like Non-Contingent Civil Penalty, Address, and Email Address.
    - The function supports extracting data from sections like Civil Complaint, Settlement, and Judgment.
    - The function includes a retry mechanism for failed URL fetch attempts.
    Returns:
        None
    """
    # Set up logging to file
    log_filename = f"scraper_errors_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        filename=log_filename,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Log start of scraping
    logging.info("Starting OAG CA Gov scraper")
    
    # Check if a file is provided as an argument
    if len(sys.argv) > 1 and os.path.exists(sys.argv[1]):
        input_file = sys.argv[1]
        
        # Check if it's an Excel file
        if input_file.endswith(('.xls', '.xlsx')):
            print(f"Reading URLs from Excel file: {input_file}")
            logging.info(f"Reading URLs from Excel file: {input_file}")
            urls = read_urls_from_excel(input_file)
            
        # Check if it's a TSV file
        elif input_file.endswith('.tsv') or input_file.endswith('.txt'):
            print(f"Reading URLs from TSV file: {input_file}")
            logging.info(f"Reading URLs from TSV file: {input_file}")
            urls = read_urls_from_tsv(input_file)
            
        else:
            error_msg = "Unsupported file format. Please provide an Excel (.xls, .xlsx) or TSV (.tsv, .txt) file."
            print(error_msg)
            logging.error(error_msg)
            return
            
        if not urls:
            error_msg = "No valid URLs found in the input file or file could not be read."
            print(error_msg)
            logging.error(error_msg)
            return
            
        print(f"Found {len(urls)} URLs in the input file.")
        logging.info(f"Found {len(urls)} URLs in the input file.")
    elif all_urls:
        # Use the generated list of URLs
        print("Using generated list of URLs.")
        logging.info("Using generated list of URLs.")
        urls = all_urls
        print("Number of URLs: ", len(urls))
    else:
        # Use the hardcoded list of URLs
        print("Using hardcoded list of URLs.")
        logging.info("Using hardcoded list of URLs.")
        urls = [
            "https://oag.ca.gov/prop65/60-Day-Notice-2021-02146",
            "https://oag.ca.gov/prop65/60-Day-Notice-2021-02145",
            "https://oag.ca.gov/prop65/60-Day-Notice-2021-02147",
            "https://oag.ca.gov/prop65/60-Day-Notice-2021-02148",
            "https://oag.ca.gov/prop65/60-Day-Notice-2022-02148"
        ]

    # Define all_data at the start of main()
    all_data = []  # Will be populated with scraped data

    # Function to safely extract data
    def extract_value(soup, label_text):
        label = soup.find("div", class_="field-label", string=lambda x: x and label_text in x)
        if label and label.find_next_sibling():
            sibling = label.find_next_sibling()
            if sibling:
                return sibling.text.strip()
        return ""

    def extract_value_from_element(element, label_text):
        children = element.find_next()
        if not children:
            return ""

        # Special handling for Non-Contingent Civil Penalty
        if label_text == 'Non-Contingent Civil Penalty:':
            # Look for details-label div with details div containing Non-Contingent Civil Penalty text
            penalty_fields = children.find_all('div', class_='details-label')
            for field in penalty_fields:
                details = field.find('div', class_='details')
                if details and 'Non-Contingent Civil Penalty' in details.text:
                    # Extract the text directly from the details-label div (outside the details div)
                    # First get all text in the parent div
                    full_text = field.get_text().strip()
                    # Remove the text from the nested details div
                    details_text = details.get_text().strip()
                    # What remains should be the amount
                    amount = full_text.replace(details_text, '').strip()
                    return amount

        # Special handling for Address fields
        if label_text == 'Address:':
            # Try to find field with prop65-address class first
            address_field = children.find('div', class_=lambda c: c and "field-name-field-prop65-address" in c)
            if address_field:
                field_item = address_field.find('div', class_='field-item')
                if field_item:
                    return field_item.text.strip()
                    
            # Try to find div containing the address label
            address_label = children.find(lambda tag: tag.name == 'div' and 
                                        (tag.get('class') and 'field-label' in tag.get('class')) and 
                                        label_text in tag.text)
            if address_label and address_label.find_next_sibling():
                return address_label.find_next_sibling().text.strip()
        
        # Special handling for Email Address
        if label_text == 'Email Address:':
            # Try to find a link (a tag) which would indicate an email address
            email_a_tag = children.find('a', href=lambda href: href and 'mailto:' in href)
            if email_a_tag:
                return email_a_tag.text.strip()
            
            # If no mailto link is found, try the standard field-label approach
            email_div = children.find('div', class_="field-label", string=lambda x: x and label_text in x)
            if email_div and email_div.find_next_sibling():
                return email_div.find_next_sibling().text.strip()
            
            # If still not found, try the details class
            email_div = children.find('div', class_="details", string=lambda x: x and label_text in x)
            if email_div:
                # Look for an anchor tag that might contain the email
                email_a = email_div.parent.find('a')
                if email_a:
                    return email_a.text.strip()
                
                # If no anchor tag, try getting text after the details div
                if email_div.parent:
                    next_text = email_div.parent.contents[-1].strip() if len(email_div.parent.contents) > 1 else ""
                    if next_text:
                        return next_text
                    
            return ""
        
        # Try to find field-label first (general case)
        sibling = children.find('div', class_="field-label", string=lambda x: x and label_text in x)
        if sibling and sibling.next_sibling:
            return sibling.next_sibling.text.strip()
        
        # If not found, try details class
        sibling = children.find('div', class_="details", string=lambda x: x and label_text in x)
        if sibling:
            # Get the parent of the details div
            parent = sibling.parent
            if parent:
                # Get the text directly after the div.details
                next_text = parent.contents[-1].strip() if len(parent.contents) > 1 else ""
                if next_text:
                    return next_text
                    
            # Try other potential locations
            next_text = sibling.next_sibling.strip() if sibling.next_sibling else ""
            if next_text:
                return next_text
                
            field_item = sibling.find_next('div', class_='field-item')
            if field_item:
                return field_item.text.strip()
                
        return ""

    # Consolidated function to extract data based on section type
    def extract_section_data(div, section_type):
        data = {}
        field_mapping = {
            "Civil Complaint": {
                "Case Name": "Case Name:",
                "Court Name": "Court Name:",
                "Date Filed": "Date Filed:",
                "Court Docket Number": "Court Docket Number:",
                "Plaintiff": "Plaintiff:",
                "Plaintiff Attorney": "Plaintiff Attorney:",
                "Defendant": "Defendant:",
                "Type of Claim": "Type of Claim:",
                "Relief Sought": "Relief Sought:",
                "Contact Name": "Contact Name:",
                "Contact Organization": "Contact Organization:",
                "Email Address": "Email Address:",
                "Address": "Address:",
                "City, State, Zip": "City, State, Zip:",
                "Phone Number": "Phone Number:"
            },
            "Settlement": {
                "Settlement Date": "Settlement Date:",
                "Case Name": "Case Name:",
                "Court Name": "Court Name:",
                "Court Docket Number": "Court Docket Number:",
                "Plaintiff": "Plaintiff:",
                "Plaintiff Attorney": "Plaintiff Attorney:",
                "Defendant": "Defendant:",
                "Injunctive Relief": "Injunctive Relief:",
                "Non-Contingent Civil Penalty": "Non-Contingent Civil Penalty:",
                "Attorneys Fees and Costs": "Attorney(s) Fees and Costs:",
                "Payment in Lieu of Penalty": "Payment in Lieu of Penalty:",
                "Total Payments": "Total Payments:",
                "Will settlement be submitted to court?": "Will settlement be submitted to court?",
                "Contact Name": "Contact Name:",
                "Contact Organization": "Contact Organization:",
                "Email Address": "Email Address:",
                "Address": "Address:",
                "City, State, Zip": "City, State, Zip:",
                "Phone Number": "Phone Number:"
            },
            "Judgment": {
                "Judgment Date": "Judgment Date:",
                "Settlement reported to AG": "Settlement reported to AG:",
                "Case Name": "Case Name:",
                "Court Name": "Court Name:",
                "Court Docket Number": "Court Docket Number:",
                "Plaintiff": "Plaintiff:",
                "Plaintiff Attorney": "Plaintiff Attorney:",
                "Defendant": "Defendant:",
                "Injunctive Relief": "Injunctive Relief:",
                "Non-Contingent Civil Penalty": "Non-Contingent Civil Penalty:",
                "Attorneys Fees and Costs": "Attorney(s) Fees and Costs:",
                "Payment in Lieu of Penalty": "Payment in Lieu of Penalty:",
                "Total Payments": "Total Payments:",
                "Is Judgment Pursuant to Settlement?": "Is Judgment Pursuant to Settlement?",
                "Contact Name": "Contact Name:",
                "Contact Organization": "Contact Organization:",
                "Email Address": "Email Address:",
                "Address": "Address:",
                "City, State, Zip": "City, State, Zip:",
                "Phone Number": "Phone Number:"
            }
        }
        
        mapping = field_mapping.get(section_type, field_mapping["Settlement"])
        
        for field, label in mapping.items():
            value = extract_value_from_element(div, label)
            data[field] = value if value is not None else ""
        
        return data

    # First, collect all data from all URLs
    failed_urls = []  # Track failed URLs

    # Add retry mechanism for failed URLs
    max_retries = 3
    retry_delay = 5  # seconds

    # Process in batches to prevent memory issues
    BATCH_SIZE = 1000
    total_urls = len(urls)
    num_batches = (total_urls + BATCH_SIZE - 1) // BATCH_SIZE

    # Track start time for progress estimation
    start_time = time.time()
    urls_processed = 0
    
    # Create a new Excel workbook and add a sheet using openpyxl instead of xlwt
    workbook = openpyxl.Workbook()
    main_sheet = workbook.active
    main_sheet.title = "Main Data"

    # Process in batches as before
    for batch_num in range(num_batches):
        start_idx = batch_num * BATCH_SIZE
        end_idx = min((batch_num + 1) * BATCH_SIZE, total_urls)
        
        print(f"Processing batch {batch_num+1}/{num_batches} (URLs {start_idx+1}-{end_idx})")
        
        for i, url in enumerate(urls[start_idx:end_idx]):
            urls_to_process = end_idx - start_idx
            current_in_batch = i + 1
            
            # Calculate progress
            if i % 10 == 0 or i == urls_to_process - 1:  # Update every 10 URLs or at the end
                speed, elapsed, remaining, completion = estimate_progress(start_time, urls_processed, total_urls)
                
                print(f"\n--- PROGRESS UPDATE ---")
                print(f"Processing URL {current_in_batch}/{urls_to_process} in current batch ({urls_processed + 1}/{total_urls} total)")
                print(f"Speed: {speed:.2f} URLs/minute")
                print(f"Elapsed time: {elapsed}")
                print(f"Estimated remaining: {remaining}")
                print(f"Estimated completion: {completion}")
                print("-----------------------\n")
                
            print(f"Processing URL: {url}")
            retries = 0
            success = False
            
            while retries < max_retries and not success:
                try:
                    print(f"Processing URL: {url} (Attempt {retries+1}/{max_retries})")
                    context = ssl._create_unverified_context()
                    response = urllib.request.urlopen(url, context=context)
                    page_content = response.read().decode('utf-8')
                    success = True
                except urllib.error.URLError as e:
                    retries += 1
                    error_msg = f"Error fetching URL: {url} - {str(e)} (Attempt {retries}/{max_retries})"
                    print(error_msg)
                    logging.error(error_msg)
                    if retries < max_retries:
                        print(f"Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                    else:
                        failed_urls.append(url)
                        continue

            # Parse the page using BeautifulSoup
            try:
                soup = BeautifulSoup(page_content, "html.parser")

                # Check if notice has been withdrawn
                notice_withdrawn = False
                withdrawal_data = {}

                # Look for the withdrawal banner
                withdrawal_banner = soup.find("span", class_="label-danger", 
                                            string=lambda x: x and "THIS 60-DAY NOTICE HAS BEEN WITHDRAWN" in x)
                if withdrawal_banner:
                    notice_withdrawn = True
                    # Extract additional withdrawal information
                
                # Extract Withdrawal Letter
                withdrawal_letter_div = soup.find("div", class_="field-label", string=lambda x: x and "Withdrawal Letter:" in x)
                if withdrawal_letter_div and withdrawal_letter_div.find_next_sibling():
                    letter_item = withdrawal_letter_div.find_next_sibling().find("a")
                    if letter_item and letter_item.get("href"):
                        pdf_url = letter_item.get("href")
                        pdf_name = letter_item.text.strip()
                        withdrawal_data["Withdrawal Letter"] = f"[{pdf_name}]({pdf_url})"
            except Exception as e:
                error_msg = f"Error parsing URL: {url} - {str(e)}"
                print(error_msg)
                logging.error(error_msg)
                failed_urls.append(url)
                continue

            # Extract relevant data from the main section
            main_data = {
                "link": url,
                "AG Number": extract_value(soup, "AG Number:"),
                "Notice PDF": f"[{url.split('/')[-1]}.pdf](https://oag.ca.gov/prop65/60-Day-Notice-{url.split('/')[-1]}/{url.split('/')[-1]}.pdf)",
                "Date Filed": extract_value(soup, "Date Filed:"),
                "Noticing Party": extract_value(soup, "Noticing Party:"),
                "Plaintiff Attorney": extract_value(soup, "Plaintiff Attorney:"),
                "Alleged Violators": extract_value(soup, "Alleged Violators:"),
                "Chemicals": extract_value(soup, "Chemicals:"),
                "Source": extract_value(soup, "Source:"),
            }

            # Add withdrawal information if notice has been withdrawn
            if notice_withdrawn:
                main_data.update(withdrawal_data)

            # Extract Civil Complaint Data
            civil_complaint_div = soup.find("div", text="Civil Complaint")
            flat_civil_complaint_data = {}
            if civil_complaint_div:
                civil_complaint_data = extract_section_data(civil_complaint_div, "Civil Complaint")
                for key, value in civil_complaint_data.items():
                    flat_civil_complaint_data[f"Civil_Complaint_{key}"] = value

            # Properly find and distinguish between Corrected Settlement and Settlement divs
            # First find all settlement-related divs
            all_settlement_divs = soup.find_all("div", string=lambda s: s and s.strip() in ["Settlement", "Corrected Settlement"])
            
            # Separate into correct categories
            corrected_settlement_divs = [div for div in all_settlement_divs if div.string and div.string.strip() == "Corrected Settlement"]
            settlement_divs = [div for div in all_settlement_divs if div.string and div.string.strip() == "Settlement"]
            
            # Update the Corrected Settlement Data extraction to handle up to 5 settlements
            flat_corrected_settlement_data = {}
            for i, div in enumerate(corrected_settlement_divs[:5]):  # Change from [:3] to [:5]
                data = extract_section_data(div, "Settlement")
                for key, value in data.items():
                    flat_corrected_settlement_data[f"Corrected_Settlement_{i+1}_{key}"] = value or ""

            # Update the Settlement Data extraction to handle up to 5 settlements
            flat_settlement_data = {}
            for i, div in enumerate(settlement_divs[:5]):  # Change from [:3] to [:5]
                data = extract_section_data(div, "Settlement")
                for key, value in data.items():
                    flat_settlement_data[f"Settlement_{i+1}_{key}"] = value or ""

            # Extract Judgment Data
            judgment_divs = soup.find_all("div", text="Judgment")
            flat_judgment_data = {}
            for i, div in enumerate(judgment_divs[:5]):  # Change from just taking all to limiting to 5
                data = extract_section_data(div, "Judgment")
                for key, value in data.items():
                    flat_judgment_data[f"Judgment_{i+1}_{key}"] = value or ""

            # Organize data into a dictionary with specific order
            organized_data = {'data': main_data}
            
            if flat_civil_complaint_data:
                organized_data['flat_civil_complaint_data'] = flat_civil_complaint_data
                
            if flat_settlement_data:
                organized_data['flat_settlement_data'] = flat_settlement_data
                
            if flat_judgment_data:
                organized_data['flat_judgment_data'] = flat_judgment_data

            if flat_corrected_settlement_data:
                organized_data['flat_corrected_settlement_data'] = flat_corrected_settlement_data

            all_data.append(organized_data)

            # Increment counter on successful processing
            if success:
                urls_processed += 1
                
                # Update progress after each successful URL
                if urls_processed % 50 == 0:  # Show full stats every 50 successful URLs
                    speed, elapsed, remaining, completion = estimate_progress(start_time, urls_processed, total_urls)
                    print(f"\n=== MILESTONE: {urls_processed} URLs PROCESSED ===")
                    print(f"Current speed: {speed:.2f} URLs/minute")
                    print(f"Elapsed time: {elapsed}")
                    print(f"Estimated remaining: {remaining}")
                    print(f"Estimated completion: {completion}")
                    print("=====================================\n")

        # Save intermediate results after each batch
        if batch_num < num_batches - 1:
            intermediate_filename = f"60-Day-Notice-Data_batch{batch_num+1}.xlsx"
            workbook.save(intermediate_filename)
            print(f"Intermediate results saved to {intermediate_filename}")

    # Log summary of results
    logging.info(f"Scraping completed. Processed {len(all_data)} URLs successfully.")
    
    if failed_urls:
        logging.warning(f"Failed to process {len(failed_urls)} URLs:")
        for failed_url in failed_urls:
            logging.warning(f"  - {failed_url}")
        
        # Also write failed URLs to a separate file for easy re-processing
        with open(f"failed_urls_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt", "w", encoding='utf-8') as f:
            for url in failed_urls:
                f.write(f"{url}\n")

    # Now, extract all possible headers from all data
    all_headers_by_category = {}
    data_categories = ['data', 'flat_civil_complaint_data', 'flat_settlement_data', 'flat_judgment_data', 'flat_corrected_settlement_data']

    # Initialize headers for each category
    for category in data_categories:
        all_headers_by_category[category] = set()

    # Gather all possible headers from all data
    for entry in all_data:
        for category in data_categories:
            if category in entry:
                all_headers_by_category[category].update(entry[category].keys())

    # Remove 'link' from data category since we'll handle it separately
    if 'link' in all_headers_by_category['data']:
        all_headers_by_category['data'].remove('link')

    # Write all data to sheet
    write_data_to_sheet_with_all_headers(main_sheet, all_data, all_headers_by_category)

    # Save the Excel file
    output_filename = "60-Day-Notice-Data.xlsx"
    workbook.save(output_filename)
    print(f"Data successfully written to {output_filename}")

# Modified estimate_progress to ensure proper time format
def estimate_progress(start_time, urls_processed, total_urls):
    """
    Estimates scraping speed and time remaining based on progress so far.

    Args:
        start_time: Timestamp when scraping started (from time.time())
        urls_processed: Number of URLs successfully processed so far
        total_urls: Total number of URLs to process
    
    Returns:
        Tuple containing:
        - speed: URLs processed per minute
        - elapsed_time_str: Formatted string of elapsed time (HH:MM:SS)
        - remaining_time_str: Formatted string of estimated remaining time (HH:MM:SS)
        - completion_time_str: Formatted string of estimated completion time (HH:MM)
    """
    current_time = time.time()
    elapsed_time = current_time - start_time
    
    # Avoid division by zero
    if urls_processed == 0:
        return 0, "00:00:00", "Unknown", "Unknown"
    
    # Calculate speed (URLs per minute)
    speed = (urls_processed / elapsed_time) * 60
    
    # Calculate remaining time
    if speed > 0:
        remaining_urls = total_urls - urls_processed
        remaining_seconds = (remaining_urls / speed) * 60
        
        # Calculate estimated completion time
        completion_time = current_time + remaining_seconds
        completion_time_str = datetime.datetime.fromtimestamp(completion_time).strftime("%H:%M on %Y-%m-%d")
    else:
        remaining_seconds = float('inf')
        completion_time_str = "Unknown"
    
    # Format elapsed time as HH:MM:SS with leading zeros
    hours, remainder = divmod(int(elapsed_time), 3600)
    minutes, seconds = divmod(remainder, 60)
    elapsed_time_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    
    # Format remaining time as HH:MM:SS with leading zeros
    if not math.isinf(remaining_seconds):
        hours, remainder = divmod(int(remaining_seconds), 3600)
        minutes, seconds = divmod(remainder, 60)
        remaining_time_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    else:
        remaining_time_str = "Unknown"
    
    return speed, elapsed_time_str, remaining_time_str, completion_time_str

if __name__ == "__main__":
    main()
