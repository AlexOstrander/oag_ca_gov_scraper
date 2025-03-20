# Import necessary libraries
import urllib2
from bs4 import BeautifulSoup
import xlwt
import pandas as pd  # Replace xlrd with pandas
import re
import sys
import os


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
        # Read Excel file using pandas
        df = pd.read_excel(excel_path, sheet_name=sheet_index)
        
        # Get the column name (or use the index if it's a number)
        if isinstance(column_index, int):
            column_name = df.columns[column_index]
        else:
            column_name = column_index
            
        # Extract URLs from the specified column
        urls = []
        for value in df[column_name].dropna():
            # Convert to string in case it's not already
            value_str = str(value).strip()
            if value_str.startswith("http"):
                urls.append(value_str)
        
        return urls
    except Exception as e:
        print("Error reading Excel file: {}".format(e))
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
        with open(tsv_path, 'r') as f:
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

# Update the main function to handle TSV files
def main():
    # Check if a file is provided as an argument
    if len(sys.argv) > 1 and os.path.exists(sys.argv[1]):
        input_file = sys.argv[1]
        
        # Check if it's an Excel file
        if input_file.endswith(('.xls', '.xlsx')):
            print("Reading URLs from Excel file: {}".format(input_file))
            urls = read_urls_from_excel(input_file)
            
        # Check if it's a TSV file
        elif input_file.endswith('.tsv') or input_file.endswith('.txt'):
            print("Reading URLs from TSV file: {}".format(input_file))
            urls = read_urls_from_tsv(input_file)
            
        else:
            print("Unsupported file format. Please provide an Excel (.xls, .xlsx) or TSV (.tsv, .txt) file.")
            return
            
        if not urls:
            print("No valid URLs found in the input file or file could not be read.")
            return
            
        print("Found {} URLs in the input file.".format(len(urls)))
    else:
        # Use the hardcoded list of URLs
        print("Using hardcoded list of URLs.")
        urls = [
            "https://oag.ca.gov/prop65/60-Day-Notice-2021-02146",
            "https://oag.ca.gov/prop65/60-Day-Notice-2021-02145",
            # Add more URLs here if needed
        ]

    # Create a new Excel workbook and add a sheet
    workbook = xlwt.Workbook()
    main_sheet = workbook.add_sheet("Main Data")

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
            data[field] = extract_value_from_element(div, label)
        
        return data

    # First, collect all data from all URLs
    all_data = []
    for url in urls:
        try:
            response = urllib2.urlopen(url)
            page_content = response.read()
        except urllib2.URLError as e:
            print("Error fetching URL: {}".format(url))
            print("Error details: {}".format(e))
            continue

        # Parse the page using BeautifulSoup
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
            withdrawal_data["Withdrawal Status"] = "WITHDRAWN"
        
            # Extract Withdrawal ID
            withdrawal_id_div = soup.find("div", class_="field-label", string=lambda x: x and "Withdrawal ID:" in x)
            if withdrawal_id_div and withdrawal_id_div.find_next_sibling():
                withdrawal_data["Withdrawal ID"] = withdrawal_id_div.find_next_sibling().text.strip()
            
            # Extract Withdrawal Date
            withdrawal_date_div = soup.find("div", class_="field-label", string=lambda x: x and "Withdrawal Date:" in x)
            if withdrawal_date_div and withdrawal_date_div.find_next_sibling():
                withdrawal_data["Withdrawal Date"] = withdrawal_date_div.find_next_sibling().text.strip()
            
            # Extract Withdrawal Letter
            withdrawal_letter_div = soup.find("div", class_="field-label", string=lambda x: x and "Withdrawal Letter:" in x)
            if withdrawal_letter_div and withdrawal_letter_div.find_next_sibling():
                letter_item = withdrawal_letter_div.find_next_sibling().find("a")
                if letter_item and letter_item.get("href"):
                    pdf_url = letter_item.get("href")
                    pdf_name = letter_item.text.strip()
                    withdrawal_data["Withdrawal Letter"] = "[{}]({})".format(pdf_name, pdf_url)

            # Extract additional contact information
            # Contact Organization
            contact_org_div = soup.find("div", class_="field-label", string=lambda x: x and "Contact Organization:" in x)
            if contact_org_div and contact_org_div.find_next_sibling():
                withdrawal_data["Withdrawal Contact Organization"] = contact_org_div.find_next_sibling().text.strip()
            
            # Address
            address_div = soup.find("div", class_="field-label", string=lambda x: x and "Address:" in x)
            if address_div and address_div.find_next_sibling():
                withdrawal_data["Withdrawal Address"] = address_div.find_next_sibling().text.strip()
            
            # City, State, Zip
            city_div = soup.find("div", class_="field-label", string=lambda x: x and "City, State, Zip:" in x)
            if city_div and city_div.find_next_sibling():
                withdrawal_data["Withdrawal City State Zip"] = city_div.find_next_sibling().text.strip()
            
            # Contact Name
            contact_name_div = soup.find("div", class_="field-label", string=lambda x: x and "Contact Name:" in x)
            if contact_name_div and contact_name_div.find_next_sibling():
                withdrawal_data["Withdrawal Contact Name"] = contact_name_div.find_next_sibling().text.strip()
            
            # Phone Number
            phone_div = soup.find("div", class_="field-label", string=lambda x: x and "Phone Number:" in x)
            if phone_div and phone_div.find_next_sibling():
                withdrawal_data["Withdrawal Phone Number"] = phone_div.find_next_sibling().text.strip()
            
            # Email Address
            email_div = soup.find("div", class_="field-label", string=lambda x: x and "Email Address:" in x)
            if email_div and email_div.find_next_sibling():
                withdrawal_data["Withdrawal Email Address"] = email_div.find_next_sibling().text.strip()
            
            # Fax Number
            fax_div = soup.find("div", class_="field-label", string=lambda x: x and "Fax Number:" in x)
            if fax_div and fax_div.find_next_sibling():
                withdrawal_data["Withdrawal Fax Number"] = fax_div.find_next_sibling().text.strip()

        # Extract relevant data from the main section
        main_data = {
            "link": url,
            "AG Number": extract_value(soup, "AG Number:"),
            "Notice PDF": "[{}.pdf](https://oag.ca.gov/prop65/60-Day-Notice-{}/{}.pdf)".format(url.split("/")[-1], url.split("/")[-1], url.split("/")[-1]),
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
                flat_civil_complaint_data["Civil_Complaint_{}".format(key)] = value

        # Properly find and distinguish between Corrected Settlement and Settlement divs
        # First find all settlement-related divs
        all_settlement_divs = soup.find_all("div", string=lambda s: s and s.strip() in ["Settlement", "Corrected Settlement"])
        
        # Separate into correct categories
        corrected_settlement_divs = [div for div in all_settlement_divs if div.string and div.string.strip() == "Corrected Settlement"]
        settlement_divs = [div for div in all_settlement_divs if div.string and div.string.strip() == "Settlement"]
        
        # Corrected Settlement Data
        flat_corrected_settlement_data = {}
        for i, div in enumerate(corrected_settlement_divs[:3]):
            data = extract_section_data(div, "Settlement")
            for key, value in data.items():
                flat_corrected_settlement_data["Corrected_Settlement_{}_{}".format(i+1, key)] = value or ""

        # Settlement Data
        flat_settlement_data = {}
        for i, div in enumerate(settlement_divs[:3]):
            data = extract_section_data(div, "Settlement")
            for key, value in data.items():
                flat_settlement_data["Settlement_{}_{}".format(i+1, key)] = value or ""

        # Extract Judgment Data
        judgment_divs = soup.find_all("div", text="Judgment")
        flat_judgment_data = {}
        for i, div in enumerate(judgment_divs):
            data = extract_section_data(div, "Judgment")
            for key, value in data.items():
                flat_judgment_data["Judgment_{}_{}".format(i+1, key)] = value or ""

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

    def write_data_to_sheet_with_all_headers(sheet, all_data, all_headers_by_category):
        # Define the exact column order
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
            
            # Withdrawal data
            'Withdrawal Address',
            'Withdrawal City State Zip',
            'Withdrawal Contact Name',
            'Withdrawal Contact Organization',
            'Withdrawal Date',
            'Withdrawal Email Address',
            'Withdrawal Fax Number',
            'Withdrawal ID',
            'Withdrawal Letter',
            'Withdrawal Phone Number',
            'Withdrawal Status',
            
            # Civil Complaint data
            'Civil_Complaint_Date_Filed',
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

        # Add Settlement columns (1, 2, 3)
        for settlement_num in range(1, 4):
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
                'Settlement_{}_Phone Number'.format(settlement_num),
                'Settlement_{}_City, State, Zip'.format(settlement_num)
            ]
            ordered_headers.extend(settlement_fields)
        
        # Add Corrected Settlement columns (1, 2, 3)
        for settlement_num in range(1, 4):
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
                'Corrected_Settlement_{}_Phone Number'.format(settlement_num),
                'Corrected_Settlement_{}_City, State, Zip'.format(settlement_num)
            ]
            ordered_headers.extend(corrected_settlement_fields)
        
        # Add Judgment columns
        judgment_fields = [
            'Judgment_1_Judgment Date',
            'Judgment_1_Settlement reported to AG',
            'Judgment_1_Case Name',
            'Judgment_1_Court Name',
            'Judgment_1_Court Docket Number',
            'Judgment_1_Plaintiff',
            'Judgment_1_Plaintiff Attorney',
            'Judgment_1_Defendant',
            'Judgment_1_Injunctive Relief',
            'Judgment_1_Non-Contingent Civil Penalty',
            'Judgment_1_Attorneys Fees and Costs',
            'Judgment_1_Payment in Lieu of Penalty',
            'Judgment_1_Total Payments',
            'Judgment_1_Is Judgment Pursuant to Settlement?',
            'Judgment_1_Contact Name',
            'Judgment_1_Contact Organization',
            'Judgment_1_Email Address',
            'Judgment_1_Address',
            'Judgment_1_City, State, Zip',
            'Judgment_1_Phone Number'
        ]
        ordered_headers.extend(judgment_fields)
        
        # Write headers in the first row
        for col, header in enumerate(ordered_headers):
            sheet.write(0, col, header)
        
        # Create a mapping of headers to their column index
        header_to_column = {header: i for i, header in enumerate(ordered_headers)}
        
        # Prepare a data matrix to hold all values before writing to the sheet
        data_matrix = []
        
        # Process each URL's data
        for entry in all_data:
            # Create a row with empty values for each column
            row_data = [""] * len(ordered_headers)
            
            # Process all categories of data
            for category in data_categories:
                if category in entry:
                    for header, value in entry[category].items():
                        if header in header_to_column:
                            row_data[header_to_column[header]] = value
            
            # Handle 'link' value separately to avoid duplication
            if 'data' in entry and 'link' in entry['data']:
                row_data[header_to_column['link']] = entry['data']['link']
            
            data_matrix.append(row_data)
        
        # Write all data to the sheet
        for row_idx, row_data in enumerate(data_matrix, 1):
            for col_idx, value in enumerate(row_data):
                if value:  # Only write non-empty values
                    sheet.write(row_idx, col_idx, value)

    # Write all data to sheet
    write_data_to_sheet_with_all_headers(main_sheet, all_data, all_headers_by_category)

    # Save the Excel file
    output_filename = "60-Day-Notice-Data.xls"
    workbook.save(output_filename)
    print("Data successfully written to {}".format(output_filename))

if __name__ == "__main__":
    main()
