import os
import re
import json
import html
from datetime import datetime
from bs4 import BeautifulSoup

INPUT_DIR = './input/'
OUTPUT_DIR = './owners/'

# Company detection keywords (same as other scripts)
COMPANY_KEYWORDS = [
    'INC', 'LLC', 'LTD', 'CORP', 'CO', 'FOUNDATION', 'ALLIANCE', 'RESCUE', 'MISSION',
    'SOLUTIONS', 'SERVICES', 'SYSTEMS', 'COUNCIL', 'VETERANS', 'FIRST RESPONDERS', 'HEROES',
    'INITIATIVE', 'ASSOCIATION', 'GROUP', 'TRUST', 'PARTNERS', 'PROPERTIES', 'HOLDINGS',
    'ENTERPRISES', 'INVESTMENTS', 'FUND', 'BANK', 'SAVINGS', 'MORTGAGE', 'REALTY',
    'COMPANY', 'LP', 'LLP', 'PLC', 'PC', 'PLLC', 'P.A.', 'P.C.', 'TR', 'Tr', 'DIST'
]


def parse_owner_name(name):
    """Parse owner name into structured format, same as other scripts"""
    if not name or not name.strip():
        return None

    name = name.strip()
    upper_name = name.upper()

    # Check if it's a company - use word boundaries for better matching
    for kw in COMPANY_KEYWORDS:
        # For single letters like 'A' from 'P.A.', be more specific
        if len(kw) == 1:
            # Only match if it's at the end as a separate word or part of common abbreviations
            if (upper_name.endswith(' ' + kw) or
                    'P.A.' in upper_name or 'P.C.' in upper_name or 'L.L.C.' in upper_name):
                return {'type': 'company', 'name': name}
        else:
            # For multi-character keywords, check if it's a separate word
            import re
            pattern = r'\b' + re.escape(kw) + r'\b'
            if re.search(pattern, upper_name):
                return {'type': 'company', 'name': name}

    # Person name parsing - same logic as your reference scripts
    # Handle "&" in names (joint ownership)
    if ' & ' in name:
        # For joint ownership, take the first name for parsing
        name = name.split(' & ')[0].strip()

    # Remove "&" and clean up
    name = name.replace('&', '').strip()
    parts = name.replace('  ', ' ').strip().split()

    if len(parts) == 0:
        return {'type': 'person', 'first_name': None, 'last_name': None, 'middle_name': None}
    elif len(parts) == 1:
        return {'type': 'person', 'first_name': parts[0], 'last_name': None, 'middle_name': None}
    elif len(parts) == 2:
        return {'type': 'person', 'first_name': parts[1], 'last_name': parts[0], 'middle_name': None}
    elif len(parts) == 3:
        return {'type': 'person', 'first_name': parts[1], 'middle_name': parts[2], 'last_name': parts[0]}
    else:
        return {'type': 'person', 'first_name': parts[0], 'middle_name': ' '.join(parts[1:-1]), 'last_name': parts[-1]}


def owners_are_equal(owner1, owner2):
    """Check if two parsed owner objects are the same"""
    if owner1['type'] != owner2['type']:
        return False

    if owner1['type'] == 'company':
        return owner1['name'].upper().strip() == owner2['name'].upper().strip()

    # For persons, compare all name components
    return (
            (owner1.get('first_name') or '').upper().strip() == (owner2.get('first_name') or '').upper().strip() and
            (owner1.get('middle_name') or '').upper().strip() == (owner2.get('middle_name') or '').upper().strip() and
            (owner1.get('last_name') or '').upper().strip() == (owner2.get('last_name') or '').upper().strip()
    )


def deduplicate_owners(owners_list):
    """Remove duplicate owners from the list"""
    unique_owners = []

    for owner in owners_list:
        # Check if this owner already exists in unique_owners
        is_duplicate = False
        for existing_owner in unique_owners:
            if owners_are_equal(owner, existing_owner):
                is_duplicate = True
                break

        if not is_duplicate:
            unique_owners.append(owner)

    return unique_owners


def decode_html_entities(text):
    """Decode HTML entities in the text"""
    return html.unescape(text)


def extract_owners_from_html_file(filepath):
    """Extract owners from an HTML file and return structured data"""
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()

        # Parse with BeautifulSoup
        soup = BeautifulSoup(content, 'html.parser')

        raw_owners = []
        owners_by_date = {}

        # Method 1: Look for the ownershipDiv section
        ownership_div = soup.find('div', id='ownershipDiv')

        if ownership_div:
            # Find all ul elements with class genericList
            generic_lists = ownership_div.find_all('ul', class_='genericList')
            for ul in generic_lists:
                for li in ul.find_all('li'):
                    name = li.get_text(strip=True)
                    if name:
                        raw_owners.append({'type': 'current', 'name': name})

        # Method 2: Search in the raw HTML content for encoded patterns
        ownership_patterns = [
            r'<p><ul class=&quot;genericList&quot;>.*?</ul><br/></p>',
            r'</li><ul class=&quot;genericList&quot;>.*?</ul></ul><br/></p>'
        ]

        for pattern in ownership_patterns:
            matches = re.findall(pattern, content, re.DOTALL)
            for match in matches:
                # Decode HTML entities
                decoded = decode_html_entities(match)
                # Extract names from li elements
                li_names = re.findall(r'<li>(.*?)</li>', decoded)
                for name in li_names:
                    clean_name = re.sub(r'\s+', ' ', name.strip())
                    clean_name = clean_name.replace('&nbsp;', ' ').strip()
                    if clean_name and not any(clean_name == o['name'] for o in raw_owners):
                        raw_owners.append({'type': 'current', 'name': clean_name})

        # Method 3: Look for owner names in the main content
        # Search for "Owner Of Record" section
        owner_record_pattern = r'Owner Of Record.*?<div class="textPanel">\s*<div>(.*?)</div>'
        owner_matches = re.findall(owner_record_pattern, content, re.DOTALL)

        for match in owner_matches:
            # Clean HTML tags and extract names
            clean_text = re.sub(r'<[^>]+>', '', match)
            lines = [line.strip() for line in clean_text.split('\n') if line.strip()]

            for line in lines:
                # Skip address lines (containing numbers, FL, zip codes, etc.)
                if re.search(r'\d+|FL \d{5}|LEHIGH ACRES|FORT MYERS', line.upper()):
                    continue

                # Check if it looks like a name
                if line and len(line.split()) >= 1:
                    # Handle joint ownership with &
                    if ' & ' in line:
                        names = line.split(' & ')
                        for name in names:
                            name = name.strip()
                            if name and not any(name == o['name'] for o in raw_owners):
                                raw_owners.append({'type': 'current', 'name': name})
                    else:
                        if not any(line == o['name'] for o in raw_owners):
                            raw_owners.append({'type': 'current', 'name': line})

        # Method 4: Lee County specific - Look for divDisplayParcelOwner section
        owner_section = soup.find('div', {'id': 'divDisplayParcelOwner'})
        if owner_section:
            # Find the text panel with owner information
            text_panel = owner_section.find('div', class_='textPanel')
            if text_panel:
                # Extract the full text
                owner_text = text_panel.get_text(strip=True)
                print(f"Found owner text in Lee County format: {owner_text}")
                
                # Parse the owner names from the text
                # The format is typically: "OWNER1 & OWNER2\nADDRESS\nCITY STATE ZIP"
                # For Lee County, the text might be concatenated without proper newlines
                # Try to extract the address part (everything after the names)
                # Look for patterns like "1418 SE 12TH TER" or "CAPE CORAL FL 33990"
                address_pattern = r'(\d+\s+[A-Z\s]+)'
                
                address_match = re.search(address_pattern, owner_text)
                
                if address_match:
                    # The owner names should be everything before the street address
                    owner_part = owner_text[:address_match.start()].strip()
                    
                    # Split owner names by common separators
                    if '&' in owner_part:
                        owner_parts = owner_part.split('&')
                        for part in owner_parts:
                            name = part.strip()
                            if name:
                                # Clean HTML entities and extra characters
                                name = name.replace("&amp;", "&").replace("&", "&").strip()
                                # Remove trailing punctuation like "&" if it's not part of the name
                                if name.endswith(" &") and len(name) > 2:
                                    name = name[:-2].strip()
                                if name and not any(name == o['name'] for o in raw_owners):
                                    raw_owners.append({'type': 'current', 'name': name})
                                    print(f"Found Lee County owner: {name}")
                    else:
                        # Single owner
                        if owner_part and not any(owner_part == o['name'] for o in raw_owners):
                            raw_owners.append({'type': 'current', 'name': owner_part})
                            print(f"Found Lee County owner: {owner_part}")
                else:
                    print(f"Could not parse Lee County owner format: {owner_text}")
            else:
                print("No textPanel found in Lee County divDisplayParcelOwner")
        else:
            print("No divDisplayParcelOwner found in Lee County HTML")

        # Try to extract sale date for owners_by_date
        sale_date = None
        sale_pattern = r'Sale Date.*?(\d{1,2}/\d{1,2}/\d{4})'
        sale_match = re.search(sale_pattern, content)
        if sale_match:
            try:
                sale_date = datetime.strptime(sale_match.group(1), '%m/%d/%Y').strftime('%Y-%m-%d')
            except:
                pass

        # If no sale date found, use "current" as the key
        if not sale_date:
            sale_date = "current"

        # Parse current owners for owners_by_date and deduplicate
        current_owners = []
        for owner in raw_owners:
            if owner['type'] == 'current':
                parsed = parse_owner_name(owner['name'])
                if parsed:
                    current_owners.append(parsed)

        # Deduplicate owners
        if current_owners:
            unique_owners = deduplicate_owners(current_owners)
            owners_by_date[sale_date] = unique_owners

        return raw_owners, owners_by_date

    except Exception as e:
        print(f"Error processing {filepath}: {str(e)}")
        return [], {}


def extract_property_id_from_html(filepath):
    """Extract property ID from filename - use the HTML filename without extension"""
    filename = os.path.basename(filepath)
    if filename.endswith('.html'):
        return filename.replace('.html', '')
    return filename


def main():
    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    extracted_owners = {}
    schema = {}
    raw_extracted = {}

    # Process all HTML files in the input directory
    if not os.path.exists(INPUT_DIR):
        print(f"Input directory {INPUT_DIR} does not exist!")
        return

    html_files = [f for f in os.listdir(INPUT_DIR) if f.endswith('.html')]

    if not html_files:
        print(f"No HTML files found in {INPUT_DIR}")
        return

    print(f"Found {len(html_files)} HTML files to process...")

    for filename in html_files:
        filepath = os.path.join(INPUT_DIR, filename)
        print(f"Processing: {filename}")

        # Extract property ID
        property_id = extract_property_id_from_html(filepath)

        # Extract owners
        raw_owners, owners_by_date = extract_owners_from_html_file(filepath)

        if raw_owners:
            # Store raw owners
            raw_extracted[property_id] = raw_owners

            # Store simple list of owner names
            owner_names = [owner['name'] for owner in raw_owners]
            extracted_owners[property_id] = owner_names

            # Store structured schema with property_ prefix + filename
            schema[f'property_{property_id}'] = {'owners_by_date': owners_by_date}

            print(f"  Found {len(raw_owners)} owner(s): {', '.join(owner_names)}")
        else:
            print(f"  No owners found")

    # Save results in the same format as other scripts
    with open(os.path.join(OUTPUT_DIR, 'owners_extracted.json'), 'w', encoding='utf-8') as f:
        json.dump(raw_extracted, f, indent=2, ensure_ascii=False)

    with open(os.path.join(OUTPUT_DIR, 'owners_schema.json'), 'w', encoding='utf-8') as f:
        json.dump(schema, f, indent=2, ensure_ascii=False)

    print(f"\nProcessing complete!")
    print(f"Total properties processed: {len(html_files)}")
    print(f"Properties with owners found: {len(extracted_owners)}")
    print(f"Results saved to: {OUTPUT_DIR}")

    # Print summary
    print(f"\nSummary of extracted owners:")
    for prop_id, owners in extracted_owners.items():
        print(f"Property {prop_id}: {', '.join(owners)}")

    # Print sample schema structure
    if schema:
        sample_key = list(schema.keys())[0]
        print(f"\nSample schema structure for {sample_key}:")
        print(json.dumps(schema[sample_key], indent=2)[:500] + "..." if len(
            json.dumps(schema[sample_key], indent=2)) > 500 else json.dumps(schema[sample_key], indent=2))


if __name__ == '__main__':
    main()