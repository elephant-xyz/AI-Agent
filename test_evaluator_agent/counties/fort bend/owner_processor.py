import os
import re
import json
from bs4 import BeautifulSoup

INPUT_DIR = './input/'
OUTPUT_RAW = 'owners/owners_extracted.json'
OUTPUT_SCHEMA = 'owners/owners_schema.json'

COMPANY_KEYWORDS = [
    'INC', 'LLC', 'LTD', 'CORP', 'CO', 'FOUNDATION', 'ALLIANCE', 'RESCUE', 'MISSION',
    'SOLUTIONS', 'SERVICES', 'SYSTEMS', 'COUNCIL', 'VETERANS', 'FIRST RESPONDERS', 'HEROES',
    'INITIATIVE', 'ASSOCIATION', 'GROUP', 'TRUST', "TR", "tr"
]

def is_company(name):
    if not name:
        return False
    name_upper = name.upper()
    for kw in COMPANY_KEYWORDS:
        if kw in name_upper:
            return True
    if name_upper.strip().endswith('&'):
        return True
    return False

def parse_person_name(name):
    if not name:
        return {'first_name': None, 'last_name': None, 'middle_name': None}
    name = name.replace('&', '').strip()
    parts = name.split()
    if len(parts) == 0:
        return {'first_name': None, 'last_name': None, 'middle_name': None}
    if len(parts) == 1:
        return {'first_name': parts[0].title(), 'last_name': None, 'middle_name': None}
    if len(parts) == 2:
        return {'first_name': parts[1].title(), 'last_name': parts[0].title(), 'middle_name': None}
    return {
        'first_name': parts[1].title(),
        'last_name': parts[0].title(),
        'middle_name': ' '.join([p.title() for p in parts[2:]])
    }

from datetime import datetime

def extract_owners_from_html(filepath):
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        html = f.read()
    soup = BeautifulSoup(html, 'html.parser')
    property_id = os.path.splitext(os.path.basename(filepath))[0]
    owners_by_date = {}
    raw_owners = []

    # --- Current Owner (from Property Details table) ---
    current_owner = None
    current_year = None
    # Find the table with 'January 1 Owner' in a <th>
    for table in soup.find_all('table'):
        ths = table.find_all('th')
        for th in ths:
            if 'January 1 Owner' in th.get_text():
                # Now find the row with <th>Name:</th>
                for row in table.find_all('tr'):
                    cells = row.find_all(['th', 'td'])
                    if len(cells) >= 2 and 'Name:' in cells[0].get_text():
                        current_owner = cells[1].get_text(strip=True)
                        break
                break
        if current_owner:
            break
    # Try to get the year from the page title
    page_title = soup.find('h3')
    if page_title and 'For Year' in page_title.get_text():
        try:
            current_year = int(page_title.get_text().split('For Year')[-1].strip())
        except Exception:
            current_year = None
    if current_owner and current_year:
        owners_by_date[str(current_year)] = [current_owner]
        raw_owners.append({'type': 'current', 'year': str(current_year), 'name': current_owner})

    # --- Previous Owners (from Property Deed History table) ---
    deed_tables = soup.find_all('table')
    for table in deed_tables:
        # Look for Deed History header
        prev = table.find_previous('div', class_='panel-heading')
        if prev and 'Deed History' in prev.get_text():
            rows = table.find_all('tr')
            for row in rows[1:]:
                cols = row.find_all('td')
                if len(cols) >= 5:
                    deed_date = cols[0].get_text(strip=True)
                    grantee = cols[4].get_text(strip=True)
                    if grantee:
                        # Convert date to ISO format if possible
                        iso_date = None
                        try:
                            if deed_date:
                                iso_date = datetime.strptime(deed_date, '%m/%d/%Y').date().isoformat()
                        except Exception:
                            iso_date = None
                        # Use ISO date if available, else skip
                        if iso_date:
                            if iso_date not in owners_by_date:
                                owners_by_date[iso_date] = []
                            # Split by & if present
                            for n in re.split(r'\s*&\s*', grantee):
                                n = n.strip()
                                if n:
                                    owners_by_date[iso_date].append(n)
                                    raw_owners.append({'type': 'historical', 'date': iso_date, 'name': n})
    # Remove empty owner names
    for k in list(owners_by_date.keys()):
        owners_by_date[k] = [o for o in owners_by_date[k] if o and o.strip()]
        if not owners_by_date[k]:
            del owners_by_date[k]
    return property_id, owners_by_date, raw_owners

def main():
    os.makedirs('owners', exist_ok=True)
    extracted = {}
    schema = {}
    raw_extracted = {}
    for file in os.listdir(INPUT_DIR):
        if file.endswith('.html'):
            path = os.path.join(INPUT_DIR, file)
            property_id, owners_by_date, raw_owners = extract_owners_from_html(path)
            extracted[property_id] = owners_by_date
            raw_extracted[property_id] = raw_owners
    with open(OUTPUT_RAW, 'w', encoding='utf-8') as f:
        json.dump(raw_extracted, f, indent=2)
    for property_id, owners_by_date in extracted.items():
        schema[property_id] = {'owners_by_date': {}}
        for date, owners in owners_by_date.items():
            owner_objs = []
            for name in owners:
                if is_company(name):
                    owner_objs.append({
                        'type': 'company',
                        'name': name.title()
                    })
                else:
                    parsed = parse_person_name(name)
                    owner_objs.append({
                        'type': 'person',
                        'first_name': parsed['first_name'],
                        'last_name': parsed['last_name'],
                        'middle_name': parsed['middle_name']
                    })
            schema[property_id]['owners_by_date'][date] = owner_objs
    with open(OUTPUT_SCHEMA, 'w', encoding='utf-8') as f:
        json.dump(schema, f, indent=2)

if __name__ == '__main__':
    main()
