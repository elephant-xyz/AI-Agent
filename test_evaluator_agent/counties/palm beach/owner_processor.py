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

def extract_owners_from_html(filepath):
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        html = f.read()
    soup = BeautifulSoup(html, 'html.parser')
    property_id = os.path.splitext(os.path.basename(filepath))[0]
    owners_by_date = {}
    raw_owners = []

    # --- Owner(s) Table ---
    owner_info = soup.find('h2', string=re.compile(r'Owner INFORMATION', re.I))
    if owner_info:
        table = owner_info.find_next('table')
        if table:
            for row in table.find_all('tr'):
                tds = row.find_all('td')
                if tds:
                    # All <span> in first <td>
                    spans = tds[0].find_all('span')
                    for span in spans:
                        name = span.get_text(strip=True)
                        if name:
                            # Split by & if present
                            for n in re.split(r'\s*&\s*', name):
                                n = n.strip()
                                if n:
                                    raw_owners.append({'type': 'current', 'name': n})
    # --- Sales Table (for previous owners) ---
    sales_info = soup.find('h2', string=re.compile(r'Sales INFORMATION', re.I))
    if sales_info:
        table = sales_info.find_next('table')
        if table:
            rows = table.find_all('tr')
            for row in rows[1:]:
                cols = row.find_all('td')
                if len(cols) >= 5:
                    date = cols[0].get_text(strip=True)
                    owner = cols[4].get_text(strip=True)
                    if owner:
                        # Split by & if present
                        for n in re.split(r'\s*&\s*', owner):
                            n = n.strip()
                            if n:
                                raw_owners.append({'type': 'historical', 'date': date, 'name': n})
                                if date not in owners_by_date:
                                    owners_by_date[date] = []
                                owners_by_date[date].append(n)
    # Portability Calculator
    port_calc = soup.find('td', string=re.compile(r'Owner Name', re.I))
    if port_calc:
        val_td = port_calc.find_next('td')
        if val_td:
            name = val_td.get_text(strip=True)
            if name and not any(name == o['name'] for o in raw_owners):
                for n in re.split(r'\s*&\s*', name):
                    n = n.strip()
                    if n:
                        raw_owners.append({'type': 'current', 'name': n})
    # Exemption Table
    exemp_info = soup.find('h2', string=re.compile(r'Exemption INFORMATION', re.I))
    if exemp_info:
        table = exemp_info.find_next('table')
        if table:
            rows = table.find_all('tr')
            for row in rows[1:]:
                cols = row.find_all('td')
                if cols:
                    name = cols[0].get_text(strip=True)
                    if name and not any(name == o['name'] for o in raw_owners):
                        for n in re.split(r'\s*&\s*', name):
                            n = n.strip()
                            if n:
                                raw_owners.append({'type': 'exemption', 'name': n})
    # Build owners_by_date for current owner (from Owner(s) table)
    prop_detail = soup.find('h2', string=re.compile(r'Property detail', re.I))
    sale_date = None
    if prop_detail:
        table = prop_detail.find_next('table')
        if table:
            for row in table.find_all('tr'):
                tds = row.find_all('td')
                if len(tds) >= 2 and 'Sale Date' in tds[0].get_text():
                    sale_date = tds[1].get_text(strip=True)
                    break
    current_owners = [o['name'] for o in raw_owners if o['type'] == 'current']
    if sale_date and current_owners:
        owners_by_date[sale_date] = current_owners
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
